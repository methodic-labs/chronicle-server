-- ==========================================================================
-- Migration: convert TEXT_UUID (varchar(36)) columns to native Postgres uuid.
--
-- Affected tables: chronicle_usage_events, chronicle_usage_stats, audit,
-- sensor_data. The preprocessed_usage_events table is handled separately
-- (the writer owns its lifecycle) and is not included here.
--
-- Strategy per table:
--   1. Rename old table + its indices/constraints (suffix _tf) so the
--      original names are free for the new objects.
--   2. CREATE the new table with native uuid columns.
--   3. Copy data with ::uuid casts in the SELECT.
--   4. (Separately) rebuild the non-unique indices CONCURRENTLY — see the
--      final section. CONCURRENTLY cannot run inside a transaction block.
--
-- Keeps the old data under <name>_type_fix until you verify the new table,
-- then DROP TABLE <name>_type_fix to reclaim space.
--
-- For large tables (chronicle_usage_events, sensor_data) the INSERT ... SELECT
-- will be a long single transaction that holds locks the whole time.
-- If that's operationally painful, batch by event_timestamp / recordeddate
-- range or use COPY to stage the data.
-- ==========================================================================

-- --------------------------------------------------------------------------
-- 1. chronicle_usage_events
-- --------------------------------------------------------------------------
BEGIN;

ALTER TABLE chronicle_usage_events RENAME TO chronicle_usage_events_type_fix;
ALTER TABLE chronicle_usage_events_type_fix
    RENAME CONSTRAINT chronicle_usage_events_dedup_hash_0_dedup_hash_1_key
    TO chronicle_usage_events_dedup_hash_0_dedup_hash_1_key_tf;
ALTER INDEX chronicle_usage_events_study_id_participant_id_idx
    RENAME TO chronicle_usage_events_study_id_participant_id_idx_tf;
ALTER INDEX chronicle_usage_events_study_id_participant_id_ts_idx
    RENAME TO chronicle_usage_events_study_id_participant_id_ts_idx_tf;

CREATE TABLE chronicle_usage_events (
    study_id          uuid              NOT NULL,
    participant_id    text              NOT NULL,
    app_package_name  text,
    interaction_type  text,
    event_type        integer           NOT NULL,
    event_timestamp   timestamptz,
    timezone          text,
    username          text,
    application_label text,
    uploaded_at       timestamptz       DEFAULT now(),
    dedup_hash_0      bigint            NOT NULL,
    dedup_hash_1      bigint            NOT NULL,
    UNIQUE (dedup_hash_0, dedup_hash_1)
);

INSERT INTO chronicle_usage_events (
    study_id, participant_id, app_package_name, interaction_type, event_type,
    event_timestamp, timezone, username, application_label, uploaded_at,
    dedup_hash_0, dedup_hash_1
)
SELECT
    study_id::uuid, participant_id, app_package_name, interaction_type, event_type,
    event_timestamp, timezone, username, application_label, uploaded_at,
    dedup_hash_0, dedup_hash_1
FROM chronicle_usage_events_type_fix;

COMMIT;

-- --------------------------------------------------------------------------
-- 2. chronicle_usage_stats
-- --------------------------------------------------------------------------
BEGIN;

ALTER TABLE chronicle_usage_stats RENAME TO chronicle_usage_stats_type_fix;
ALTER TABLE chronicle_usage_stats_type_fix
    RENAME CONSTRAINT chronicle_usage_stats_dedup_hash_0_dedup_hash_1_key
    TO chronicle_usage_stats_dedup_hash_0_dedup_hash_1_key_tf;

CREATE TABLE chronicle_usage_stats (
    study_id          uuid              NOT NULL,
    participant_id    text              NOT NULL,
    app_package_name  text,
    interaction_type  text,
    start_time        timestamptz,
    end_time          timestamptz,
    duration          bigint,
    event_timestamp   timestamptz,
    timezone          text,
    application_label text,
    dedup_hash_0      bigint            NOT NULL,
    dedup_hash_1      bigint            NOT NULL,
    UNIQUE (dedup_hash_0, dedup_hash_1)
);

INSERT INTO chronicle_usage_stats (
    study_id, participant_id, app_package_name, interaction_type,
    start_time, end_time, duration, event_timestamp, timezone, application_label,
    dedup_hash_0, dedup_hash_1
)
SELECT
    study_id::uuid, participant_id, app_package_name, interaction_type,
    start_time, end_time, duration, event_timestamp, timezone, application_label,
    dedup_hash_0, dedup_hash_1
FROM chronicle_usage_stats_type_fix;

COMMIT;

-- --------------------------------------------------------------------------
-- 3. audit  (no dedup hash, no explicit indices)
--
-- acl_key moves from varchar(256) to uuid[]. The source format is the
-- AclKey.index serialization: hyphen-stripped 32-char-per-uuid chunks
-- concatenated (e.g. "aaaaaaaabbbbccccddddeeeeeeeeeeee" for a single
-- uuid, 64 chars for two, etc). We split it back on 32-char boundaries
-- and re-insert the hyphens at positions 8/12/16/20.
-- --------------------------------------------------------------------------
BEGIN;

ALTER TABLE audit RENAME TO audit_type_fix;

CREATE TABLE audit (
    acl_key          uuid[]         NOT NULL,
    id               uuid           NOT NULL,
    principal_type   varchar(128)   NOT NULL,
    principal_id     varchar(256)   NOT NULL,
    audit_event_type varchar(256)   NOT NULL,
    study_id         uuid           NOT NULL,
    organization_id  uuid           NOT NULL,
    description      varchar(256)   NOT NULL,
    data             varchar(65535) NOT NULL,
    event_timestamp  timestamptz
);

-- Session-scoped helper: parse a serialized acl_key into uuid[].
-- Tolerates both formats seen in historical data:
--   * hyphen-stripped concat-hex as written by AclKey.index (32 chars per uuid)
--   * canonical UUIDs with hyphens, possibly concatenated (36 chars each,
--     sometimes with an extra separator dash between them)
-- by stripping all hyphens first, then chunking on 32-char boundaries.
CREATE OR REPLACE FUNCTION pg_temp.acl_key_hex_to_uuid_array(s text)
RETURNS uuid[] AS $$
    SELECT COALESCE(
        (
            SELECT array_agg(
                (substring(h FROM i      FOR 8)  || '-' ||
                 substring(h FROM i + 8  FOR 4)  || '-' ||
                 substring(h FROM i + 12 FOR 4)  || '-' ||
                 substring(h FROM i + 16 FOR 4)  || '-' ||
                 substring(h FROM i + 20 FOR 12))::uuid
                ORDER BY i
            )
            FROM (SELECT replace(s, '-', '') AS h) cleaned,
                 generate_series(1, length(cleaned.h), 32) AS i
        ),
        ARRAY[]::uuid[]
    )
$$ LANGUAGE SQL IMMUTABLE;

INSERT INTO audit (
    acl_key, id, principal_type, principal_id, audit_event_type,
    study_id, organization_id, description, data, event_timestamp
)
SELECT
    pg_temp.acl_key_hex_to_uuid_array(acl_key),
    id::uuid, principal_type, principal_id, audit_event_type,
    study_id::uuid, organization_id::uuid, description, data, event_timestamp
FROM audit_type_fix;

COMMIT;

-- --------------------------------------------------------------------------
-- 4. sensor_data  (iOS)
-- --------------------------------------------------------------------------
BEGIN;

ALTER TABLE sensor_data RENAME TO sensor_data_type_fix;
ALTER TABLE sensor_data_type_fix
    RENAME CONSTRAINT sensor_data_dedup_hash_0_dedup_hash_1_key
    TO sensor_data_dedup_hash_0_dedup_hash_1_key_tf;
ALTER INDEX sensor_data_study_id_participant_id_idx
    RENAME TO sensor_data_study_id_participant_id_idx_tf;
ALTER INDEX sensor_data_study_id_participant_id_recordeddate_idx
    RENAME TO sensor_data_study_id_participant_id_recordeddate_idx_tf;

CREATE TABLE sensor_data (
    -- shared
    study_id            uuid             NOT NULL,
    participant_id      text             NOT NULL,
    sample_id           uuid             NOT NULL,
    sensor_type         text             NOT NULL,
    sample_duration     double precision NOT NULL,
    recordeddate        timestamptz,
    datetimestart       timestamptz,
    datetimeend         timestamptz,
    timezone            text,
    device_version      text             NOT NULL,
    device_name         text             NOT NULL,
    device_model        text             NOT NULL,
    device_system_name  text             NOT NULL,
    -- device usage
    total_screen_wakes           integer,
    total_unlock_duration        double precision,
    total_unlocks                integer,
    app_category                 text,
    app_usage_time               double precision,
    text_input_source            text,
    text_input_duration          double precision,
    bundle_identifier            text,
    app_category_web_duration    double precision,
    -- phone usage
    total_incoming_calls         integer,
    total_outgoing_calls         integer,
    total_call_duration          double precision,
    total_unique_contacts        integer,
    -- messages usage
    total_incoming_messages      integer,
    total_outgoing_messages      integer,
    -- keyboard metrics
    total_words                       integer,
    total_altered_words               integer,
    total_taps                        integer,
    total_drags                       integer,
    total_deletes                     integer,
    total_emojis                      integer,
    total_paths                       integer,
    total_path_time                   double precision,
    total_path_length                 double precision,
    total_autocorrections             integer,
    total_space_corrections           integer,
    total_transposition_corrections   integer,
    insert_key_corrections            integer,
    total_retro_corrections           integer,
    total_skip_touch_corrections      integer,
    total_near_key_corrections        integer,
    total_substitution_corrections    integer,
    total_test_hit_corrections        integer,
    total_typing_duration             double precision,
    total_path_pauses                 integer,
    total_pauses                      integer,
    total_typing_episodes             integer,
    sentiment                         text,
    sentiment_word_count              integer,
    sentiment_emoji_count             integer,
    typing_speed                      double precision,
    path_typing_speed                 double precision,
    -- utility
    exact_recordeddate  timestamptz,
    -- dedup
    dedup_hash_0        bigint NOT NULL,
    dedup_hash_1        bigint NOT NULL,
    UNIQUE (dedup_hash_0, dedup_hash_1)
);

INSERT INTO sensor_data (
    study_id, participant_id, sample_id, sensor_type, sample_duration,
    recordeddate, datetimestart, datetimeend, timezone,
    device_version, device_name, device_model, device_system_name,
    total_screen_wakes, total_unlock_duration, total_unlocks, app_category, app_usage_time,
    text_input_source, text_input_duration, bundle_identifier, app_category_web_duration,
    total_incoming_calls, total_outgoing_calls, total_call_duration, total_unique_contacts,
    total_incoming_messages, total_outgoing_messages,
    total_words, total_altered_words, total_taps, total_drags, total_deletes, total_emojis,
    total_paths, total_path_time, total_path_length, total_autocorrections,
    total_space_corrections, total_transposition_corrections, insert_key_corrections,
    total_retro_corrections, total_skip_touch_corrections, total_near_key_corrections,
    total_substitution_corrections, total_test_hit_corrections, total_typing_duration,
    total_path_pauses, total_pauses, total_typing_episodes,
    sentiment, sentiment_word_count, sentiment_emoji_count,
    typing_speed, path_typing_speed, exact_recordeddate,
    dedup_hash_0, dedup_hash_1
)
SELECT
    study_id::uuid, participant_id, sample_id::uuid, sensor_type, sample_duration,
    recordeddate, datetimestart, datetimeend, timezone,
    device_version, device_name, device_model, device_system_name,
    total_screen_wakes, total_unlock_duration, total_unlocks, app_category, app_usage_time,
    text_input_source, text_input_duration, bundle_identifier, app_category_web_duration,
    total_incoming_calls, total_outgoing_calls, total_call_duration, total_unique_contacts,
    total_incoming_messages, total_outgoing_messages,
    total_words, total_altered_words, total_taps, total_drags, total_deletes, total_emojis,
    total_paths, total_path_time, total_path_length, total_autocorrections,
    total_space_corrections, total_transposition_corrections, insert_key_corrections,
    total_retro_corrections, total_skip_touch_corrections, total_near_key_corrections,
    total_substitution_corrections, total_test_hit_corrections, total_typing_duration,
    total_path_pauses, total_pauses, total_typing_episodes,
    sentiment, sentiment_word_count, sentiment_emoji_count,
    typing_speed, path_typing_speed, exact_recordeddate,
    dedup_hash_0, dedup_hash_1
FROM sensor_data_type_fix;

COMMIT;

-- ==========================================================================
-- Index rebuild (run each in its own connection; CONCURRENTLY cannot run
-- inside a transaction block).
-- ==========================================================================

CREATE INDEX CONCURRENTLY IF NOT EXISTS chronicle_usage_events_study_id_participant_id_idx
    ON chronicle_usage_events (study_id, participant_id);

CREATE INDEX CONCURRENTLY IF NOT EXISTS chronicle_usage_events_study_id_participant_id_ts_idx
    ON chronicle_usage_events (study_id, participant_id, event_timestamp);

CREATE INDEX CONCURRENTLY IF NOT EXISTS sensor_data_study_id_participant_id_idx
    ON sensor_data (study_id, participant_id);

CREATE INDEX CONCURRENTLY IF NOT EXISTS sensor_data_study_id_participant_id_recordeddate_idx
    ON sensor_data (study_id, participant_id, recordeddate);

-- ==========================================================================
-- Cleanup (after verifying the new tables):
--
--   DROP TABLE chronicle_usage_events_type_fix;
--   DROP TABLE chronicle_usage_stats_type_fix;
--   DROP TABLE audit_type_fix;
--   DROP TABLE sensor_data_type_fix;
--
-- Rollback before cleanup (if needed, reverse one table at a time):
--
--   BEGIN;
--   DROP TABLE chronicle_usage_events;
--   ALTER TABLE chronicle_usage_events_type_fix RENAME TO chronicle_usage_events;
--   ALTER TABLE chronicle_usage_events
--       RENAME CONSTRAINT chronicle_usage_events_dedup_hash_0_dedup_hash_1_key_tf
--       TO chronicle_usage_events_dedup_hash_0_dedup_hash_1_key;
--   ALTER INDEX chronicle_usage_events_study_id_participant_id_idx_tf
--       RENAME TO chronicle_usage_events_study_id_participant_id_idx;
--   ALTER INDEX chronicle_usage_events_study_id_participant_id_ts_idx_tf
--       RENAME TO chronicle_usage_events_study_id_participant_id_ts_idx;
--   COMMIT;
-- ==========================================================================
