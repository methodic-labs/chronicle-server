-- ============================================================================
-- Redshift to Aurora (Postgres) Migration via Foreign Data Wrapper
-- ============================================================================
-- Prerequisites:
--   1. postgres_fdw extension installed and foreign tables set up (see redshift_fdw_setup.sql)
--   2. dedup_hash columns added to target tables (see ALTER TABLE statements below)
--
-- Uses Postgres built-in hashtextextended(text, bigint) with two seeds (0, 1)
-- to produce a 128-bit salted hash for dedup. No extensions required beyond postgres_fdw.
--
-- Run these statements in order. Adjust foreign schema name, batch sizes,
-- and timestamp ranges as needed. Safe to re-run (idempotent).
-- ============================================================================

-- Step 0: Create event tables on Aurora
-- These tables previously only existed on Redshift. Create them on Aurora with
-- dedup_hash columns and unique index included from the start.
-- IF NOT EXISTS makes this safe to re-run.

CREATE TABLE IF NOT EXISTS chronicle_usage_events (
    study_id            varchar(36) NOT NULL,
    participant_id      text NOT NULL,
    app_package_name    text,
    interaction_type    text,
    event_type          integer,
    event_timestamp     timestamptz,
    timezone            text,
    username            text,
    application_label   text,
    uploaded_at         timestamptz DEFAULT now(),
    dedup_hash_0        bigint NOT NULL DEFAULT 0,
    dedup_hash_1        bigint NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS chronicle_usage_stats (
    study_id            varchar(36) NOT NULL,
    participant_id      text NOT NULL,
    app_package_name    text,
    interaction_type    text,
    start_time          timestamptz,
    end_time            timestamptz,
    duration            bigint,
    event_timestamp     timestamptz,
    timezone            text,
    application_label   text,
    dedup_hash_0        bigint NOT NULL DEFAULT 0,
    dedup_hash_1        bigint NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS sensor_data (
    -- shared sensor columns
    study_id            varchar(36) NOT NULL,
    participant_id      text NOT NULL,
    sample_id           varchar(36) NOT NULL,
    sensor_type         text NOT NULL,
    sample_duration     double precision NOT NULL,
    recordeddate        timestamptz,
    datetimestart       timestamptz,
    datetimeend         timestamptz,
    timezone            text,
    device_version      text NOT NULL,
    device_name         text NOT NULL,
    device_model        text NOT NULL,
    device_system_name  text NOT NULL,
    -- device usage
    total_screen_wakes  integer,
    total_unlock_duration double precision,
    total_unlocks       integer,
    app_category        text,
    app_usage_time      double precision,
    text_input_source   text,
    text_input_duration double precision,
    bundle_identifier   text,
    app_category_web_duration double precision,
    -- phone usage
    total_incoming_calls integer,
    total_outgoing_calls integer,
    total_call_duration double precision,
    total_unique_contacts integer,
    -- messages usage
    total_incoming_messages integer,
    total_outgoing_messages integer,
    -- keyboard metrics
    total_words         integer,
    total_altered_words integer,
    total_taps          integer,
    total_drags         integer,
    total_deletes       integer,
    total_emojis        integer,
    total_paths         integer,
    total_path_time     double precision,
    total_path_length   double precision,
    total_autocorrections integer,
    total_space_corrections integer,
    total_transposition_corrections integer,
    insert_key_corrections integer,
    total_retro_corrections integer,
    total_skip_touch_corrections integer,
    total_near_key_corrections integer,
    total_substitution_corrections integer,
    total_test_hit_corrections integer,
    total_typing_duration double precision,
    total_path_pauses   integer,
    total_pauses        integer,
    total_typing_episodes integer,
    sentiment           text,
    sentiment_word_count integer,
    sentiment_emoji_count integer,
    typing_speed        double precision,
    path_typing_speed   double precision,
    -- utility
    exact_recordeddate  timestamptz,
    -- dedup
    dedup_hash_0        bigint NOT NULL DEFAULT 0,
    dedup_hash_1        bigint NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS audit (
    acl_key             varchar(256) NOT NULL,
    id                  varchar(36) NOT NULL,
    principal_type      varchar(128) NOT NULL,
    principal_id        varchar(256) NOT NULL,
    audit_event_type    varchar(256) NOT NULL,
    study_id            varchar(36) NOT NULL,
    organization_id     varchar(36) NOT NULL,
    description         varchar(256) NOT NULL,
    data                varchar(65535) NOT NULL,
    event_timestamp     timestamptz
);

-- Step 1: Add dedup hash columns to existing tables (no-op if Step 0 already created them)
-- Column additions are instant (metadata-only) in Postgres 11+ with a non-volatile default.
-- Safe to run while the server is active — existing queries use explicit column lists.
-- DO NOT create indexes here — all rows default to (0,0) which violates uniqueness.
-- Indexes are created in Step 2b AFTER backfilling hashes.

ALTER TABLE chronicle_usage_events ADD COLUMN IF NOT EXISTS dedup_hash_0 bigint NOT NULL DEFAULT 0;
ALTER TABLE chronicle_usage_events ADD COLUMN IF NOT EXISTS dedup_hash_1 bigint NOT NULL DEFAULT 0;

ALTER TABLE chronicle_usage_stats ADD COLUMN IF NOT EXISTS dedup_hash_0 bigint NOT NULL DEFAULT 0;
ALTER TABLE chronicle_usage_stats ADD COLUMN IF NOT EXISTS dedup_hash_1 bigint NOT NULL DEFAULT 0;

ALTER TABLE sensor_data ADD COLUMN IF NOT EXISTS dedup_hash_0 bigint NOT NULL DEFAULT 0;
ALTER TABLE sensor_data ADD COLUMN IF NOT EXISTS dedup_hash_1 bigint NOT NULL DEFAULT 0;

-- Step 2: Backfill dedup hashes for any existing rows in Aurora
-- (These may take a while on large tables; consider batching with WHERE clauses)

UPDATE chronicle_usage_events
SET dedup_hash_0 = hashtextextended(concat_ws(chr(31),
        study_id::text, participant_id::text, app_package_name::text,
        interaction_type::text, event_type::text, event_timestamp::text,
        timezone::text, username::text, application_label::text
    ), 0),
    dedup_hash_1 = hashtextextended(concat_ws(chr(31),
        study_id::text, participant_id::text, app_package_name::text,
        interaction_type::text, event_type::text, event_timestamp::text,
        timezone::text, username::text, application_label::text
    ), 1)
WHERE dedup_hash_0 = 0 AND dedup_hash_1 = 0;

UPDATE sensor_data
SET dedup_hash_0 = hashtextextended(concat_ws(chr(31),
        study_id::text, participant_id::text, sensor_type::text,
        sample_duration::text, recordeddate::text, timezone::text,
        device_version::text, device_name::text, device_model::text,
        device_system_name::text,
        total_screen_wakes::text, total_unlock_duration::text, total_unlocks::text,
        app_category::text, app_usage_time::text, text_input_source::text,
        text_input_duration::text, bundle_identifier::text, app_category_web_duration::text,
        total_incoming_calls::text, total_outgoing_calls::text, total_call_duration::text,
        total_unique_contacts::text,
        total_incoming_messages::text, total_outgoing_messages::text,
        total_words::text, total_altered_words::text, total_taps::text,
        total_drags::text, total_deletes::text, total_emojis::text,
        total_paths::text, total_path_time::text, total_path_length::text,
        total_autocorrections::text, total_space_corrections::text,
        total_transposition_corrections::text, insert_key_corrections::text,
        total_retro_corrections::text, total_skip_touch_corrections::text,
        total_near_key_corrections::text, total_substitution_corrections::text,
        total_test_hit_corrections::text, total_typing_duration::text,
        total_path_pauses::text, total_pauses::text, total_typing_episodes::text,
        sentiment::text, sentiment_word_count::text, sentiment_emoji_count::text,
        typing_speed::text, path_typing_speed::text
    ), 0),
    dedup_hash_1 = hashtextextended(concat_ws(chr(31),
        study_id::text, participant_id::text, sensor_type::text,
        sample_duration::text, recordeddate::text, timezone::text,
        device_version::text, device_name::text, device_model::text,
        device_system_name::text,
        total_screen_wakes::text, total_unlock_duration::text, total_unlocks::text,
        app_category::text, app_usage_time::text, text_input_source::text,
        text_input_duration::text, bundle_identifier::text, app_category_web_duration::text,
        total_incoming_calls::text, total_outgoing_calls::text, total_call_duration::text,
        total_unique_contacts::text,
        total_incoming_messages::text, total_outgoing_messages::text,
        total_words::text, total_altered_words::text, total_taps::text,
        total_drags::text, total_deletes::text, total_emojis::text,
        total_paths::text, total_path_time::text, total_path_length::text,
        total_autocorrections::text, total_space_corrections::text,
        total_transposition_corrections::text, insert_key_corrections::text,
        total_retro_corrections::text, total_skip_touch_corrections::text,
        total_near_key_corrections::text, total_substitution_corrections::text,
        total_test_hit_corrections::text, total_typing_duration::text,
        total_path_pauses::text, total_pauses::text, total_typing_episodes::text,
        sentiment::text, sentiment_word_count::text, sentiment_emoji_count::text,
        typing_speed::text, path_typing_speed::text
    ), 1)
WHERE dedup_hash_0 = 0 AND dedup_hash_1 = 0;

-- Step 2b: Create unique indexes AFTER backfill
-- Uses CONCURRENTLY to avoid locking tables while the running server is active.
-- NOTE: CONCURRENTLY cannot run inside a transaction block — run each statement individually.

CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS chronicle_usage_events_dedup_idx ON chronicle_usage_events (dedup_hash_0, dedup_hash_1);
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS chronicle_usage_stats_dedup_idx ON chronicle_usage_stats (dedup_hash_0, dedup_hash_1);
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS sensor_data_dedup_idx ON sensor_data (dedup_hash_0, dedup_hash_1);

-- Step 3: Migrate chronicle_usage_events
-- Run in batches by timestamp range. Adjust date range as needed.
-- ROW_NUMBER deduplicates at source, taking the latest upload per unique event.

WITH ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY study_id, participant_id, app_package_name, interaction_type,
                         event_type, event_timestamp, timezone, username, application_label
            ORDER BY uploaded_at DESC NULLS LAST
        ) as rn
    FROM redshift_fdw.chronicle_usage_events
    WHERE event_timestamp >= '2020-01-01'::timestamptz
      AND event_timestamp <  '2020-02-01'::timestamptz
)
INSERT INTO chronicle_usage_events (
    study_id, participant_id, app_package_name, interaction_type, event_type,
    event_timestamp, timezone, username, application_label, uploaded_at,
    dedup_hash_0, dedup_hash_1
)
SELECT
    study_id, participant_id, app_package_name, interaction_type, event_type,
    event_timestamp, timezone, username, application_label, uploaded_at,
    hashtextextended(concat_ws(chr(31),
        study_id::text, participant_id::text, app_package_name::text,
        interaction_type::text, event_type::text, event_timestamp::text,
        timezone::text, username::text, application_label::text
    ), 0),
    hashtextextended(concat_ws(chr(31),
        study_id::text, participant_id::text, app_package_name::text,
        interaction_type::text, event_type::text, event_timestamp::text,
        timezone::text, username::text, application_label::text
    ), 1)
FROM ranked
WHERE rn = 1
ON CONFLICT (dedup_hash_0, dedup_hash_1) DO UPDATE SET uploaded_at = EXCLUDED.uploaded_at
WHERE chronicle_usage_events.study_id IS NOT DISTINCT FROM EXCLUDED.study_id
  AND chronicle_usage_events.participant_id IS NOT DISTINCT FROM EXCLUDED.participant_id
  AND chronicle_usage_events.app_package_name IS NOT DISTINCT FROM EXCLUDED.app_package_name
  AND chronicle_usage_events.interaction_type IS NOT DISTINCT FROM EXCLUDED.interaction_type
  AND chronicle_usage_events.event_type IS NOT DISTINCT FROM EXCLUDED.event_type
  AND chronicle_usage_events.event_timestamp IS NOT DISTINCT FROM EXCLUDED.event_timestamp
  AND chronicle_usage_events.timezone IS NOT DISTINCT FROM EXCLUDED.timezone
  AND chronicle_usage_events.username IS NOT DISTINCT FROM EXCLUDED.username
  AND chronicle_usage_events.application_label IS NOT DISTINCT FROM EXCLUDED.application_label;

-- Step 4: Migrate chronicle_usage_stats

WITH ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY study_id, participant_id, app_package_name, interaction_type,
                         start_time, end_time, duration, event_timestamp, timezone, application_label
            ORDER BY event_timestamp DESC NULLS LAST
        ) as rn
    FROM redshift_fdw.chronicle_usage_stats
    WHERE event_timestamp >= '2020-01-01'::timestamptz
      AND event_timestamp <  '2020-02-01'::timestamptz
)
INSERT INTO chronicle_usage_stats (
    study_id, participant_id, app_package_name, interaction_type,
    start_time, end_time, duration, event_timestamp, timezone, application_label,
    dedup_hash_0, dedup_hash_1
)
SELECT
    study_id, participant_id, app_package_name, interaction_type,
    start_time, end_time, duration, event_timestamp, timezone, application_label,
    hashtextextended(concat_ws(chr(31),
        study_id::text, participant_id::text, app_package_name::text,
        interaction_type::text, start_time::text, end_time::text,
        duration::text, event_timestamp::text, timezone::text, application_label::text
    ), 0),
    hashtextextended(concat_ws(chr(31),
        study_id::text, participant_id::text, app_package_name::text,
        interaction_type::text, start_time::text, end_time::text,
        duration::text, event_timestamp::text, timezone::text, application_label::text
    ), 1)
FROM ranked
WHERE rn = 1
ON CONFLICT (dedup_hash_0, dedup_hash_1) DO NOTHING;

-- Step 5: Migrate sensor_data (iOS)
-- ROW_NUMBER deduplicates at source, taking latest recording per unique sensor sample.

WITH ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY study_id, participant_id, sensor_type, sample_duration,
                         recordeddate, timezone,
                         device_version, device_name, device_model, device_system_name,
                         total_screen_wakes, total_unlock_duration, total_unlocks,
                         app_category, app_usage_time, text_input_source, text_input_duration,
                         bundle_identifier, app_category_web_duration,
                         total_incoming_calls, total_outgoing_calls, total_call_duration, total_unique_contacts,
                         total_incoming_messages, total_outgoing_messages,
                         total_words, total_altered_words, total_taps, total_drags, total_deletes,
                         total_emojis, total_paths, total_path_time, total_path_length,
                         total_autocorrections, total_space_corrections, total_transposition_corrections,
                         insert_key_corrections, total_retro_corrections, total_skip_touch_corrections,
                         total_near_key_corrections, total_substitution_corrections, total_test_hit_corrections,
                         total_typing_duration, total_path_pauses, total_pauses, total_typing_episodes,
                         sentiment, sentiment_word_count, sentiment_emoji_count,
                         typing_speed, path_typing_speed
            ORDER BY exact_recordeddate DESC NULLS LAST
        ) as rn
    FROM redshift_fdw.sensor_data
    WHERE recordeddate >= '2020-01-01'::timestamptz
      AND recordeddate <  '2020-02-01'::timestamptz
)
INSERT INTO sensor_data (
    study_id, participant_id, sample_id, sensor_type, sample_duration,
    recordeddate, datetimestart, datetimeend, timezone,
    device_version, device_name, device_model, device_system_name,
    total_screen_wakes, total_unlock_duration, total_unlocks,
    app_category, app_usage_time, text_input_source, text_input_duration,
    bundle_identifier, app_category_web_duration,
    total_incoming_calls, total_outgoing_calls, total_call_duration, total_unique_contacts,
    total_incoming_messages, total_outgoing_messages,
    total_words, total_altered_words, total_taps, total_drags, total_deletes,
    total_emojis, total_paths, total_path_time, total_path_length,
    total_autocorrections, total_space_corrections, total_transposition_corrections,
    insert_key_corrections, total_retro_corrections, total_skip_touch_corrections,
    total_near_key_corrections, total_substitution_corrections, total_test_hit_corrections,
    total_typing_duration, total_path_pauses, total_pauses, total_typing_episodes,
    sentiment, sentiment_word_count, sentiment_emoji_count,
    typing_speed, path_typing_speed,
    exact_recordeddate,
    dedup_hash_0, dedup_hash_1
)
SELECT
    study_id, participant_id, sample_id, sensor_type, sample_duration,
    recordeddate, datetimestart, datetimeend, timezone,
    device_version, device_name, device_model, device_system_name,
    total_screen_wakes, total_unlock_duration, total_unlocks,
    app_category, app_usage_time, text_input_source, text_input_duration,
    bundle_identifier, app_category_web_duration,
    total_incoming_calls, total_outgoing_calls, total_call_duration, total_unique_contacts,
    total_incoming_messages, total_outgoing_messages,
    total_words, total_altered_words, total_taps, total_drags, total_deletes,
    total_emojis, total_paths, total_path_time, total_path_length,
    total_autocorrections, total_space_corrections, total_transposition_corrections,
    insert_key_corrections, total_retro_corrections, total_skip_touch_corrections,
    total_near_key_corrections, total_substitution_corrections, total_test_hit_corrections,
    total_typing_duration, total_path_pauses, total_pauses, total_typing_episodes,
    sentiment, sentiment_word_count, sentiment_emoji_count,
    typing_speed, path_typing_speed,
    exact_recordeddate,
    hashtextextended(concat_ws(chr(31),
        study_id::text, participant_id::text, sensor_type::text,
        sample_duration::text, recordeddate::text, timezone::text,
        device_version::text, device_name::text, device_model::text,
        device_system_name::text,
        total_screen_wakes::text, total_unlock_duration::text, total_unlocks::text,
        app_category::text, app_usage_time::text, text_input_source::text,
        text_input_duration::text, bundle_identifier::text, app_category_web_duration::text,
        total_incoming_calls::text, total_outgoing_calls::text, total_call_duration::text,
        total_unique_contacts::text,
        total_incoming_messages::text, total_outgoing_messages::text,
        total_words::text, total_altered_words::text, total_taps::text,
        total_drags::text, total_deletes::text, total_emojis::text,
        total_paths::text, total_path_time::text, total_path_length::text,
        total_autocorrections::text, total_space_corrections::text,
        total_transposition_corrections::text, insert_key_corrections::text,
        total_retro_corrections::text, total_skip_touch_corrections::text,
        total_near_key_corrections::text, total_substitution_corrections::text,
        total_test_hit_corrections::text, total_typing_duration::text,
        total_path_pauses::text, total_pauses::text, total_typing_episodes::text,
        sentiment::text, sentiment_word_count::text, sentiment_emoji_count::text,
        typing_speed::text, path_typing_speed::text
    ), 0),
    hashtextextended(concat_ws(chr(31),
        study_id::text, participant_id::text, sensor_type::text,
        sample_duration::text, recordeddate::text, timezone::text,
        device_version::text, device_name::text, device_model::text,
        device_system_name::text,
        total_screen_wakes::text, total_unlock_duration::text, total_unlocks::text,
        app_category::text, app_usage_time::text, text_input_source::text,
        text_input_duration::text, bundle_identifier::text, app_category_web_duration::text,
        total_incoming_calls::text, total_outgoing_calls::text, total_call_duration::text,
        total_unique_contacts::text,
        total_incoming_messages::text, total_outgoing_messages::text,
        total_words::text, total_altered_words::text, total_taps::text,
        total_drags::text, total_deletes::text, total_emojis::text,
        total_paths::text, total_path_time::text, total_path_length::text,
        total_autocorrections::text, total_space_corrections::text,
        total_transposition_corrections::text, insert_key_corrections::text,
        total_retro_corrections::text, total_skip_touch_corrections::text,
        total_near_key_corrections::text, total_substitution_corrections::text,
        total_test_hit_corrections::text, total_typing_duration::text,
        total_path_pauses::text, total_pauses::text, total_typing_episodes::text,
        sentiment::text, sentiment_word_count::text, sentiment_emoji_count::text,
        typing_speed::text, path_typing_speed::text
    ), 1)
FROM ranked
WHERE rn = 1
ON CONFLICT (dedup_hash_0, dedup_hash_1) DO UPDATE SET
    sample_id = LEAST(sensor_data.sample_id, EXCLUDED.sample_id),
    datetimestart = LEAST(sensor_data.datetimestart, EXCLUDED.datetimestart),
    datetimeend = GREATEST(sensor_data.datetimeend, EXCLUDED.datetimeend),
    exact_recordeddate = LEAST(sensor_data.exact_recordeddate, EXCLUDED.exact_recordeddate)
WHERE sensor_data.study_id IS NOT DISTINCT FROM EXCLUDED.study_id
  AND sensor_data.participant_id IS NOT DISTINCT FROM EXCLUDED.participant_id
  AND sensor_data.sensor_type IS NOT DISTINCT FROM EXCLUDED.sensor_type
  AND sensor_data.sample_duration IS NOT DISTINCT FROM EXCLUDED.sample_duration
  AND sensor_data.recordeddate IS NOT DISTINCT FROM EXCLUDED.recordeddate
  AND sensor_data.timezone IS NOT DISTINCT FROM EXCLUDED.timezone
  AND sensor_data.device_version IS NOT DISTINCT FROM EXCLUDED.device_version
  AND sensor_data.device_name IS NOT DISTINCT FROM EXCLUDED.device_name
  AND sensor_data.device_model IS NOT DISTINCT FROM EXCLUDED.device_model
  AND sensor_data.device_system_name IS NOT DISTINCT FROM EXCLUDED.device_system_name
  AND sensor_data.total_screen_wakes IS NOT DISTINCT FROM EXCLUDED.total_screen_wakes
  AND sensor_data.total_unlock_duration IS NOT DISTINCT FROM EXCLUDED.total_unlock_duration
  AND sensor_data.total_unlocks IS NOT DISTINCT FROM EXCLUDED.total_unlocks
  AND sensor_data.app_category IS NOT DISTINCT FROM EXCLUDED.app_category
  AND sensor_data.app_usage_time IS NOT DISTINCT FROM EXCLUDED.app_usage_time
  AND sensor_data.text_input_source IS NOT DISTINCT FROM EXCLUDED.text_input_source
  AND sensor_data.text_input_duration IS NOT DISTINCT FROM EXCLUDED.text_input_duration
  AND sensor_data.bundle_identifier IS NOT DISTINCT FROM EXCLUDED.bundle_identifier
  AND sensor_data.app_category_web_duration IS NOT DISTINCT FROM EXCLUDED.app_category_web_duration
  AND sensor_data.total_incoming_calls IS NOT DISTINCT FROM EXCLUDED.total_incoming_calls
  AND sensor_data.total_outgoing_calls IS NOT DISTINCT FROM EXCLUDED.total_outgoing_calls
  AND sensor_data.total_call_duration IS NOT DISTINCT FROM EXCLUDED.total_call_duration
  AND sensor_data.total_unique_contacts IS NOT DISTINCT FROM EXCLUDED.total_unique_contacts
  AND sensor_data.total_incoming_messages IS NOT DISTINCT FROM EXCLUDED.total_incoming_messages
  AND sensor_data.total_outgoing_messages IS NOT DISTINCT FROM EXCLUDED.total_outgoing_messages
  AND sensor_data.total_words IS NOT DISTINCT FROM EXCLUDED.total_words
  AND sensor_data.total_altered_words IS NOT DISTINCT FROM EXCLUDED.total_altered_words
  AND sensor_data.total_taps IS NOT DISTINCT FROM EXCLUDED.total_taps
  AND sensor_data.total_drags IS NOT DISTINCT FROM EXCLUDED.total_drags
  AND sensor_data.total_deletes IS NOT DISTINCT FROM EXCLUDED.total_deletes
  AND sensor_data.total_emojis IS NOT DISTINCT FROM EXCLUDED.total_emojis
  AND sensor_data.total_paths IS NOT DISTINCT FROM EXCLUDED.total_paths
  AND sensor_data.total_path_time IS NOT DISTINCT FROM EXCLUDED.total_path_time
  AND sensor_data.total_path_length IS NOT DISTINCT FROM EXCLUDED.total_path_length
  AND sensor_data.total_autocorrections IS NOT DISTINCT FROM EXCLUDED.total_autocorrections
  AND sensor_data.total_space_corrections IS NOT DISTINCT FROM EXCLUDED.total_space_corrections
  AND sensor_data.total_transposition_corrections IS NOT DISTINCT FROM EXCLUDED.total_transposition_corrections
  AND sensor_data.insert_key_corrections IS NOT DISTINCT FROM EXCLUDED.insert_key_corrections
  AND sensor_data.total_retro_corrections IS NOT DISTINCT FROM EXCLUDED.total_retro_corrections
  AND sensor_data.total_skip_touch_corrections IS NOT DISTINCT FROM EXCLUDED.total_skip_touch_corrections
  AND sensor_data.total_near_key_corrections IS NOT DISTINCT FROM EXCLUDED.total_near_key_corrections
  AND sensor_data.total_substitution_corrections IS NOT DISTINCT FROM EXCLUDED.total_substitution_corrections
  AND sensor_data.total_test_hit_corrections IS NOT DISTINCT FROM EXCLUDED.total_test_hit_corrections
  AND sensor_data.total_typing_duration IS NOT DISTINCT FROM EXCLUDED.total_typing_duration
  AND sensor_data.total_path_pauses IS NOT DISTINCT FROM EXCLUDED.total_path_pauses
  AND sensor_data.total_pauses IS NOT DISTINCT FROM EXCLUDED.total_pauses
  AND sensor_data.total_typing_episodes IS NOT DISTINCT FROM EXCLUDED.total_typing_episodes
  AND sensor_data.sentiment IS NOT DISTINCT FROM EXCLUDED.sentiment
  AND sensor_data.sentiment_word_count IS NOT DISTINCT FROM EXCLUDED.sentiment_word_count
  AND sensor_data.sentiment_emoji_count IS NOT DISTINCT FROM EXCLUDED.sentiment_emoji_count
  AND sensor_data.typing_speed IS NOT DISTINCT FROM EXCLUDED.typing_speed
  AND sensor_data.path_typing_speed IS NOT DISTINCT FROM EXCLUDED.path_typing_speed;

-- Step 6: Migrate audit events (append-only, no dedup)
INSERT INTO audit (
    acl_key, id, principal_type, principal_id, audit_event_type,
    study_id, organization_id, description, data, event_timestamp
)
SELECT
    acl_key, id, principal_type, principal_id, audit_event_type,
    study_id, organization_id, description, data, event_timestamp
FROM redshift_fdw.audit
WHERE event_timestamp >= '2020-01-01'::timestamptz
  AND event_timestamp <  '2020-02-01'::timestamptz
ON CONFLICT DO NOTHING;

-- Step 7: Verification queries
-- Compare row counts between Redshift (via FDW) and Aurora:

-- SELECT 'redshift' as source, count(*) FROM redshift_fdw.chronicle_usage_events
-- UNION ALL
-- SELECT 'aurora' as source, count(*) FROM chronicle_usage_events;

-- SELECT 'redshift' as source, count(*) FROM redshift_fdw.sensor_data
-- UNION ALL
-- SELECT 'aurora' as source, count(*) FROM sensor_data;

-- SELECT 'redshift' as source, count(*) FROM redshift_fdw.chronicle_usage_stats
-- UNION ALL
-- SELECT 'aurora' as source, count(*) FROM chronicle_usage_stats;
