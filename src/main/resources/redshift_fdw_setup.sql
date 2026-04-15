-- ============================================================================
-- Foreign Data Wrapper Setup: Aurora -> Redshift
-- ============================================================================
-- This sets up postgres_fdw so Aurora can read from Redshift tables directly.
-- Run on the Aurora/Postgres instance.
--
-- Fill in your Redshift connection details in Step 2 and Step 3.
-- ============================================================================

-- Step 1: Install extensions
CREATE EXTENSION IF NOT EXISTS postgres_fdw;

-- Step 2: Create foreign server pointing to Redshift
-- Replace host, port, dbname with your Redshift cluster details.
CREATE SERVER IF NOT EXISTS redshift
    FOREIGN DATA WRAPPER postgres_fdw
    OPTIONS (
        host 'your-cluster.region.redshift.amazonaws.com',
        port '5439',
        dbname 'your_database',
        fetch_size '10000'
    );

-- Step 3: Create user mapping
-- Replace user/password with Redshift credentials.
CREATE USER MAPPING IF NOT EXISTS FOR current_user
    SERVER redshift
    OPTIONS (
        user 'redshift_user',
        password 'redshift_password'
    );

-- Step 4: Create foreign schema
CREATE SCHEMA IF NOT EXISTS redshift_fdw;

-- Step 5: Create foreign tables
-- These match the Redshift table schemas exactly.

-- 5a: chronicle_usage_events
CREATE FOREIGN TABLE IF NOT EXISTS redshift_fdw.chronicle_usage_events (
    study_id        varchar(36) NOT NULL,
    participant_id  text NOT NULL,
    app_package_name text,
    interaction_type text,
    event_type      integer NOT NULL,
    event_timestamp timestamptz,
    timezone        text,
    username        text,
    application_label text,
    uploaded_at     timestamptz
)
SERVER redshift
OPTIONS (schema_name 'public', table_name 'chronicle_usage_events');

-- 5b: chronicle_usage_stats
CREATE FOREIGN TABLE IF NOT EXISTS redshift_fdw.chronicle_usage_stats (
    study_id        varchar(36) NOT NULL,
    participant_id  text NOT NULL,
    app_package_name text,
    interaction_type text,
    start_time      timestamptz,
    end_time        timestamptz,
    duration        bigint,
    event_timestamp timestamptz,
    timezone        text,
    application_label text
)
SERVER redshift
OPTIONS (schema_name 'public', table_name 'chronicle_usage_stats');

-- 5c: sensor_data (iOS)
CREATE FOREIGN TABLE IF NOT EXISTS redshift_fdw.sensor_data (
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
    exact_recordeddate  timestamptz
)
SERVER redshift
OPTIONS (schema_name 'public', table_name 'sensor_data');

-- 5d: audit
CREATE FOREIGN TABLE IF NOT EXISTS redshift_fdw.audit (
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
)
SERVER redshift
OPTIONS (schema_name 'public', table_name 'audit');

-- Step 6: Verify access
-- Run these to confirm the foreign tables are accessible:

-- SELECT count(*) FROM redshift_fdw.chronicle_usage_events LIMIT 1;
-- SELECT count(*) FROM redshift_fdw.chronicle_usage_stats LIMIT 1;
-- SELECT count(*) FROM redshift_fdw.sensor_data LIMIT 1;
-- SELECT count(*) FROM redshift_fdw.audit LIMIT 1;

-- Step 7: Tune for large migrations (optional)
-- Increase fetch_size for better throughput on large scans:
-- ALTER SERVER redshift OPTIONS (SET fetch_size '50000');
--
-- If you hit timeouts on large batches:
-- ALTER SERVER redshift OPTIONS (ADD connect_timeout '30');
