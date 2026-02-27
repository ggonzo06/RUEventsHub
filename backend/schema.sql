-- RU Events Hub: events table
-- Run this once in the Supabase SQL editor to set up your schema.

CREATE TABLE IF NOT EXISTS events (
    id          uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id    text        UNIQUE NOT NULL,
    source      text        NOT NULL,
    title       text        NOT NULL,
    description text,
    start_time  timestamptz NOT NULL,
    end_time    timestamptz,
    location    text,
    campus      text,
    organization text,
    category    text,
    source_url  text        NOT NULL,
    last_seen   timestamptz NOT NULL DEFAULT now(),
    created_at  timestamptz NOT NULL DEFAULT now()
);

-- Useful indexes
CREATE INDEX IF NOT EXISTS events_source_idx     ON events (source);
CREATE INDEX IF NOT EXISTS events_campus_idx     ON events (campus);
CREATE INDEX IF NOT EXISTS events_start_time_idx ON events (start_time);

-- Allow the scraper (anon key) to read and write.
-- If you have RLS enabled, add the policy below; otherwise skip it.
-- ALTER TABLE events ENABLE ROW LEVEL SECURITY;
-- CREATE POLICY "anon can read events"  ON events FOR SELECT USING (true);
-- CREATE POLICY "anon can upsert events" ON events FOR INSERT WITH CHECK (true);
-- CREATE POLICY "anon can update events" ON events FOR UPDATE USING (true);
