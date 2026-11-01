-- Create the channel_logs telemetry table.
--
-- Why this file exists: channel_logs was created by hand on the original live
-- database and never captured in a committed migration, so a fresh/empty
-- Supabase project had no such table while later migrations (for example
-- 20260814000003_add_channel_logs_instance_id.sql) and the Go log forwarder
-- (channel.Channel.persistLog -> database.Client.SaveLog) all assume it.
--
-- Column set mirrors database.ChannelLog in backend/database/supabase.go:
--   id, channel_id, username, log_level, message, instance_id, created_at.
--
-- The filename is timestamped before 20260814000003 so a replay in filename
-- order creates the table before the ALTER that adds instance_id.
--
-- Idempotent: safe to re-run.

CREATE TABLE IF NOT EXISTS public.channel_logs (
  id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  channel_id  uuid,
  username    varchar(255) NOT NULL,
  log_level   text NOT NULL DEFAULT 'info'::text,
  message     text NOT NULL,
  instance_id text,
  created_at  timestamptz NOT NULL DEFAULT now()
);

-- Per-channel log reads (GetLogs): newest first, limited.
CREATE INDEX IF NOT EXISTS idx_channel_logs_username_created
  ON public.channel_logs (username, created_at DESC);

-- Retention pruning (DeleteChannelLogsBefore) scans by created_at.
CREATE INDEX IF NOT EXISTS idx_channel_logs_created
  ON public.channel_logs (created_at);

-- Level filtering for diagnostics.
CREATE INDEX IF NOT EXISTS idx_channel_logs_level_created
  ON public.channel_logs (log_level, created_at DESC);

-- RLS: the DVR/coordinator write with the service_role key and browser/admin
-- reads are public, matching the policy set in
-- 20260814000002_harden_rls_and_functions.sql (which drops and re-creates these
-- same policies, so either order ends in the same state).
ALTER TABLE public.channel_logs ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "channel_logs_public_select" ON public.channel_logs;
CREATE POLICY "channel_logs_public_select" ON public.channel_logs
  FOR SELECT TO anon, authenticated USING (true);

DROP POLICY IF EXISTS "channel_logs_service_all" ON public.channel_logs;
CREATE POLICY "channel_logs_service_all" ON public.channel_logs
  FOR ALL TO service_role USING (true) WITH CHECK (true);
