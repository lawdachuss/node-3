-- Migration: 20261101020000_dedup_policies_and_indexes.sql
--
-- Addresses Supabase Database Linter findings:
--   - 0006 multiple_permissive_policies (WARN)
--     * performer_follows (SELECT, authenticated)
--     * recordings (SELECT, anon & authenticated)
--     * requests (SELECT, authenticated)
--     * upload_links (SELECT, anon & authenticated)
--   - 0009 duplicate_index (WARN)
--     * watch_history (idx_watch_history_continue vs watch_history_user_watched_at_desc_idx)
--
-- Idempotent: safe to re-run.

-- 1. Recordings: remove duplicate public_read_recordings (keep recordings_public_select)
DROP POLICY IF EXISTS "public_read_recordings" ON public.recordings;

-- 2. Upload Links: remove duplicate public_read_upload_links (keep upload_links_public_select)
DROP POLICY IF EXISTS "public_read_upload_links" ON public.upload_links;

-- 3. Performer Follows: user_owns_rows covers authenticated SELECT/INSERT/UPDATE/DELETE; drop broad public_select
DROP POLICY IF EXISTS "performer_follows_public_select" ON public.performer_follows;

-- 4. Requests: user_owns_rows covers authenticated SELECT/INSERT/UPDATE/DELETE; drop broad public_select
DROP POLICY IF EXISTS "requests_public_select" ON public.requests;

-- 5. Watch History: drop identical duplicate index (keep idx_watch_history_continue)
DROP INDEX IF EXISTS public.watch_history_user_watched_at_desc_idx;

NOTIFY pgrst, 'reload schema';
