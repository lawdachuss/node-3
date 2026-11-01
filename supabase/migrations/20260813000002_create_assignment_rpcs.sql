-- Create the assignment/lease RPCs that the Go backend calls but that no
-- committed migration ever defined.
--
-- Why this file exists: these five functions only lived in one-off SQL files
-- (backend/supabase/migrate_fix_rpcs.sql, backend/supabase/fix_assignment_rpc.sql,
-- backend/database/migrate_assignment_writes.sql) marked "Run ONCE in the SQL
-- editor". A fresh project therefore had no such functions at all:
--   POST /rest/v1/rpc/claim_controller_lease   -> 404
--   POST /rest/v1/rpc/release_controller_lease -> 404
--   POST /rest/v1/rpc/mark_channel_recording   -> 404
--   POST /rest/v1/rpc/reset_channel_status     -> 404
--   POST /rest/v1/rpc/reclaim_node_channels    -> 404
-- so no node could win the controller lease and channel state never advanced.
--
-- Deliberately NOT copied from those one-off files: claim_channels,
-- claim_specific_channel, reassign_channel and touch_channel_recordings. Those
-- have since been superseded by committed migrations (fair-share claim cap,
-- protected recording assignment, reassign reports moves) and re-creating the
-- older bodies here would silently regress that behaviour.
--
-- Every function is SECURITY DEFINER with an empty search_path and fully
-- schema-qualified references, matching the hardening migration. Writes go
-- through the functions so the anon API role never needs table write access.
--
-- Idempotent: safe to re-run.

-- ── 1) Leader lease ────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public.claim_controller_lease(p_node_id text, p_ttl int)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $$
DECLARE
  updated int;
BEGIN
  UPDATE public.controller_lease
     SET node_id     = p_node_id,
         acquired_at = now(),
         expires_at  = now() + (p_ttl || ' seconds')::interval
   WHERE id = 1
     AND (expires_at < now() OR node_id = p_node_id);
  GET DIAGNOSTICS updated = ROW_COUNT;

  IF updated = 0 THEN
    INSERT INTO public.controller_lease (id, node_id, expires_at)
    VALUES (1, p_node_id, now() + (p_ttl || ' seconds')::interval)
    ON CONFLICT (id) DO NOTHING;
    SELECT count(*) INTO updated
      FROM public.controller_lease
     WHERE id = 1 AND node_id = p_node_id AND expires_at > now();
    RETURN updated > 0;
  END IF;
  RETURN true;
END;
$$;
GRANT EXECUTE ON FUNCTION public.claim_controller_lease(text, int) TO anon;
GRANT EXECUTE ON FUNCTION public.claim_controller_lease(text, int) TO authenticated;

-- ── 2) Release lease on graceful shutdown ───────────────────────────────────
CREATE OR REPLACE FUNCTION public.release_controller_lease(p_node_id text)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $$
BEGIN
  UPDATE public.controller_lease
     SET expires_at = now() - interval '1 second'
   WHERE node_id = p_node_id;
END;
$$;
GRANT EXECUTE ON FUNCTION public.release_controller_lease(text) TO anon;
GRANT EXECUTE ON FUNCTION public.release_controller_lease(text) TO authenticated;

-- ── 3) Mark a channel as actively recording on its node ─────────────────────
-- The per-node assignment-sync loop calls this so the controller's rebalancer
-- (which refuses to move status='recording' rows) never yanks an in-progress
-- recording.
CREATE OR REPLACE FUNCTION public.mark_channel_recording(p_username text, p_site text)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $$
BEGIN
  UPDATE public.channel_assignments
     SET status           = 'recording',
         last_recorded_at = now(),
         last_heartbeat   = now(),
         updated_at       = now()
   WHERE username = p_username
     AND site     = p_site;
END;
$$;
GRANT EXECUTE ON FUNCTION public.mark_channel_recording(text, text) TO anon;
GRANT EXECUTE ON FUNCTION public.mark_channel_recording(text, text) TO authenticated;

-- ── 4) Reset a channel's status ─────────────────────────────────────────────
-- General-purpose single-column status setter (e.g. recording -> claimed once
-- the stream is confirmed offline).
CREATE OR REPLACE FUNCTION public.reset_channel_status(p_username text, p_site text, p_status text)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $$
BEGIN
  UPDATE public.channel_assignments
     SET status     = p_status,
         updated_at = now()
   WHERE username = p_username
     AND site     = p_site;
END;
$$;
GRANT EXECUTE ON FUNCTION public.reset_channel_status(text, text, text) TO anon;
GRANT EXECUTE ON FUNCTION public.reset_channel_status(text, text, text) TO authenticated;

-- ── 5) Reclaim every channel owned by a dead node ───────────────────────────
-- Refuses to touch status='recording' defensively (a dead node cannot be
-- recording, but never interrupt a row mid-write). Returns a TABLE so PostgREST
-- emits predictable [{"reclaimed": N}] JSON.
CREATE OR REPLACE FUNCTION public.reclaim_node_channels(p_node_id text)
RETURNS TABLE(reclaimed int)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $$
DECLARE
  n int;
BEGIN
  UPDATE public.channel_assignments
     SET assigned_node = NULL,
         status        = 'unassigned',
         updated_at    = now()
   WHERE assigned_node = p_node_id
     AND status       <> 'recording';
  GET DIAGNOSTICS n = ROW_COUNT;
  RETURN QUERY SELECT n;
END;
$$;
GRANT EXECUTE ON FUNCTION public.reclaim_node_channels(text) TO anon;
GRANT EXECUTE ON FUNCTION public.reclaim_node_channels(text) TO authenticated;

NOTIFY pgrst, 'reload schema';
