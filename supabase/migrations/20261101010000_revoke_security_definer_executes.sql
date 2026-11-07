-- Migration: 20261101010000_revoke_security_definer_executes.sql
--
-- Addresses Supabase Database Linter findings:
--   - 0028 anon_security_definer_function_executable (Public Can Execute SECURITY DEFINER Function)
--   - 0029 authenticated_security_definer_function_executable (Signed-In Users Can Execute SECURITY DEFINER Function)
--
-- Revokes EXECUTE privileges from PUBLIC, anon, and authenticated roles on all 20 SECURITY DEFINER
-- functions in public schema that are internal fleet coordination, server-side admin, database triggers,
-- or auth hooks.
-- Also sets DEFAULT PRIVILEGES in public schema so future functions do not automatically grant EXECUTE to PUBLIC/anon/authenticated.
--
-- Idempotent: safe to re-run.

-- 1. Default Privileges: functions created in public are private to service_role by default
ALTER DEFAULT PRIVILEGES IN SCHEMA public REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC, anon, authenticated;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT EXECUTE ON FUNCTIONS TO service_role;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC, anon, authenticated;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public GRANT EXECUTE ON FUNCTIONS TO service_role;

-- 2. Fleet & Node coordination functions (service_role only)
DO $$
DECLARE
  fn text;
  fleet_fns text[] := ARRAY[
    'public.claim_channels(text,integer)',
    'public.claim_controller_lease(text,integer)',
    'public.claim_live_channels(text,integer)',
    'public.claim_offline_channels(text,integer)',
    'public.claim_specific_channel(text,text,text)',
    'public.fair_share_target(text)',
    'public.mark_channel_recording(text,text)',
    'public.reassign_channel(text,text,text,text)',
    'public.reclaim_node_channels(text)',
    'public.release_controller_lease(text)',
    'public.reset_channel_status(text,text,text)',
    'public.save_recording_with_links(jsonb,jsonb,jsonb)',
    'public.touch_channel_recordings(text,jsonb)',
    'public.recordings_without_upload_links()'
  ];
BEGIN
  FOREACH fn IN ARRAY fleet_fns LOOP
    IF to_regprocedure(fn) IS NOT NULL THEN
      EXECUTE format('REVOKE ALL ON FUNCTION %s FROM PUBLIC, anon, authenticated', fn);
      EXECUTE format('GRANT EXECUTE ON FUNCTION %s TO service_role', fn);
    END IF;
  END LOOP;
END $$;

-- 3. Backend admin, uploader & server-authoritative functions (service_role only)
DO $$
DECLARE
  fn text;
  backend_fns text[] := ARRAY[
    'public.get_admin_stats()',
    'public.increment_viewer_count(uuid)',
    'public.resolve_username(text)'
  ];
BEGIN
  FOREACH fn IN ARRAY backend_fns LOOP
    IF to_regprocedure(fn) IS NOT NULL THEN
      EXECUTE format('REVOKE ALL ON FUNCTION %s FROM PUBLIC, anon, authenticated', fn);
      EXECUTE format('GRANT EXECUTE ON FUNCTION %s TO service_role', fn);
    END IF;
  END LOOP;
END $$;

-- 4. Trigger functions (never callable via RPC)
DO $$
DECLARE
  fn text;
  trigger_fns text[] := ARRAY[
    'public.user_collection_items_set_position()',
    'public.notify_requesters_on_upload()'
  ];
BEGIN
  FOREACH fn IN ARRAY trigger_fns LOOP
    IF to_regprocedure(fn) IS NOT NULL THEN
      EXECUTE format('REVOKE ALL ON FUNCTION %s FROM PUBLIC, anon, authenticated', fn);
    END IF;
  END LOOP;
END $$;

-- 5. Auth hook (GoTrue only: supabase_auth_admin + service_role)
DO $$
BEGIN
  IF to_regprocedure('public.custom_access_token_hook(jsonb)') IS NOT NULL THEN
    REVOKE ALL ON FUNCTION public.custom_access_token_hook(jsonb) FROM PUBLIC, anon, authenticated;
    GRANT EXECUTE ON FUNCTION public.custom_access_token_hook(jsonb) TO supabase_auth_admin, service_role;
  END IF;
END $$;

-- 6. Explicitly allow client-intended public API functions
DO $$
BEGIN
  IF to_regprocedure('public.reorder_collection_items(text,text[])') IS NOT NULL THEN
    GRANT EXECUTE ON FUNCTION public.reorder_collection_items(text, text[]) TO authenticated, service_role;
  END IF;
  IF to_regprocedure('public.get_site_stats()') IS NOT NULL THEN
    GRANT EXECUTE ON FUNCTION public.get_site_stats() TO anon, authenticated, service_role;
  END IF;
  IF to_regprocedure('public.get_tag_counts()') IS NOT NULL THEN
    GRANT EXECUTE ON FUNCTION public.get_tag_counts() TO anon, authenticated, service_role;
  END IF;
  IF to_regprocedure('public.get_performer_stats()') IS NOT NULL THEN
    GRANT EXECUTE ON FUNCTION public.get_performer_stats() TO anon, authenticated, service_role;
  END IF;
  IF to_regprocedure('public.get_reaction_counts(text)') IS NOT NULL THEN
    GRANT EXECUTE ON FUNCTION public.get_reaction_counts(text) TO anon, authenticated, service_role;
  END IF;
  IF to_regprocedure('public.get_comment_feed(text,text,integer,integer,text)') IS NOT NULL THEN
    GRANT EXECUTE ON FUNCTION public.get_comment_feed(text, text, integer, integer, text) TO anon, authenticated, service_role;
  END IF;
  IF to_regprocedure('public.get_comment_like_stats(integer,text)') IS NOT NULL THEN
    GRANT EXECUTE ON FUNCTION public.get_comment_like_stats(integer, text) TO anon, authenticated, service_role;
  END IF;
  IF to_regprocedure('public.get_comment_like_stats(integer[],text)') IS NOT NULL THEN
    GRANT EXECUTE ON FUNCTION public.get_comment_like_stats(integer[], text) TO anon, authenticated, service_role;
  END IF;
  IF to_regprocedure('public.get_user_history_stats(text)') IS NOT NULL THEN
    GRANT EXECUTE ON FUNCTION public.get_user_history_stats(text) TO authenticated, service_role;
  END IF;
END $$;

NOTIFY pgrst, 'reload schema';
