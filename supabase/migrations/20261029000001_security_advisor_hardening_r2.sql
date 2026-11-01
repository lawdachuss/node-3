-- Security Advisor hardening, round 2 (2026-10-29)
--
-- Clears the findings left after 20261029000000_security_advisor_hardening.sql:
--
--   0029 authenticated_security_definer_function_executable
--        public.reorder_collection_items
--        -> SECURITY INVOKER: the function already checks collection ownership
--           itself, and the RLS policies added in the previous migration now
--           permit exactly the rows it updates, so DEFINER is no longer needed.
--
--   0011 function_search_path_mutable  (4x, schema `_supabase`)
--        register_access_token / check_token_jti / revoke_token_by_id /
--        list_access_tokens
--        -> these are the self-hosted stack's own functions (created by
--           supabase/volumes/db/access-tokens.sql), so their search_path is
--           pinned here as well as in that bootstrap file. Bodies are left
--           untouched: they already qualify `_supabase.access_tokens`, and the
--           explicit list keeps pgcrypto's gen_random_bytes() (extensions)
--           resolvable. Guarded, because a hosted Supabase project has no
--           `_supabase` schema.
--
--   0008 rls_enabled_no_policy  (INFO)  _schema_migrations / activity_events /
--        controller_lease / edge_cache
--        -> intentionally service-role-only; each gets an explicit
--           service_role policy so the intent is declared in the database
--           instead of relying on "no policy means nobody". service_role
--           bypasses RLS either way, so behaviour is unchanged.
--
-- Idempotent: safe to re-run.

-- ============================================================================
-- 1) reorder_collection_items: SECURITY DEFINER -> SECURITY INVOKER
--    Behaviour is identical for the owner (it re-checks auth.uid() itself);
--    a caller who is not the collection owner is still rejected, and RLS now
--    also blocks any row it does not own.
-- ============================================================================

CREATE OR REPLACE FUNCTION public.reorder_collection_items(p_collection_id text, p_recording_ids text[])
RETURNS integer
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path = ''
AS $function$
declare
  v_user text;
  v_caller text;
  v_updated integer := 0;
  rec record;
begin
  v_caller := (auth.uid())::text;
  if v_caller is null then
    raise exception 'not authenticated';
  end if;

  select user_id into v_user
  from public.user_collections
  where id = p_collection_id;
  if v_user is null then
    raise exception 'collection not found';
  end if;
  if v_user <> v_caller then
    raise exception 'not permitted';
  end if;

  if p_recording_ids is null or cardinality(p_recording_ids) = 0 then
    raise exception 'recording_ids required';
  end if;

  -- Duplicate ids would make the renumber ambiguous — reject loudly.
  if exists (
    select 1 from unnest(p_recording_ids) x
    group by x
    having count(*) > 1
  ) then
    raise exception 'duplicate recording ids in payload';
  end if;

  -- Renumber the payload rows by their array index (0-based).
  for rec in
    select i, x from unnest(p_recording_ids) with ordinality as t(x, i)
  loop
    update public.user_collection_items
       set position = rec.i - 1
     where collection_id = p_collection_id
       and recording_id = rec.x;
    v_updated := v_updated + 1;
  end loop;

  -- Keep metadata fresh for the collections list view.
  update public.user_collections
     set updated_at = now()
   where id = p_collection_id;

  return v_updated;
end;
$function$;

-- The API server calls this with the user's JWT (role authenticated).
REVOKE ALL ON FUNCTION public.reorder_collection_items(text, text[]) FROM PUBLIC, anon;
GRANT EXECUTE ON FUNCTION public.reorder_collection_items(text, text[]) TO authenticated, service_role;

-- ============================================================================
-- 2) Pin search_path on the stack's own `_supabase` token functions.
-- ============================================================================

DO $do$
BEGIN
  IF to_regnamespace('_supabase') IS NULL THEN
    RAISE NOTICE 'schema _supabase absent (hosted project) — skipping';
    RETURN;
  END IF;

  IF to_regprocedure('_supabase.register_access_token(text,text)') IS NOT NULL THEN
    EXECUTE 'ALTER FUNCTION _supabase.register_access_token(text, text) SET search_path = _supabase, extensions';
  END IF;
  IF to_regprocedure('_supabase.check_token_jti(uuid)') IS NOT NULL THEN
    EXECUTE 'ALTER FUNCTION _supabase.check_token_jti(uuid) SET search_path = _supabase, extensions';
  END IF;
  IF to_regprocedure('_supabase.revoke_token_by_id(uuid)') IS NOT NULL THEN
    EXECUTE 'ALTER FUNCTION _supabase.revoke_token_by_id(uuid) SET search_path = _supabase, extensions';
  END IF;
  IF to_regprocedure('_supabase.list_access_tokens(boolean)') IS NOT NULL THEN
    EXECUTE 'ALTER FUNCTION _supabase.list_access_tokens(boolean) SET search_path = _supabase, extensions';
  END IF;
END
$do$;

-- ============================================================================
-- 3) Declare the service-role-only tables explicitly.
--    These four are reached exclusively through functions that run as the
--    owner/service role; no anon or authenticated policy is wanted.
-- ============================================================================

DROP POLICY IF EXISTS "service_role_only" ON public._schema_migrations;
CREATE POLICY "service_role_only" ON public._schema_migrations
  FOR ALL TO service_role USING (true) WITH CHECK (true);

DROP POLICY IF EXISTS "service_role_only" ON public.activity_events;
CREATE POLICY "service_role_only" ON public.activity_events
  FOR ALL TO service_role USING (true) WITH CHECK (true);

DROP POLICY IF EXISTS "service_role_only" ON public.controller_lease;
CREATE POLICY "service_role_only" ON public.controller_lease
  FOR ALL TO service_role USING (true) WITH CHECK (true);

DROP POLICY IF EXISTS "service_role_only" ON public.edge_cache;
CREATE POLICY "service_role_only" ON public.edge_cache
  FOR ALL TO service_role USING (true) WITH CHECK (true);

NOTIFY pgrst, 'reload schema';
