-- Security Advisor hardening (2026-10-29)
--
-- Addresses the Supabase database-linter findings against this schema:
--   0011 function_search_path_mutable   (6 app functions)
--   0028/0029 anon|authenticated can execute SECURITY DEFINER function
--   0008 rls_enabled_no_policy          (user_collection_items — real bug)
--   0014 extension_in_public            (pg_trgm)
--
-- Deliberately NOT touched: the 5 `_supabase.*` findings (Supabase's own
-- access-token schema, owned by the platform), and the INFO rows for
-- _schema_migrations / activity_events / controller_lease / edge_cache, which
-- are intentionally service-role-only and reached solely through functions.
--
-- Grant model used here (verified against the code before changing anything):
--   * The browser bundle never calls a PostgREST RPC — `grep -r '.rpc('
--     artifacts/video-archive` is empty. All RPCs are called by the API server,
--     whose client prefers SUPABASE_SERVICE_ROLE_KEY (lib/supabase.ts).
--   * The single exception is `reorder_collection_items`, called through
--     req.supabase (role `authenticated`) in routes/user.ts after an ownership
--     check, so it keeps its EXECUTE grant for authenticated.
--   * The Go fleet writes with the service_role key (server/db.go supabaseKey()).
--   Therefore every fleet/aggregate RPC can be closed to anon/authenticated,
--   which removes the ability to claim channels, reassign live captures or
--   write recordings with the public anon key that ships in the browser.
--
-- Idempotent: safe to re-run.

-- ============================================================================
-- 1) user_collection_items had RLS enabled and NO policy at all.
--    The API's user routes act as the `authenticated` role, so adding,
--    removing and reordering playlist items failed for every signed-in user.
--    Access is now scoped to the owner of the parent collection.
-- ============================================================================

ALTER TABLE public.user_collection_items ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "user_collection_items_own_select" ON public.user_collection_items;
CREATE POLICY "user_collection_items_own_select" ON public.user_collection_items
  FOR SELECT TO authenticated
  USING (EXISTS (
    SELECT 1 FROM public.user_collections c
     WHERE c.id = user_collection_items.collection_id
       AND c.user_id = (SELECT auth.uid())::text
  ));

DROP POLICY IF EXISTS "user_collection_items_own_insert" ON public.user_collection_items;
CREATE POLICY "user_collection_items_own_insert" ON public.user_collection_items
  FOR INSERT TO authenticated
  WITH CHECK (EXISTS (
    SELECT 1 FROM public.user_collections c
     WHERE c.id = user_collection_items.collection_id
       AND c.user_id = (SELECT auth.uid())::text
  ));

DROP POLICY IF EXISTS "user_collection_items_own_update" ON public.user_collection_items;
CREATE POLICY "user_collection_items_own_update" ON public.user_collection_items
  FOR UPDATE TO authenticated
  USING (EXISTS (
    SELECT 1 FROM public.user_collections c
     WHERE c.id = user_collection_items.collection_id
       AND c.user_id = (SELECT auth.uid())::text
  ))
  WITH CHECK (EXISTS (
    SELECT 1 FROM public.user_collections c
     WHERE c.id = user_collection_items.collection_id
       AND c.user_id = (SELECT auth.uid())::text
  ));

DROP POLICY IF EXISTS "user_collection_items_own_delete" ON public.user_collection_items;
CREATE POLICY "user_collection_items_own_delete" ON public.user_collection_items
  FOR DELETE TO authenticated
  USING (EXISTS (
    SELECT 1 FROM public.user_collections c
     WHERE c.id = user_collection_items.collection_id
       AND c.user_id = (SELECT auth.uid())::text
  ));

DROP POLICY IF EXISTS "user_collection_items_service_all" ON public.user_collection_items;
CREATE POLICY "user_collection_items_service_all" ON public.user_collection_items
  FOR ALL TO service_role USING (true) WITH CHECK (true);

-- ============================================================================
-- 2) Close the fleet/aggregate RPCs to anon + authenticated.
--    CREATE FUNCTION grants EXECUTE to PUBLIC by default, which is why the
--    anon key (public in the browser bundle) could call these.
--    `reorder_collection_items` is intentionally left granted to authenticated.
-- ============================================================================

DO $$
DECLARE
  fn text;
  fleet_fns text[] := ARRAY[
    'public.claim_channels(text,integer)',
    'public.claim_live_channels(text,integer)',
    'public.claim_offline_channels(text,integer)',
    'public.claim_specific_channel(text,text,text)',
    'public.claim_controller_lease(text,integer)',
    'public.release_controller_lease(text)',
    'public.fair_share_target(text)',
    'public.mark_channel_recording(text,text)',
    'public.reset_channel_status(text,text,text)',
    'public.reassign_channel(text,text,text,text)',
    'public.reclaim_node_channels(text)',
    'public.save_recording_with_links(jsonb,jsonb,jsonb)',
    'public.touch_channel_recordings(text,jsonb)',
    'public.recordings_without_upload_links()',
    -- aggregate/read RPCs the API server calls with the service key
    'public.get_site_stats()',
    'public.get_tag_counts()',
    'public.get_performer_stats()',
    'public.get_reaction_counts(text)',
    'public.get_comment_feed(text,text,integer,integer,text)',
    'public.get_comment_like_stats(integer[],text)',
    'public.get_user_history_stats(text)',
    'public.get_admin_stats()',
    'public.increment_viewer_count(uuid)',
    'public.claim_premium_reward(uuid,integer,integer)',
    -- trigger functions: a trigger never needs EXECUTE for a role
    'public.notify_requesters_on_upload()',
    'public.protect_recording_assignment()',
    'public.protect_live_owner_assignment()',
    'public.enforce_fair_share_claim()',
    'public.user_collection_items_set_position()',
    'public.update_user_channels_updated_at()',
    'public.update_updated_at_column()'
  ];
BEGIN
  FOREACH fn IN ARRAY fleet_fns LOOP
    -- to_regprocedure() keeps this safe when an optional function is absent.
    IF to_regprocedure(fn) IS NOT NULL THEN
      EXECUTE format('REVOKE ALL ON FUNCTION %s FROM PUBLIC, anon, authenticated', fn);
      EXECUTE format('GRANT EXECUTE ON FUNCTION %s TO service_role', fn);
    END IF;
  END LOOP;
END $$;

-- The one RPC the API calls with the user's JWT keeps working for signed-in
-- users; anon must not be able to reorder someone else's collection.
DO $$
BEGIN
  IF to_regprocedure('public.reorder_collection_items(text,text[])') IS NOT NULL THEN
    REVOKE ALL ON FUNCTION public.reorder_collection_items(text, text[]) FROM PUBLIC, anon;
    GRANT EXECUTE ON FUNCTION public.reorder_collection_items(text, text[]) TO authenticated, service_role;
  END IF;
END $$;

-- ============================================================================
-- 3) Pin search_path on the functions the linter flagged (0011).
--    With an empty search_path every relation/type reference must be
--    schema-qualified; pg_catalog (now(), jsonb_*, GREATEST, …) stays implicit.
-- ============================================================================

CREATE OR REPLACE FUNCTION public.update_user_channels_updated_at()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = ''
AS $fn$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$fn$;

CREATE OR REPLACE FUNCTION public.protect_recording_assignment()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = ''
AS $fn$
BEGIN
    IF NEW.status = 'recording' AND OLD.assigned_node IS DISTINCT FROM NEW.assigned_node THEN
        NEW.assigned_node := OLD.assigned_node;
    END IF;
    RETURN NEW;
END;
$fn$;

CREATE OR REPLACE FUNCTION public.protect_live_owner_assignment()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = ''
AS $fn$
DECLARE
    owner_alive boolean;
BEGIN
    -- Claims from 'unassigned' have no owner to protect; releases to NULL
    -- remove the channel from every node (pause/removal) and can never create
    -- a duplicate capture.
    IF OLD.assigned_node IS NULL OR NEW.assigned_node IS NULL THEN
        RETURN NEW;
    END IF;
    -- Nothing relevant is changing.
    IF OLD.assigned_node IS NOT DISTINCT FROM NEW.assigned_node THEN
        RETURN NEW;
    END IF;

    SELECT (status IN ('online', 'draining') AND last_heartbeat > now() - interval '20 minutes')
      INTO owner_alive
      FROM public.nodes
     WHERE node_id = OLD.assigned_node;

    IF owner_alive IS NOT TRUE THEN
        RETURN NEW; -- owner is gone/offline — its rows may be cleaned & redistributed
    END IF;

    -- Owner is alive. The row is pinned while it could still be captured: it
    -- is still marked 'recording' (refreshed every ~30s by the owner while it
    -- captures; a stalled sync ages the heartbeat but never this status). A
    -- claimed row on a live owner — finished recording, or lease-reset before
    -- a re-pin — stays movable so the fleet can rebalance.
    IF OLD.status = 'recording' THEN
        NEW.assigned_node := OLD.assigned_node;
    END IF;

    RETURN NEW;
END;
$fn$;

CREATE OR REPLACE FUNCTION public.reassign_channel(p_username text, p_site text, p_from_node text, p_to_node text)
RETURNS SETOF boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $fn$
BEGIN
  RETURN QUERY
  UPDATE public.channel_assignments
     SET assigned_node = p_to_node,
         status        = 'claimed',
         assigned_at   = now(),
         updated_at    = now()
   WHERE username      = p_username
     AND site          = p_site
     AND assigned_node = p_from_node
     AND status        <> 'recording'
  RETURNING true;
END;
$fn$;

CREATE OR REPLACE FUNCTION public.claim_premium_reward(p_user_id uuid, p_cooldown_s integer, p_target integer)
RETURNS jsonb
LANGUAGE plpgsql
SET search_path = ''
AS $fn$
DECLARE
  v_row         public.user_premium;
  v_now         timestamptz := now();
  v_today       date        := current_date;
  v_granted     boolean     := false;
  v_cooldown_ms integer     := 0;
BEGIN
  -- Ensure a row exists so SELECT ... FOR UPDATE has something to lock.
  INSERT INTO public.user_premium (user_id, created_at, updated_at)
  VALUES (p_user_id, v_now, v_now)
  ON CONFLICT (user_id) DO NOTHING;

  SELECT * INTO v_row
  FROM public.user_premium
  WHERE user_id = p_user_id
  FOR UPDATE;

  -- Cooldown gate (rejects early unless it's already expired).
  IF v_row.last_rewarded_at IS NOT NULL AND p_cooldown_s > 0 THEN
    v_cooldown_ms := GREATEST(
      0,
      p_cooldown_s * 1000 - (EXTRACT(EPOCH FROM (v_now - v_row.last_rewarded_at))::integer) * 1000
    );
    IF v_cooldown_ms > 0 THEN
      RETURN jsonb_build_object(
        'granted',             false,
        'cooldown_ms',         v_cooldown_ms,
        'already_granted_today', false,
        'ads_viewed_today',    v_row.ads_seen_count,
        'ads_target',          p_target,
        'premium_until',       v_row.premium_expires_at,
        'is_premium',          v_row.premium_expires_at > v_now
      );
    END IF;
  END IF;

  -- Roll the daily window when the date changes.
  IF v_row.ads_seen_date IS DISTINCT FROM v_today THEN
    v_row.ads_seen_date      := v_today;
    v_row.ads_seen_count     := 0;
    v_row.last_granted_date  := NULL;
  END IF;

  -- DAILY GRANT CAP: if the user already received a reward today, refuse.
  IF v_row.last_granted_date = v_today THEN
    RETURN jsonb_build_object(
      'granted',             false,
      'cooldown_ms',         0,
      'already_granted_today', true,
      'ads_viewed_today',    v_row.ads_seen_count,
      'ads_target',          p_target,
      'premium_until',       v_row.premium_expires_at,
      'is_premium',          v_row.premium_expires_at > v_now
    );
  END IF;

  v_row.ads_seen_count  := v_row.ads_seen_count + 1;
  v_row.last_rewarded_at := v_now;

  -- Grant: extend from the later of "now" and the existing expiry so stacking
  -- never shortens a current subscription, then reset the daily counter.
  IF p_target > 0 AND v_row.ads_seen_count >= p_target THEN
    v_row.premium_expires_at := GREATEST(v_now, COALESCE(v_row.premium_expires_at, v_now)) + interval '1 day';
    v_row.ads_seen_count     := 0;
    v_row.last_granted_date  := v_today;
    v_granted                := true;
  END IF;

  UPDATE public.user_premium
  SET premium_expires_at = v_row.premium_expires_at,
      ads_seen_date      = v_row.ads_seen_date,
      ads_seen_count     = v_row.ads_seen_count,
      last_rewarded_at   = v_row.last_rewarded_at,
      last_granted_date  = v_row.last_granted_date,
      updated_at         = v_now
  WHERE user_id = p_user_id;

  RETURN jsonb_build_object(
    'granted',             v_granted,
    'cooldown_ms',         v_cooldown_ms,
    'already_granted_today', false,
    'ads_viewed_today',    v_row.ads_seen_count,
    'ads_target',          p_target,
    'premium_until',       v_row.premium_expires_at,
    'is_premium',          v_row.premium_expires_at > v_now
  );
END;
$fn$;

CREATE OR REPLACE FUNCTION public.save_recording_with_links(p_rec jsonb, p_links jsonb, p_preview jsonb)
RETURNS text
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $fn$
DECLARE
  v_id         uuid;
  v_filename   text;
  v_thumb      text;
  v_sprite     text;
  v_preview    text;
  v_embed_url  text;
BEGIN
  -- ── 1. Upsert recording ────────────────────────────────────────────
  v_filename  := p_rec->>'filename';
  v_thumb     := p_rec->>'thumbnail_url';
  v_sprite    := p_rec->>'sprite_url';
  v_preview   := p_rec->>'preview_url';
  v_embed_url := p_rec->>'embed_url';

  INSERT INTO public.recordings (
    username, filename, timestamp, room_title, tags, viewers,
    resolution, framerate, filesize, duration, gender, end_reason,
    embed_url, thumbnail_url, sprite_url, preview_url,
    instance_id
  ) VALUES (
    p_rec->>'username',
    v_filename,
    (p_rec->>'timestamp')::timestamptz,
    p_rec->>'room_title',
    -- jsonb array of strings -> text[].  jsonb_typeof() is NULL for a missing
    -- key, so a non-array (including NULL) lands on '{}' exactly as the old
    -- COALESCE(p_rec->'tags', '[]'::jsonb) intended.
    ARRAY(
      SELECT jsonb_array_elements_text(
        CASE WHEN jsonb_typeof(p_rec->'tags') = 'array'
             THEN p_rec->'tags'
             ELSE '[]'::jsonb END
      )
    ),
    (p_rec->>'viewers')::int,
    p_rec->>'resolution',
    (p_rec->>'framerate')::int,
    (p_rec->>'filesize')::bigint,
    (p_rec->>'duration')::double precision,
    p_rec->>'gender',
    p_rec->>'end_reason',
    v_embed_url,
    v_thumb,
    v_sprite,
    v_preview,
    p_rec->>'instance_id'
  )
  ON CONFLICT (filename) DO UPDATE SET
    username      = EXCLUDED.username,
    timestamp     = EXCLUDED.timestamp,
    room_title    = EXCLUDED.room_title,
    tags          = EXCLUDED.tags,
    viewers       = EXCLUDED.viewers,
    resolution    = EXCLUDED.resolution,
    framerate     = EXCLUDED.framerate,
    filesize      = EXCLUDED.filesize,
    duration      = EXCLUDED.duration,
    gender        = EXCLUDED.gender,
    end_reason    = EXCLUDED.end_reason,
    embed_url     = EXCLUDED.embed_url,
    thumbnail_url = EXCLUDED.thumbnail_url,
    sprite_url    = EXCLUDED.sprite_url,
    preview_url   = EXCLUDED.preview_url,
    instance_id   = EXCLUDED.instance_id,
    updated_at    = now()
  RETURNING id INTO v_id;

  -- ── 2. Upload links (delete-then-insert is atomic per-transaction) ─
  IF p_links IS NOT NULL AND jsonb_array_length(p_links) > 0 THEN
    DELETE FROM public.upload_links WHERE recording_id = v_id;

    INSERT INTO public.upload_links (recording_id, host, url, instance_id)
    SELECT
      v_id,
      l->>'host',
      l->>'url',
      l->>'instance_id'
    FROM jsonb_array_elements(p_links) l;
  END IF;

  -- ── 3. Preview images ──────────────────────────────────────────────
  -- recording_id ties the preview_images row to the recording just upserted
  -- so FK-based joins (preview_images.recording_id → recordings.id) work.
  -- Without it every row stays NULL and consumers cannot link the two tables.
  IF p_preview IS NOT NULL THEN
    INSERT INTO public.preview_images (
      recording_id, filename, thumbnail_url, sprite_url, preview_url,
      thumbnail_mirrors, sprite_mirrors, preview_mirrors,
      uploaded_at, instance_id
    ) VALUES (
      v_id,
      v_filename,
      p_preview->>'thumbnail_url',
      p_preview->>'sprite_url',
      p_preview->>'preview_url',
      COALESCE(p_preview->'thumbnail_mirrors', '{}'::jsonb),
      COALESCE(p_preview->'sprite_mirrors', '{}'::jsonb),
      COALESCE(p_preview->'preview_mirrors', '{}'::jsonb),
      COALESCE(NULLIF(p_preview->>'uploaded_at', '')::timestamptz, now()),
      p_preview->>'instance_id'
    )
    ON CONFLICT (filename) DO UPDATE SET
      recording_id      = EXCLUDED.recording_id,
      thumbnail_url     = EXCLUDED.thumbnail_url,
      sprite_url        = EXCLUDED.sprite_url,
      preview_url       = EXCLUDED.preview_url,
      thumbnail_mirrors = EXCLUDED.thumbnail_mirrors,
      sprite_mirrors    = EXCLUDED.sprite_mirrors,
      preview_mirrors   = EXCLUDED.preview_mirrors,
      uploaded_at       = EXCLUDED.uploaded_at,
      instance_id       = EXCLUDED.instance_id;
  END IF;

  RETURN v_id::text;
END;
$fn$;

-- Re-assert the grant after recreating the fleet RPCs (CREATE OR REPLACE keeps
-- the existing ACL, but this keeps a fresh replay deterministic).
REVOKE ALL ON FUNCTION public.save_recording_with_links(jsonb, jsonb, jsonb) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.save_recording_with_links(jsonb, jsonb, jsonb) TO service_role;
REVOKE ALL ON FUNCTION public.reassign_channel(text, text, text, text) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.reassign_channel(text, text, text, text) TO service_role;
REVOKE ALL ON FUNCTION public.claim_premium_reward(uuid, integer, integer) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.claim_premium_reward(uuid, integer, integer) TO service_role;

-- ============================================================================
-- 4) Move pg_trgm out of the public schema (0014).
--    Relocatable extension: existing GIN indexes keep working (the operator
--    class is bound by OID); new DDL must use extensions.gin_trgm_ops, which
--    all-in-one.sql and migrations/017 already do after this change.
-- ============================================================================

DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM pg_extension e
      JOIN pg_namespace n ON n.oid = e.extnamespace
     WHERE e.extname = 'pg_trgm' AND n.nspname = 'public'
  ) THEN
    IF NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'extensions') THEN
      CREATE SCHEMA extensions;
    END IF;
    ALTER EXTENSION pg_trgm SET SCHEMA extensions;
  END IF;
END $$;

NOTIFY pgrst, 'reload schema';
