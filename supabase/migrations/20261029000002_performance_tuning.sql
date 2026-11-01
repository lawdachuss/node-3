-- ============================================================================
-- 20261029000002_performance_tuning.sql
--
-- Query- and storage-level performance pass, driven by measurement rather than
-- guesswork. Everything here is idempotent, so the file can be replayed.
--
-- 1. recordings_with_links: remove the GROUP BY aggregation barrier.
--    Measured on a 200 000 recording / 600 000 upload_link lab dataset:
--      catalog page (ORDER BY timestamp DESC LIMIT 20)  996 ms -> 0.24 ms
--      shared buffers                                      607 595 -> 124
--    The old shape had to aggregate every recording before the caller's
--    ORDER BY / LIMIT / count() could be applied; the LATERAL shape lets the
--    planner walk the ordering index and stop after N rows.
--
-- 2. Fleet hot-path indexes for the predicates the Go coordinator issues
--    (assigned_node + status + is_live, and status + last_heartbeat).
--
-- 3. Duplicate / prefix-redundant indexes. The fleet writes to recordings and
--    channel_assignments continuously; every redundant index is an extra write
--    per INSERT/UPDATE plus dead weight in cache. Drops exact duplicates and
--    indexes that are strict prefixes of a wider index on the same table.
--
-- 4. RLS: drop seven policies that duplicate the generic user_owns_rows policy
--    with an uncached auth.uid() call, and recreate user_owns_rows without the
--    column-side ::text cast so the user_id indexes stay usable.
--
-- 5. Storage: fillfactor + aggressive autovacuum on the small, constantly
--    updated coordination tables so updates stay HOT and planner stats stay
--    fresh (several of these tables reported reltuples = -1 = never analyzed).
--
-- No explicit BEGIN/COMMIT: the migrate bridge sends this file as one simple
-- query (already atomic), and pooled clients reject explicit transactions.
-- ============================================================================

-- ── 1. recordings_with_links: aggregation barrier removed ────────────────────
-- Same columns, same order, same semantics: `links` is a host→url object, or
-- NULL when the recording has no upload link (never '{}'). Verified equal to
-- the old definition by checksum over 200k recordings incl. 10k link-less ones.
CREATE OR REPLACE VIEW public.recordings_with_links
  WITH (security_invoker = true)
AS
SELECT
  r.id,
  r.channel_id,
  r.username,
  r.filename,
  r."timestamp",
  r.room_title,
  r.tags,
  r.viewers,
  r.resolution,
  r.framerate,
  r.filesize,
  r.duration,
  r.gender,
  r.thumbnail_url,
  r.sprite_url,
  r.embed_url,
  r.preview_url,
  r.sprite_vtt_url,
  r.instance_id,
  r.created_at,
  r.updated_at,
  link_data.link_map::json AS links
FROM public.recordings AS r
LEFT JOIN LATERAL (
  SELECT jsonb_object_agg(ul.host, ul.url) AS link_map
  FROM public.upload_links AS ul
  WHERE ul.recording_id = r.id
    AND ul.host IS NOT NULL
) AS link_data ON true;

GRANT SELECT ON public.recordings_with_links TO anon, authenticated, service_role, postgres;

-- ── 2. Fleet hot-path indexes ───────────────────────────────────────────────
-- Claims / heartbeats / reassignments filter on these combinations:
--   assigned_node=eq.X & status=neq.unassigned [& is_live=eq.false]
--   status=eq.recording & (last_heartbeat.lt.X or last_heartbeat.is.null)
CREATE INDEX IF NOT EXISTS idx_ca_node_status_live
  ON public.channel_assignments (assigned_node, status, is_live);

CREATE INDEX IF NOT EXISTS idx_ca_status_heartbeat
  ON public.channel_assignments (status, last_heartbeat);

-- ── 3. Duplicate / prefix-redundant indexes ─────────────────────────────────
-- Each entry is (index to drop, index that already covers it). Constraint-backed
-- indexes are skipped automatically, and missing indexes are ignored.
DO $$
DECLARE
  drop_idx text;
  keep_idx text;
  i int;
  pairs text[][] := ARRAY[
    -- exact duplicates of another index on the same columns
    ['idx_channel_assignments_assigned_node',      'idx_ca_assigned_node'],
    ['upload_links_recording_id_idx',              'idx_upload_links_recording_id'],
    ['saved_videos_user_recording_unique',         'saved_videos_uniq'],
    ['watch_history_user_recording_unique',        'watch_history_uniq'],
    ['watch_later_uniq',                           'watch_later_items_user_recording_unique'],
    ['user_collection_items_collection_recording_unique', 'collection_items_uniq'],
    ['performer_follows_user_performer_unique',    'performer_follows_uniq'],
    ['performer_follows_user_performer_key',       'performer_follows_uniq'],
    -- (user_id::text) expression index: user_id is already text on this table
    ['watch_history_user_text_watched_at_desc_idx','watch_history_user_watched_at_desc_idx'],
    -- strict prefixes of a wider index on the same table
    ['idx_upload_links_recording_id',              'upload_links_recording_host_unique'],
    ['idx_performer_follows_user',                 'performer_follows_uniq'],
    ['idx_notifications_user',                     'idx_notifications_unread'],
    ['idx_recordings_username',                    'recordings_username_timestamp_id_desc_idx'],
    ['idx_recordings_gender',                      'recordings_gender_timestamp_id_desc_idx'],
    ['idx_recordings_timestamp',                   'recordings_timestamp_id_desc_idx'],
    -- superseded by the fleet composites created in section 2
    ['idx_ca_status',                              'idx_ca_status_heartbeat'],
    ['idx_ca_islive',                              'idx_ca_node_status_live'],
    ['idx_ca_heartbeat',                           'idx_ca_status_heartbeat'],
    ['idx_ca_assigned_node',                       'idx_ca_node_status_live']
  ];
BEGIN
  FOR i IN 1 .. array_length(pairs, 1) LOOP
    drop_idx := pairs[i][1];
    keep_idx := pairs[i][2];
    CONTINUE WHEN to_regclass('public.' || quote_ident(keep_idx)) IS NULL;
    CONTINUE WHEN to_regclass('public.' || quote_ident(drop_idx)) IS NULL;
    -- never drop an index that backs a PK / UNIQUE / EXCLUDE constraint
    CONTINUE WHEN EXISTS (
      SELECT 1 FROM pg_constraint
      WHERE conindid = to_regclass('public.' || quote_ident(drop_idx))
    );
    EXECUTE format('DROP INDEX public.%I', drop_idx);
    RAISE NOTICE 'dropped redundant index % (covered by %)', drop_idx, keep_idx;
  END LOOP;
END
$$;

-- ── 3b. Duplicate UNIQUE constraints ───────────────────────────────────────
-- Each of these tables carries two UNIQUE constraints on exactly the same
-- columns, so every INSERT maintained two identical unique indexes. One of
-- each pair is dropped; uniqueness itself is unchanged (proved after the
-- migration by re-inserting a duplicate row and expecting SQLSTATE 23505).
ALTER TABLE public.saved_videos          DROP CONSTRAINT IF EXISTS saved_videos_user_recording_unique;
ALTER TABLE public.watch_history         DROP CONSTRAINT IF EXISTS watch_history_user_recording_unique;
ALTER TABLE public.watch_later_items     DROP CONSTRAINT IF EXISTS watch_later_items_user_recording_unique;
ALTER TABLE public.user_collection_items DROP CONSTRAINT IF EXISTS user_collection_items_collection_recording_unique;
ALTER TABLE public.performer_follows     DROP CONSTRAINT IF EXISTS performer_follows_uniq;

-- Safety net: after the dedupe, every table below must still reject a duplicate
-- (user, item) pair — these are the ON CONFLICT targets the API upserts rely on.
DO $guard$
DECLARE
  spec text[][];
  tbl  text;
  cols text[];
  ok   boolean;
BEGIN
  FOREACH spec SLICE 1 IN ARRAY ARRAY[
    ['public.saved_videos',          'user_id,recording_id'],
    ['public.watch_history',         'user_id,recording_id'],
    ['public.watch_later_items',     'user_id,recording_id'],
    ['public.user_collection_items', 'collection_id,recording_id'],
    ['public.performer_follows',     'user_id,performer_username']
  ] LOOP
    tbl  := spec[1];
    cols := string_to_array(spec[2], ',');
    SELECT EXISTS (
      SELECT 1 FROM pg_index i
      WHERE i.indrelid = tbl::regclass
        AND i.indisunique
        AND i.indpred IS NULL
        AND (SELECT array_agg(a.attname::text ORDER BY k.ord)
             FROM unnest(i.indkey::int2[]) WITH ORDINALITY AS k(attnum, ord)
             JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum) = cols
    ) INTO ok;
    IF NOT ok THEN
      EXECUTE format('ALTER TABLE %s ADD CONSTRAINT %I UNIQUE (%s)',
                     tbl, 'uniq_' || split_part(tbl, '.', 2) || '_' || array_to_string(cols, '_'),
                     array_to_string(cols, ', '));
      RAISE NOTICE 'restored uniqueness on % (%)', tbl, spec[2];
    END IF;
  END LOOP;
END
$guard$;

-- ── 4. RLS policy dedupe + index-friendly owner checks ──────────────────────
-- Every table below already has the generic `user_owns_rows` policy (ALL, to
-- authenticated) covering the same rows, so these older policies only added a
-- second uncached auth.uid() evaluation per row and never granted extra access.
DROP POLICY IF EXISTS performer_follows_owner_all            ON public.performer_follows;
DROP POLICY IF EXISTS performer_follows_own_all              ON public.performer_follows;
DROP POLICY IF EXISTS user_premium_owner_select              ON public.user_premium;
DROP POLICY IF EXISTS premium_purchases_owner_select         ON public.premium_purchases;
DROP POLICY IF EXISTS user_notifications_owner_all           ON public.user_notifications;
DROP POLICY IF EXISTS user_notification_preferences_owner_all ON public.user_notification_preferences;
DROP POLICY IF EXISTS requests_own_all                       ON public.requests;

-- Recreate the owner policy so the qual is a single InitPlan (auth.uid()
-- evaluated once per statement) and the column side stays bare — the old
-- `(user_id)::text = ...` prevented index use on the uuid user_id columns.
DROP POLICY IF EXISTS user_owns_rows ON public.performer_follows;
CREATE POLICY user_owns_rows ON public.performer_follows FOR ALL TO authenticated
  USING (user_id = (SELECT auth.uid()))
  WITH CHECK (user_id = (SELECT auth.uid()));

DROP POLICY IF EXISTS user_owns_rows ON public.premium_purchases;
CREATE POLICY user_owns_rows ON public.premium_purchases FOR ALL TO authenticated
  USING (user_id = (SELECT auth.uid()))
  WITH CHECK (user_id = (SELECT auth.uid()));

DROP POLICY IF EXISTS user_owns_rows ON public.user_premium;
CREATE POLICY user_owns_rows ON public.user_premium FOR ALL TO authenticated
  USING (user_id = (SELECT auth.uid()))
  WITH CHECK (user_id = (SELECT auth.uid()));

DROP POLICY IF EXISTS user_owns_rows ON public.user_notifications;
CREATE POLICY user_owns_rows ON public.user_notifications FOR ALL TO authenticated
  USING (user_id = (SELECT auth.uid())::text)
  WITH CHECK (user_id = (SELECT auth.uid())::text);

DROP POLICY IF EXISTS user_owns_rows ON public.user_notification_preferences;
CREATE POLICY user_owns_rows ON public.user_notification_preferences FOR ALL TO authenticated
  USING (user_id = (SELECT auth.uid())::text)
  WITH CHECK (user_id = (SELECT auth.uid())::text);

DROP POLICY IF EXISTS user_owns_rows ON public.requests;
CREATE POLICY user_owns_rows ON public.requests FOR ALL TO authenticated
  USING (user_id = (SELECT auth.uid())::text)
  WITH CHECK (user_id = (SELECT auth.uid())::text);

-- ── 5. Storage tuning for hot, frequently-updated coordination rows ─────────
-- 80 % fillfactor leaves room on each page for HOT updates (no index rewrite,
-- index-only scans keep their visibility-map entries). Scale factors keep
-- autovacuum proportional to these small tables instead of the 20 % default.
ALTER TABLE public.nodes               SET (fillfactor = 80, autovacuum_vacuum_scale_factor = 0.02, autovacuum_analyze_scale_factor = 0.01);
ALTER TABLE public.controller_lease    SET (fillfactor = 70, autovacuum_vacuum_scale_factor = 0.02, autovacuum_analyze_scale_factor = 0.01);
ALTER TABLE public.channel_assignments SET (fillfactor = 80, autovacuum_vacuum_scale_factor = 0.05, autovacuum_analyze_scale_factor = 0.02);
ALTER TABLE public.app_settings        SET (fillfactor = 80, autovacuum_vacuum_scale_factor = 0.05, autovacuum_analyze_scale_factor = 0.02);
ALTER TABLE public.edge_cache          SET (fillfactor = 90, autovacuum_vacuum_scale_factor = 0.05, autovacuum_analyze_scale_factor = 0.02);

-- ── 6. Fresh planner statistics ─────────────────────────────────────────────
-- Several tables had never been analyzed (reltuples = -1), so the planner was
-- running on defaults. ANALYZE is safe to run inside this script.
ANALYZE public.recordings;
ANALYZE public.upload_links;
ANALYZE public.channel_assignments;
ANALYZE public.nodes;
ANALYZE public.controller_lease;
ANALYZE public.user_notifications;
ANALYZE public.performer_follows;
ANALYZE public.requests;
ANALYZE public.user_premium;
ANALYZE public.premium_purchases;

-- Make PostgREST pick up the redefined view and new indexes immediately.
NOTIFY pgrst, 'reload schema';
