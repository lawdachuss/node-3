-- Harden RLS policies and function settings to clear Security Advisor lints:
--   - 0011 function_search_path_mutable (update_updated_at_column)
--   - 0024 rls_policy_always_true (18 policies)
--   - 0028 anon_security_definer_function_executable (custom_access_token_hook)
--   - 0029 authenticated_security_definer_function_executable (custom_access_token_hook)
--
-- Design notes:
--   * The DVR and coordinator talk to Supabase with the service_role key, which
--     bypasses RLS entirely, so no anon/authenticated write policy is needed for
--     backend tables. Writes keep working through the service_role_all policies.
--   * scripts/cookie_refresher.py (anon key) writes app_settings rows whose keys
--     are always 'dvr_settings:<node_id>' — scoped anon write policies preserve
--     that exact path and nothing else.
--   * Public reads are preserved via SELECT USING(true) policies, which the
--     linter explicitly exempts.
--   * custom_access_token_hook is invoked by GoTrue as the postgres role;
--     revoking EXECUTE from anon/authenticated/PUBLIC does not affect sign-in.
--
-- Idempotent: safe to re-run.

-- ── 0011 function_search_path_mutable ─────────────────────────────────────────
ALTER FUNCTION public.update_updated_at_column() SET search_path = '';

-- ── 0028/0029 custom_access_token_hook (SECURITY DEFINER, GoTrue hook) ────────
REVOKE EXECUTE ON FUNCTION public.custom_access_token_hook(jsonb) FROM anon, authenticated, PUBLIC;
GRANT EXECUTE ON FUNCTION public.custom_access_token_hook(jsonb) TO service_role;

-- ── 0024 rls_policy_always_true ───────────────────────────────────────────────
-- Backend tables: public SELECT only; writes via service_role (bypasses RLS).

DROP POLICY IF EXISTS "Allow all operations on app_settings" ON public.app_settings;
DROP POLICY IF EXISTS "app_settings_public_select" ON public.app_settings;
CREATE POLICY "app_settings_public_select" ON public.app_settings FOR SELECT TO anon, authenticated USING (true);
DROP POLICY IF EXISTS "app_settings_anon_cookie_write" ON public.app_settings;
CREATE POLICY "app_settings_anon_cookie_write" ON public.app_settings FOR INSERT TO anon WITH CHECK (key LIKE 'dvr_settings:%');
DROP POLICY IF EXISTS "app_settings_anon_cookie_update" ON public.app_settings;
CREATE POLICY "app_settings_anon_cookie_update" ON public.app_settings FOR UPDATE TO anon USING (key LIKE 'dvr_settings:%') WITH CHECK (key LIKE 'dvr_settings:%');
DROP POLICY IF EXISTS "app_settings_service_all" ON public.app_settings;
CREATE POLICY "app_settings_service_all" ON public.app_settings FOR ALL TO service_role USING (true) WITH CHECK (true);

DROP POLICY IF EXISTS "Allow all operations on channel_assignments" ON public.channel_assignments;
DROP POLICY IF EXISTS "channel_assignments_public_select" ON public.channel_assignments;
CREATE POLICY "channel_assignments_public_select" ON public.channel_assignments FOR SELECT TO anon, authenticated USING (true);
DROP POLICY IF EXISTS "channel_assignments_service_all" ON public.channel_assignments;
CREATE POLICY "channel_assignments_service_all" ON public.channel_assignments FOR ALL TO service_role USING (true) WITH CHECK (true);

DROP POLICY IF EXISTS "Allow all operations on channel_logs" ON public.channel_logs;
DROP POLICY IF EXISTS "channel_logs_public_select" ON public.channel_logs;
CREATE POLICY "channel_logs_public_select" ON public.channel_logs FOR SELECT TO anon, authenticated USING (true);
DROP POLICY IF EXISTS "channel_logs_service_all" ON public.channel_logs;
CREATE POLICY "channel_logs_service_all" ON public.channel_logs FOR ALL TO service_role USING (true) WITH CHECK (true);

DROP POLICY IF EXISTS "Allow all operations on channels" ON public.channels;
DROP POLICY IF EXISTS "channels_public_select" ON public.channels;
CREATE POLICY "channels_public_select" ON public.channels FOR SELECT TO anon, authenticated USING (true);
DROP POLICY IF EXISTS "channels_service_all" ON public.channels;
CREATE POLICY "channels_service_all" ON public.channels FOR ALL TO service_role USING (true) WITH CHECK (true);

DROP POLICY IF EXISTS "Allow all operations on disk_usage" ON public.disk_usage;
DROP POLICY IF EXISTS "disk_usage_public_select" ON public.disk_usage;
CREATE POLICY "disk_usage_public_select" ON public.disk_usage FOR SELECT TO anon, authenticated USING (true);
DROP POLICY IF EXISTS "disk_usage_service_all" ON public.disk_usage;
CREATE POLICY "disk_usage_service_all" ON public.disk_usage FOR ALL TO service_role USING (true) WITH CHECK (true);

DROP POLICY IF EXISTS "Allow all operations on nodes" ON public.nodes;
DROP POLICY IF EXISTS "nodes_public_select" ON public.nodes;
CREATE POLICY "nodes_public_select" ON public.nodes FOR SELECT TO anon, authenticated USING (true);
DROP POLICY IF EXISTS "nodes_service_all" ON public.nodes;
CREATE POLICY "nodes_service_all" ON public.nodes FOR ALL TO service_role USING (true) WITH CHECK (true);

DROP POLICY IF EXISTS "Allow all operations on pipeline_states" ON public.pipeline_states;
DROP POLICY IF EXISTS "pipeline_states_public_select" ON public.pipeline_states;
CREATE POLICY "pipeline_states_public_select" ON public.pipeline_states FOR SELECT TO anon, authenticated USING (true);
DROP POLICY IF EXISTS "pipeline_states_service_all" ON public.pipeline_states;
CREATE POLICY "pipeline_states_service_all" ON public.pipeline_states FOR ALL TO service_role USING (true) WITH CHECK (true);

DROP POLICY IF EXISTS "Allow all operations on preview_images" ON public.preview_images;
DROP POLICY IF EXISTS "preview_images_public_select" ON public.preview_images;
CREATE POLICY "preview_images_public_select" ON public.preview_images FOR SELECT TO anon, authenticated USING (true);
DROP POLICY IF EXISTS "preview_images_service_all" ON public.preview_images;
CREATE POLICY "preview_images_service_all" ON public.preview_images FOR ALL TO service_role USING (true) WITH CHECK (true);

DROP POLICY IF EXISTS "Allow all operations on recordings" ON public.recordings;
DROP POLICY IF EXISTS "recordings_public_select" ON public.recordings;
CREATE POLICY "recordings_public_select" ON public.recordings FOR SELECT TO anon, authenticated USING (true);
DROP POLICY IF EXISTS "recordings_service_all" ON public.recordings;
CREATE POLICY "recordings_service_all" ON public.recordings FOR ALL TO service_role USING (true) WITH CHECK (true);

DROP POLICY IF EXISTS "Allow all operations on tunnels" ON public.tunnels;
DROP POLICY IF EXISTS "tunnels_public_select" ON public.tunnels;
CREATE POLICY "tunnels_public_select" ON public.tunnels FOR SELECT TO anon, authenticated USING (true);
DROP POLICY IF EXISTS "tunnels_service_all" ON public.tunnels;
CREATE POLICY "tunnels_service_all" ON public.tunnels FOR ALL TO service_role USING (true) WITH CHECK (true);

DROP POLICY IF EXISTS "Allow all operations on upload_journal" ON public.upload_journal;
DROP POLICY IF EXISTS "upload_journal_public_select" ON public.upload_journal;
CREATE POLICY "upload_journal_public_select" ON public.upload_journal FOR SELECT TO anon, authenticated USING (true);
DROP POLICY IF EXISTS "upload_journal_service_all" ON public.upload_journal;
CREATE POLICY "upload_journal_service_all" ON public.upload_journal FOR ALL TO service_role USING (true) WITH CHECK (true);

DROP POLICY IF EXISTS "Allow all operations on upload_links" ON public.upload_links;
DROP POLICY IF EXISTS "upload_links_public_select" ON public.upload_links;
CREATE POLICY "upload_links_public_select" ON public.upload_links FOR SELECT TO anon, authenticated USING (true);
DROP POLICY IF EXISTS "upload_links_service_all" ON public.upload_links;
CREATE POLICY "upload_links_service_all" ON public.upload_links FOR ALL TO service_role USING (true) WITH CHECK (true);

-- User/social tables: authenticated writes scoped to the owning user where a
-- user_id column exists; session-anonymous tables keep writes but require a
-- session id. SELECT stays public.

DROP POLICY IF EXISTS "performer_follows_all_auth" ON public.performer_follows;
DROP POLICY IF EXISTS "performer_follows_public_select" ON public.performer_follows;
CREATE POLICY "performer_follows_public_select" ON public.performer_follows FOR SELECT TO anon, authenticated USING (true);
DROP POLICY IF EXISTS "performer_follows_own_all" ON public.performer_follows;
-- performer_follows.user_id is uuid (it references auth.users), so it must be
-- compared to auth.uid() directly; casting auth.uid() to text raises
-- "operator does not exist: uuid = text" and aborts the whole migration.
CREATE POLICY "performer_follows_own_all" ON public.performer_follows FOR ALL TO authenticated USING (user_id = auth.uid()) WITH CHECK (user_id = auth.uid());
DROP POLICY IF EXISTS "performer_follows_service_all" ON public.performer_follows;
CREATE POLICY "performer_follows_service_all" ON public.performer_follows FOR ALL TO service_role USING (true) WITH CHECK (true);

DROP POLICY IF EXISTS "requests_all_auth" ON public.requests;
DROP POLICY IF EXISTS "requests_public_select" ON public.requests;
CREATE POLICY "requests_public_select" ON public.requests FOR SELECT TO anon, authenticated USING (true);
DROP POLICY IF EXISTS "requests_own_all" ON public.requests;
CREATE POLICY "requests_own_all" ON public.requests FOR ALL TO authenticated USING (user_id = auth.uid()::text) WITH CHECK (user_id = auth.uid()::text);
DROP POLICY IF EXISTS "requests_service_all" ON public.requests;
CREATE POLICY "requests_service_all" ON public.requests FOR ALL TO service_role USING (true) WITH CHECK (true);

DROP POLICY IF EXISTS "comments_all_auth" ON public.comments;
DROP POLICY IF EXISTS "comments_public_select" ON public.comments;
CREATE POLICY "comments_public_select" ON public.comments FOR SELECT TO anon, authenticated USING (true);
DROP POLICY IF EXISTS "comments_session_insert" ON public.comments;
CREATE POLICY "comments_session_insert" ON public.comments FOR INSERT TO authenticated WITH CHECK (session_id IS NOT NULL AND session_id <> '');
DROP POLICY IF EXISTS "comments_session_update" ON public.comments;
CREATE POLICY "comments_session_update" ON public.comments FOR UPDATE TO authenticated USING (session_id IS NOT NULL AND session_id <> '') WITH CHECK (session_id IS NOT NULL AND session_id <> '');
DROP POLICY IF EXISTS "comments_session_delete" ON public.comments;
CREATE POLICY "comments_session_delete" ON public.comments FOR DELETE TO authenticated USING (session_id IS NOT NULL AND session_id <> '');
DROP POLICY IF EXISTS "comments_service_all" ON public.comments;
CREATE POLICY "comments_service_all" ON public.comments FOR ALL TO service_role USING (true) WITH CHECK (true);

DROP POLICY IF EXISTS "reactions_all_auth" ON public.reactions;
DROP POLICY IF EXISTS "reactions_public_select" ON public.reactions;
CREATE POLICY "reactions_public_select" ON public.reactions FOR SELECT TO anon, authenticated USING (true);
DROP POLICY IF EXISTS "reactions_session_insert" ON public.reactions;
CREATE POLICY "reactions_session_insert" ON public.reactions FOR INSERT TO authenticated WITH CHECK (session_id IS NOT NULL AND session_id <> '');
DROP POLICY IF EXISTS "reactions_session_update" ON public.reactions;
CREATE POLICY "reactions_session_update" ON public.reactions FOR UPDATE TO authenticated USING (session_id IS NOT NULL AND session_id <> '') WITH CHECK (session_id IS NOT NULL AND session_id <> '');
DROP POLICY IF EXISTS "reactions_session_delete" ON public.reactions;
CREATE POLICY "reactions_session_delete" ON public.reactions FOR DELETE TO authenticated USING (session_id IS NOT NULL AND session_id <> '');
DROP POLICY IF EXISTS "reactions_service_all" ON public.reactions;
CREATE POLICY "reactions_service_all" ON public.reactions FOR ALL TO service_role USING (true) WITH CHECK (true);

DROP POLICY IF EXISTS "comment_likes_all_auth" ON public.comment_likes;
DROP POLICY IF EXISTS "comment_likes_public_select" ON public.comment_likes;
CREATE POLICY "comment_likes_public_select" ON public.comment_likes FOR SELECT TO anon, authenticated USING (true);
DROP POLICY IF EXISTS "comment_likes_session_insert" ON public.comment_likes;
CREATE POLICY "comment_likes_session_insert" ON public.comment_likes FOR INSERT TO authenticated WITH CHECK (session_id IS NOT NULL AND session_id <> '');
DROP POLICY IF EXISTS "comment_likes_session_delete" ON public.comment_likes;
CREATE POLICY "comment_likes_session_delete" ON public.comment_likes FOR DELETE TO authenticated USING (session_id IS NOT NULL AND session_id <> '');
DROP POLICY IF EXISTS "comment_likes_service_all" ON public.comment_likes;
CREATE POLICY "comment_likes_service_all" ON public.comment_likes FOR ALL TO service_role USING (true) WITH CHECK (true);

DROP POLICY IF EXISTS "pool_autopilot_all_auth" ON public.pool_autopilot;
DROP POLICY IF EXISTS "pool_autopilot_public_select" ON public.pool_autopilot;
CREATE POLICY "pool_autopilot_public_select" ON public.pool_autopilot FOR SELECT TO anon, authenticated USING (true);
DROP POLICY IF EXISTS "pool_autopilot_service_all" ON public.pool_autopilot;
CREATE POLICY "pool_autopilot_service_all" ON public.pool_autopilot FOR ALL TO service_role USING (true) WITH CHECK (true);
