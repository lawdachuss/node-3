-- upload_host_backoffs: fleet-wide backoff for shared upload-host credentials.
--
-- Why a shared lease is needed here.  The upload credentials are identical on
-- every node (they come from the same GitHub Actions secrets), so a per-account
-- cap is really a FLEET cap.
--
-- The per-process `timedDisabled` map
-- (uploader/uploader.go) can only ever teach ONE node about that; the other 17
-- each spend their own request to rediscover it, and every node that restarts
-- (the CI runners rotate every ~6h) pays again.  This table carries the verdict
-- between nodes so discovery costs one request fleet-wide, not one per node.
--
-- Contract:
--   * host        — the uploader host name exactly as the fleet names it
--                   ("Vidara", "VoeSX", …), primary key so a re-report
--                   upserts instead of piling up rows.
--   * backoff_until — when the host may be retried (the cap's reset time, or a
--                   conservative window for an unknown reset).
--   * reason      — the host's own wording, surfaced in logs.
--   * updated_by  — reporting node id, so a bad entry is traceable.
--
-- A node reads this on a timer (manager/host_backoff.go) and skips the host
-- while the expiry is in the future, exactly like its in-process timed disable.
-- Nothing here is authoritative: an expired row is simply ignored, so a stale
-- table can never permanently disable a healthy host.
--
-- Degradation: nodes that have not applied this migration get a PGRST205
-- ("table not in schema cache") and fall back to the old per-node behaviour
-- (database.HostBackoffTableMissing) — no crash, no lost upload.
--
-- Idempotent: safe to re-run.

CREATE TABLE IF NOT EXISTS public.upload_host_backoffs (
    host          text        PRIMARY KEY,
    backoff_until timestamptz NOT NULL,
    reason        text,
    updated_by    text,
    updated_at    timestamptz NOT NULL DEFAULT now()
);

-- Scheduled backoffs are read by expiry, so keep the scan on the index.
CREATE INDEX IF NOT EXISTS idx_upload_host_backoffs_until
    ON public.upload_host_backoffs (backoff_until);

-- Backend table: public SELECT (same read access as every other backend table),
-- writes are service_role only — the DVR and coordinator hold the service_role
-- key, which bypasses RLS.  Matches the policies in
-- 20260814000002_harden_rls_and_functions.sql.
ALTER TABLE public.upload_host_backoffs ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "upload_host_backoffs_public_select" ON public.upload_host_backoffs;
CREATE POLICY "upload_host_backoffs_public_select" ON public.upload_host_backoffs
    FOR SELECT TO anon, authenticated USING (true);

DROP POLICY IF EXISTS "upload_host_backoffs_service_all" ON public.upload_host_backoffs;
CREATE POLICY "upload_host_backoffs_service_all" ON public.upload_host_backoffs
    FOR ALL TO service_role USING (true) WITH CHECK (true);

GRANT SELECT ON public.upload_host_backoffs TO anon, authenticated;
GRANT ALL ON public.upload_host_backoffs TO service_role;

SELECT 'upload_host_backoffs created' AS progress;

-- Reload the PostgREST schema cache so the table is queryable immediately.
-- Without this the first reads after deploy return PGRST205 even though the
-- table exists, which would look like "migration not applied" for minutes.
SELECT pg_notify('pgrst', 'reload schema cache');
