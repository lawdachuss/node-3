-- D2 (plan 2026-10-17-fleet-remediation): prune stale `tunnels` rows.
--
-- The table is a legacy registry: nothing writes to it in practice (the
-- operative tunnel registry is `nodes.web_url`, kept fresh by keep-alive.ps1's
-- 5-minute upsert). It still held one DNS-dead row —
-- memphis-unto-outsourcing-meanwhile.trycloudflare.com (instance `default`,
-- 2026-08-10, already flipped to is_active=false) — which old-URL fallbacks
-- could latch onto.
--
-- Note the predicate deliberately does NOT filter on is_active: the observed
-- dead row had is_active=false, so `WHERE is_active AND created_at < ...`
-- would have deleted nothing. Age alone is authoritative here — a
-- trycloudflare tunnel lives ~6h, so anything older than 48h is dead
-- regardless of its flag.
--
-- Idempotent: safe to re-run.
-- RLS policies (`tunnels_public_select` / `tunnels_service_all`) untouched.

DELETE FROM public.tunnels
WHERE created_at < now() - interval '48 hours';
