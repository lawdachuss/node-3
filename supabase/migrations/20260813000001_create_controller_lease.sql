-- Create the controller_lease singleton table used for controller leadership.
--
-- Why this file exists: claim_controller_lease() / release_controller_lease()
-- (see 20260813000002_create_assignment_rpcs.sql) read and write
-- public.controller_lease, but no committed migration ever created it — it was
-- created by hand on the original live database. On a fresh project the RPCs
-- failed with "relation controller_lease does not exist", no node ever became
-- the controller, and the fair-share rebalancer never ran.
--
-- Exactly one row exists (id = 1). expires_at is the lease deadline; the
-- holder renews by calling claim_controller_lease again.
--
-- The idempotent-rls event trigger (009-enable-rls-by-default.sql) turns RLS on
-- for this table automatically; the RPCs are SECURITY DEFINER so they keep
-- working, and no API role gets direct access.
--
-- Idempotent: safe to re-run.

CREATE TABLE IF NOT EXISTS public.controller_lease (
  id          integer PRIMARY KEY,
  node_id     text,
  acquired_at timestamptz DEFAULT now(),
  expires_at  timestamptz NOT NULL DEFAULT now()
);

-- Singleton row, already expired so the first claim wins it.
INSERT INTO public.controller_lease (id, node_id, acquired_at, expires_at)
VALUES (1, NULL, now(), now() - interval '1 second')
ON CONFLICT (id) DO NOTHING;

COMMENT ON TABLE public.controller_lease IS
  'Singleton (id=1) controller leadership lease used by claim_controller_lease/release_controller_lease.';
