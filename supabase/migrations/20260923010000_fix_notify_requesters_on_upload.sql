-- Fix notify_requesters_on_upload(): its two queries could not be planned, which
-- silently blocked EVERY insert into upload_links.
--
-- The function was attached as `AFTER INSERT ON public.upload_links` and its first
-- INSERT ... SELECT joined
--
--     public.user_notification_preferences unp ON unp.user_id = rq.user_id
--
-- where `user_notification_preferences.user_id` is uuid and `requests.user_id` is
-- text.  PL/pgSQL plans a statement when it is first reached, so the trigger raised
--
--     42883: operator does not exist: uuid = text
--
-- at parse analysis for each and every insert — independent of the row's data —
-- and PostgREST surfaced it as HTTP 404 on POST /rest/v1/upload_links.  The DVR
-- fleet stores every host upload as an upload_links row, so from the moment this
-- trigger was installed no recording could become "uploaded" in the database
-- again: recordings kept their enqueue-time row with zero links (visible live as
-- 09-21: 100% of recordings had links, 09-22: 264/345, 09-23: 0/98).
--
-- Casts are needed on both joins:
--   * `unp.user_id::text = rq.user_id` — the requests branch that aborted planning;
--   * `unp.user_id = pf.user_id::text` — user_notification_preferences.user_id is
--     text while performer_follows.user_id is uuid (migration 007 normalizes it to
--     uuid), so the follower branch aborted with 42883 "operator does not exist:
--     text = uuid" as well, blocking the insert even when no user follows the
--     performer. Both branches are fixed here.
--
-- `SET search_path = ''` is added too, matching the hardening migration: the body
-- qualifies every table with public.* already, and a SECURITY DEFINER function
-- should not resolve names through a caller-controlled search path.
--
-- Idempotent: safe to re-run.  The site repo owns this function; carry the same
-- body there so its next deploy does not revert the fix.

CREATE OR REPLACE FUNCTION public.notify_requesters_on_upload()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $function$
    DECLARE
      v_username text;
    BEGIN
      -- Get the performer username from the associated recording
      SELECT r.username INTO v_username
      FROM public.recordings r
      WHERE r.id = NEW.recording_id;

      -- Skip if recording not found or no username
      IF v_username IS NULL THEN
        RETURN NEW;
      END IF;

      -- 1. Notify users with pending or approved requests for this performer
      INSERT INTO public.user_notifications (user_id, type, message, related_id, is_read, created_at)
      SELECT
        rq.user_id,
        'recording_available',
        'A new recording of @' || rq.performer_username || ' on ' || rq.platform || ' is now available in the archive!',
        NEW.recording_id::text,
        false,
        NOW()
      FROM public.requests rq
      LEFT JOIN public.user_notification_preferences unp
        ON unp.user_id::text = rq.user_id
        AND unp.notification_type = 'recording_available'
      WHERE rq.performer_username IS NOT NULL
        AND LOWER(rq.performer_username) = LOWER(v_username)
        AND rq.status IN ('pending', 'approved')
        AND (unp.enabled IS NULL OR unp.enabled = true)
        AND NOT EXISTS (
          SELECT 1 FROM public.user_notifications un
          WHERE un.user_id = rq.user_id
            AND un.type = 'recording_available'
            AND un.related_id = NEW.recording_id::text
        );

      -- 2. Notify users following this performer
      INSERT INTO public.user_notifications (user_id, type, message, related_id, is_read, created_at)
      SELECT
        pf.user_id::text,
        'recording_available',
        'New recording available for @' || pf.performer_username || '!',
        NEW.recording_id::text,
        false,
        NOW()
      FROM public.performer_follows pf
      LEFT JOIN public.user_notification_preferences unp
        ON unp.user_id = pf.user_id::text
        AND unp.notification_type = 'recording_available'
      WHERE LOWER(pf.performer_username) = LOWER(v_username)
        AND (unp.enabled IS NULL OR unp.enabled = true)
        AND NOT EXISTS (
          SELECT 1 FROM public.user_notifications un
          WHERE un.user_id = pf.user_id::text
            AND un.type = 'recording_available'
            AND un.related_id = NEW.recording_id::text
        );

      RETURN NEW;
    END;
    $function$;
