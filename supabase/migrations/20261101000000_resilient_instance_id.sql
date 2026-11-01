-- Migration: 20261101000000_resilient_instance_id.sql
--
-- Relax NOT NULL on instance_id across tables so omitempty from Go models
-- (and omitted fields in JSON payloads) never abort pipeline upserts.
--
-- Idempotent: safe to re-run.

ALTER TABLE public.recordings ALTER COLUMN instance_id DROP NOT NULL;
ALTER TABLE public.upload_links ALTER COLUMN instance_id DROP NOT NULL;
ALTER TABLE public.preview_images ALTER COLUMN instance_id DROP NOT NULL;
ALTER TABLE public.tunnels ALTER COLUMN instance_id DROP NOT NULL;

-- Also update save_recording_with_links to coalesce instance_id to default
CREATE OR REPLACE FUNCTION public.save_recording_with_links(
  p_rec         jsonb,
  p_links       jsonb,
  p_preview     jsonb
)
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
    COALESCE(NULLIF(p_rec->>'instance_id', ''), 'default')
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
      COALESCE(NULLIF(l->>'instance_id', ''), 'default')
    FROM jsonb_array_elements(p_links) l;
  END IF;

  -- ── 3. Preview images ──────────────────────────────────────────────
  IF p_preview IS NOT NULL AND jsonb_typeof(p_preview) = 'object' THEN
    INSERT INTO public.preview_images (
      filename, recording_id, thumbnail_url, sprite_url, preview_url,
      sprite_vtt_url, instance_id, uploaded_at
    ) VALUES (
      v_filename,
      v_id,
      p_preview->>'thumbnail_url',
      p_preview->>'sprite_url',
      p_preview->>'preview_url',
      p_preview->>'sprite_vtt_url',
      COALESCE(NULLIF(p_preview->>'instance_id', ''), 'default'),
      COALESCE((p_preview->>'uploaded_at')::timestamptz, now())
    )
    ON CONFLICT (filename) DO UPDATE SET
      recording_id   = EXCLUDED.recording_id,
      thumbnail_url  = COALESCE(EXCLUDED.thumbnail_url, preview_images.thumbnail_url),
      sprite_url     = COALESCE(EXCLUDED.sprite_url, preview_images.sprite_url),
      preview_url    = COALESCE(EXCLUDED.preview_url, preview_images.preview_url),
      sprite_vtt_url = COALESCE(EXCLUDED.sprite_vtt_url, preview_images.sprite_vtt_url),
      instance_id    = EXCLUDED.instance_id,
      uploaded_at    = EXCLUDED.uploaded_at;
  END IF;

  RETURN v_id::text;
END;
$fn$;

NOTIFY pgrst, 'reload schema';
