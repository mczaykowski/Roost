ALTER TABLE roost_work_meta
  ADD COLUMN IF NOT EXISTS children JSONB NOT NULL DEFAULT '[]'::jsonb;
