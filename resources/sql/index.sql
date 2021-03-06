-- :name create-array-idx
-- :command :execute
-- :result :raw
-- :doc Create a multi-column index on the :dimension_ids array.
CREATE INDEX IF NOT EXISTS array_idx ON series (dimension_ids, dataset_id);

-- :name drop-array-idx
-- :command :execute
-- :result :raw
-- :doc Drop array_idx.
DROP INDEX IF EXISTS array_idx;

-- :name create-obs-idx
-- :command :execute
-- :result :raw
-- :doc Create a multi-column index on the observaiton table.
CREATE INDEX IF NOT EXISTS obs_idx ON observation (created, series_id);

-- :name drop-obs-idx
-- :command :execute
-- :result :raw
-- :doc Drop obs_idx.
DROP INDEX IF EXISTS obs_idx;

-- :name create-obs-attr-idx
-- :command :execute
-- :result :raw
-- :doc Create an index on the observation attribute table.
CREATE INDEX IF NOT EXISTS obs_attr_idx ON observation_attribute (observation_id);

-- :name drop-obs-attr-idx
-- :command :execute
-- :result :raw
-- :doc Drop obs_idx.
DROP INDEX IF EXISTS obs_attr_idx;


