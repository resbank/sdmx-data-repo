-- :name create-living-idx
-- :command :execute
-- :result :raw
-- :doc Create a partial index on the :lifetime of all 'living' observations.
CREATE INDEX IF NOT EXISTS living_idx ON observation (valid) WHERE valid=true;

-- :name drop-living-idx
-- :command :execute
-- :result :raw
-- :doc Drop living_idx.
DROP INDEX IF EXISTS living_idx;

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
