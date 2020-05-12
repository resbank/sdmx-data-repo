-- :name delete-dataset-by-dataset-id
-- :command :execute
-- :result :affected
-- :doc Delete dataset by id.
DELETE FROM dataset 
WHERE dataset_id=:dataset_id;

-- :name delete-series-by-dataset-id
-- :command :execute
-- :result :affected
-- :doc Delete series by id.
DELETE FROM series 
WHERE dataset_id=:dataset_id;

-- :name delete-obs
-- :command :execute
-- :result :affected
-- :doc Delete observations by ids.
DELETE FROM observation
WHERE observation_id=:observation_id;

-- :name delete-release
-- :command :execute
-- :result :affected
-- :doc Delete release.
DELETE FROM release
WHERE embargo=:embargo::TIMESTAMP
AND dataset_id=:dataset_id;
