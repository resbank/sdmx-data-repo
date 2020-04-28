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

