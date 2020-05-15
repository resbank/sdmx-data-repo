-- :name insert-dataset
-- :command :query
-- :result :one
-- :doc Insert and return dataset record.
INSERT INTO dataset (agencyid, id, version)
VALUES (:agencyid, :id, :version) 
RETURNING dataset_id;

-- :name upsert-dataset-attribute
-- :command :execute
-- :result :affected
-- :doc Insert attribute, update :val on conflict.
INSERT INTO dataset_attribute (attr, val, dataset_id)
VALUES (:attr, :val, :dataset_id)
ON CONFLICT ON CONSTRAINT dataset_attribute_attr_val_dataset_id_key
DO UPDATE SET val = :val;

-- :name insert-release
-- :command :query
-- :result :one
-- :doc Insert and return a release.
INSERT INTO release (release, description, dataset_id)
VALUES (:release::TIMESTAMP, :description, :dataset_id)
RETURNING release;

-- :name insert-dimension
-- :command :query
-- :result :one
-- :doc Insert and return dimension.
INSERT INTO dimension (pos, dim, val, dataset_id)
VALUES (:pos, :dim, :val, :dataset_id) 
RETURNING dimension_id;

-- :name insert-series
-- :command :query
-- :result :one
-- :doc Insert and return series.
INSERT INTO series (dataset_id, dimension_ids)
VALUES (:dataset_id, ARRAY[:v*:dimension_ids]::INT[]) 
RETURNING series_id;

-- :name upsert-series-dimension
-- :command :execute
-- :result :affected
-- :doc Insert series_dimension, do nothing on conflict.
INSERT INTO series_dimension (series_id, dimension_id)
VALUES (:series_id, :dimension_id)
ON CONFLICT ON CONSTRAINT series_dimension_pkey 
DO NOTHING;

-- :name upsert-series-attribute
-- :command :execute
-- :result :affected
-- :doc Insert attribute, update :val on conflict.
INSERT INTO series_attribute (attr, val, series_id)
VALUES (:attr, :val, :series_id)
ON CONFLICT ON CONSTRAINT series_attribute_attr_val_series_id_key
DO UPDATE SET val = :val;

-- :name upsert-obs
-- :command :query
-- :result :one
-- :doc Insert and return new observation, update on conflict. 
INSERT INTO observation (
  created,
  time_period, 
  obs_value,
  series_id 
)
VALUES (
  :created::TIMESTAMP,
  :time_period::DATE,
  :obs_value,
  :series_id
)
ON CONFLICT ON CONSTRAINT observation_series_id_time_period_created_key
DO UPDATE SET 
  created = :created::TIMESTAMP,
  obs_value = :obs_value
RETURNING observation_id;

-- :name upsert-obs-attributes
-- :command :execute
-- :result :affected
-- :doc Insert attributes, update :val on conflict.
INSERT INTO observation_attribute (attr, val, observation_id)
VALUES :t*:attrs
ON CONFLICT ON CONSTRAINT observation_attribute_attr_val_observation_id_key
DO UPDATE SET val = excluded.val;
