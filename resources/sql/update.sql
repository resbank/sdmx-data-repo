-- :name insert-dataset
-- :command :query
-- :result :one
-- :doc Insert dataset record, do nothing on conflict.
INSERT INTO dataset (agencyid, id, version)
VALUES (:agencyid, :id, :version) 
RETURNING dataset_id;

-- :name upsert-dataset-attribute
-- :command execute
-- :result :affected
-- :doc Insert attribute, update :val on conflict.
INSERT INTO dataset_attribute (attr, val, dataset_id)
VALUES (:attr, :val, :dataset_id)
ON CONFLICT ON CONSTRAINT dataset_attribute_attr_val_dataset_id_key
DO UPDATE SET val = :val;

-- :name insert-release
-- :command :query
-- :result :one
-- :doc Insert a release date (:embargo) for the given dataset, do nothing on conflict.
INSERT INTO release (embargo, description, dataset_id)
VALUES (:embargo, :description, :dataset_id)
RETURNING embargo;

-- :name insert-dimension
-- :command :query
-- :result :one
-- :doc Insert dimension record.
INSERT INTO dimension (pos, dim, val, dataset_id)
VALUES (:pos, :dim, :val, :dataset_id) 
RETURNING dimension_id;

-- :name insert-series
-- :command :query
-- :result :one
-- :doc Insert series record.
INSERT INTO series (dataset_id, dimension_ids)
VALUES (:dataset_id, ARRAY[:v*:dimension_ids]::INT[]) 
RETURNING series_id;

-- :name upsert-series-dimension
-- :command :execute
-- :result :affected
-- :doc Insert attribute, update :val on conflict.
INSERT INTO series_dimension (series_id, dimension_id)
VALUES (:series_id, :dimension_id)
ON CONFLICT ON CONSTRAINT series_dimension_pkey DO NOTHING;

-- :name upsert-series-attribute
-- :command :execute
-- :result :affected
-- :doc Insert attribute, update :val on conflict.
INSERT INTO series_attribute (attr, val, series_id)
VALUES (:attr, :val, :series_id)
ON CONFLICT ON CONSTRAINT series_attribute_attr_val_series_id_key
DO UPDATE SET val = :val;

-- :name kill-obs
-- :command :execute
-- :result :affected
-- :doc 'Kill' the observation (selected by its ID) by setting the end of its 'lifetime' to the current time.
UPDATE Observation 
SET valid=false 
WHERE observation_id = :observation_id;

-- :name update-obs
-- :command :query
-- :result :one
-- :doc Replace the value of the observation for a given observation ID.
UPDATE observation
SET obs_value = :obs_value
WHERE observation_id = :observation_id
RETURNING observation_id; 

-- :name insert-obs
-- :command :query
-- :result :one
-- :doc Insert a new observation record. Will not throw an error if there is currently a 'live' observation for the given series and time period.
INSERT INTO observation (
  created,
  time_period, 
  obs_value,
  series_id 
)
VALUES (
  :created,
  :time_period,
  :obs_value,
  :series_id
)
RETURNING observation_id;

-- :name insert-obs2
-- :command :query
-- :result :one
-- :doc Insert a new observation record. Will not throw an error if there is currently a 'live' observation for the given series and time period.
INSERT INTO observation (
  created,
  time_period, 
  obs_value,
  series_id 
)
VALUES (
  :created,
  :time_period::TIMESTAMP,
  :obs_value,
  :series_id
)
RETURNING observation_id;

-- :name insert-obs3
-- :command :query
-- :result :one
-- :doc Insert a new observation record. Will not throw an error if there is currently a 'live' observation for the given series and time period.
INSERT INTO observation (
  created,
  valid,
  time_period, 
  obs_value,
  series_id 
)
VALUES (
  :created,
  :valid,
  :time_period::TIMESTAMP,
  :obs_value,
  :series_id
)
ON CONFLICT ON CONSTRAINT observation_series_id_time_period_created_key
DO UPDATE SET 
  created = :created,
  valid = :valid,
  time_period = :time_period::TIMESTAMP,
  obs_value = :obs_value,
  series_id = :series_id
RETURNING observation_id;

-- :name upsert-obs-attribute
-- :command :execute
-- :result :affected
-- :doc Insert attribute, update :val on conflict.
INSERT INTO observation_attribute (attr, val, observation_id)
VALUES (:attr, :val, :observation_id)
ON CONFLICT ON CONSTRAINT observation_attribute_attr_val_observation_id_key
DO UPDATE SET val = :val;

-- :name upsert-obs-attributes
-- :command :execute
-- :result :affected
-- :doc Insert attribute, update :val on conflict.
INSERT INTO observation_attribute (attr, val, observation_id)
VALUES :t*:attrs
ON CONFLICT ON CONSTRAINT observation_attribute_attr_val_observation_id_key
DO UPDATE SET val = excluded.val;

-- :name insert-table
-- :command :execute
-- :result :affected
-- :doc Insert a new observation record. Will not throw an error if there is currently a 'live' observation for the given series and time period.
select * into test from observation where series_id=:series_id;
