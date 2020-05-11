-- :name get-dataset-id
-- :command :query
-- :result :one
-- :doc Return dataset id corresponding to a map of agency/id/version.
SELECT 
  dataset_id
FROM dataset
WHERE agencyid =:agencyid
AND id = :id
AND version = :version;

-- :name get-dataset-attrs
-- :command :query
-- :result :many
-- :doc Return dataset id corresponding to a map of agencyid/id/version.
SELECT 
  attr,
  val
FROM dataset_attribute
WHERE dataset_id=:dataset_id;

-- :name get-latest-release
-- :command query
-- :result :one
-- :doc Return the timestamp of the most recent release of the dataset
SELECT 
  embargo
FROM release
WHERE dataset_id=:dataset_id
ORDER BY embargo DESC
LIMIT 1;

-- :name get-dimension-id
-- :command :query
-- :result :one
-- :doc Return the :dimension_id matching the given :dim, :val, and :dataset_id
SELECT 
  dimension_id
FROM dimension
WHERE dim = :dim
AND val = :val
AND dataset_id = :dataset_id;

-- :name get-dim-ids-by-pos
-- :command :query
-- :result :many
-- :doc Return the :dimension_ids matching the given :pos and :dataset_id, :pos is the position relating to a certain dimension in the data structure definition
SELECT 
  dimension_id
FROM dimension
WHERE pos = :pos
AND dataset_id = :dataset_id;

-- :name get-dim-ids-by-vals
-- :command :query
-- :result :many
-- :doc Return the :dimension_id matchings the given dimension :vals and :dataset_id
SELECT 
  dimension_id
FROM dimension
WHERE val IN (:v*:vals)
AND pos = :pos
AND dataset_id = :dataset_id;

-- :name get-series-id
-- :command :query
-- :result :one
-- :doc Return the :series_id that matches the given dimensions.
SELECT series_id
FROM series 
WHERE dataset_id=:dataset_id
AND dimension_ids=ARRAY[:v*:dimension_ids]::INT[];

-- :name get-series-ids
-- :command :query
-- :result :many
-- :doc Return the :series_id that matches the given dataset ID.
SELECT series_id
FROM series 
WHERE dataset_id=:dataset_id;

-- :name get-series
-- :command :query
-- :result :many
-- :doc Return observation.
SELECT
  series_dimension_tmp.series_id,
  series_dimension_tmp.dims,
  series_dimension_tmp.dim_vals,
  array_agg(series_attribute.attr) AS attrs,
  array_agg(series_attribute.val) AS attr_vals
FROM (
  SELECT
    series.series_id,
    array_agg(dimension.dim) AS dims,
    array_agg(dimension.val) AS dim_vals
  FROM series
  INNER JOIN series_dimension ON series_dimension.series_id=series.series_id
  INNER JOIN dimension ON dimension.dimension_id = series_dimension.dimension_id
  WHERE series.dataset_id=:dataset_id
  GROUP BY series.series_id
) AS series_dimension_tmp
LEFT JOIN series_attribute ON series_attribute.series_id=series_dimension_tmp.series_id
GROUP BY series_dimension_tmp.series_id, series_dimension_tmp.dims, series_dimension_tmp.dim_vals;

-- :name get-series-dims
-- :command :query
-- :result :many
-- :doc Return the dimensions that matches the given series ID.
SELECT 
  dim,
  val
FROM series 
INNER JOIN series_dimension ON series_dimension.series_id = series.series_id
INNER JOIN dimension ON dimension.dimension_id = series_dimension.dimension_id
WHERE series.series_id=:series_id;

-- :name get-series-ids-from-dim-ids
-- :command :query
-- :result :many
-- :doc Return the series IDs that are referenced by  the given dimension IDs
SELECT series_id
FROM series_dimension
WHERE dimension_id IN (:v*:dimension_ids)

-- :name get-series-from-ids
-- :command :query
-- :result :many
-- :doc Return the series IDs that are referenced by  the given dimension IDs
SELECT
  series_dimension_tmp.series_id,
  series_dimension_tmp.dims,
  series_dimension_tmp.dim_vals,
  array_agg(series_attribute.attr) AS attrs,
  array_agg(series_attribute.val) AS attr_vals
FROM (
  SELECT
    series.series_id,
    array_agg(dimension.dim) AS dims,
    array_agg(dimension.val) AS dim_vals
  FROM series
  INNER JOIN series_dimension ON series_dimension.series_id=series.series_id
  INNER JOIN dimension ON dimension.dimension_id = series_dimension.dimension_id
  WHERE series.series_id IN (:v*:series_ids)
  GROUP BY series.series_id
) AS series_dimension_tmp
LEFT JOIN series_attribute ON series_attribute.series_id=series_dimension_tmp.series_id
GROUP BY series_dimension_tmp.series_id, series_dimension_tmp.dims, series_dimension_tmp.dim_vals;


-- :name get-series-attrs
-- :command :query
-- :result :many
-- :doc Return dataset id corresponding to a map of agencyid/id/version.
SELECT 
  attr,
  val
FROM series_attribute
WHERE series_id=:series_id;

-- :name get-obs
-- :command :query
-- :result :many
-- :doc Return observation.
SELECT
  observation.time_period::TEXT,
  observation.obs_value,
  array_agg(observation_attribute.attr) AS attrs,
  array_agg(observation_attribute.val) AS vals
FROM observation
INNER JOIN observation_attribute ON observation_attribute.observation_id=observation.observation_id
WHERE observation.series_id=:series_id
AND observation.valid=true
GROUP BY observation.observation_id, observation.time_period, observation.obs_value
ORDER BY observation.time_period;

-- :name get-obs-by-release
-- :command :query
-- :result :many
-- :doc Return observation.
SELECT
  max(created) AS release, 
  observation.time_period::TEXT,
  observation.obs_value,
  array_agg(observation_attribute.attr) AS attrs,
  array_agg(observation_attribute.val) AS vals
FROM observation
INNER JOIN observation_attribute ON observation_attribute.observation_id=observation.observation_id
WHERE observation.series_id=:series_id
AND created <= :release::TIMESTAMP 
GROUP BY observation.time_period, observation.obs_value
ORDER BY observation.time_period;

-- :name get-obs-attrs
-- :command :query
-- :result :many
-- :doc Return observation attributes from its observation ID
SELECT
  attr,
  val
FROM observation_attribute
WHERE observation_id=:observation_id;

-- :name get-live-obs
-- :command :query
-- :result :one
-- :doc Return the 'live' observation for a given series and time period.
SELECT 
  observation_id, 
  time_period, 
  obs_value, 
  series_id
FROM observation 
WHERE series_id = :series_id 
AND time_period = :time_period 
AND valid=true;

-- :name get-live-obs2
-- :command :query
-- :result :many
-- :doc Return the 'live' observation for a given series and time period.
SELECT 
  created,
  valid,
  time_period::TEXT, 
  obs_value, 
  series_id
FROM observation 
WHERE series_id = :series_id 
AND valid=true;

-- :name get-live-obs3
-- :command :query
-- :result :many
-- :doc Return the 'live' observation for a given series and time period.
SELECT 
  observation_id,
  created,
  valid,
  time_period::TEXT, 
  obs_value, 
  series_id
FROM observation 
WHERE series_id = :series_id 
AND time_period IN (:v*:time_periods)
AND valid=true;

-- :name created-previous-to?
-- :command :query
-- :result :one
-- :doc Tests whether a release is within the 'lifetime' of a particular observation
SELECT 
  created <= :release::TIMESTAMP AS antecedent
FROM observation
WHERE observation_id=:observation_id;
