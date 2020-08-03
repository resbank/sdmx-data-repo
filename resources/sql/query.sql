


--------------------------------------------------------------------------------
-- DATA QUERIES
--------------------------------------------------------------------------------

-- :name get-datasets
-- :command :query
-- :result :many
-- :doc Return all datasets.
SELECT *
FROM dataset;

-- :name get-dataset
-- :command :query
-- :result :one
-- :doc Return dataset corresponding to a map of :agency/:id/:version.
SELECT 
  dataset_id
FROM dataset
WHERE agencyid =:agencyid
AND id = :id
AND version = :version;

-- :name get-dataset-and-attrs
-- :command :query
-- :result :one
-- :doc Return the observations along with attributes corresponding to :series_id.
SELECT
  dataset.dataset_id,
  dataset.has_release_attr,
  array_agg(dataset_attribute.attr) AS attrs,
  array_agg(dataset_attribute.val) AS vals
FROM dataset
LEFT JOIN dataset_attribute ON dataset_attribute.dataset_id=dataset.dataset_id
WHERE dataset.agencyid=:agencyid
AND dataset.id=:id
AND dataset.version=:version
GROUP BY dataset.dataset_id;

-- :name get-latest-release
-- :command :query
-- :result :one
-- :doc Return the timestamp of the most recent release corresponding to a map of :dataset_id.
SELECT *
FROM release
WHERE dataset_id=:dataset_id
ORDER BY release DESC
LIMIT 1;

-- :name get-releases
-- :command :query
-- :result :many
-- :doc Return the timestamps of the the releases (latest first)  corresponding to a map of :dataset_id.
SELECT *
FROM release
WHERE dataset_id=:dataset_id
ORDER BY release DESC;

-- :name get-dimension
-- :command :query
-- :result :one
-- :doc Return the dimension corresponding to a map of :dim/:val/:dataset_id.
SELECT 
  dimension_id
FROM dimension
WHERE dim = :dim
AND val = :val
AND dataset_id = :dataset_id;

-- :name get-dims-by-series
-- :command :query
-- :result :many
-- :doc Return the dimensions corresponding to a map of :series_id.
SELECT 
  dim,
  val
FROM series 
INNER JOIN series_dimension ON series_dimension.series_id = series.series_id
INNER JOIN dimension ON dimension.dimension_id = series_dimension.dimension_id
WHERE series.series_id=:series_id;

-- :name get-dims-by-pos
-- :command :query
-- :result :many
-- :doc Return the dimensions corresponding to a map of :pos/:dataset_id. NB, the position of the dimension in the data structure definition is denoted by pos.
SELECT 
  dimension_id
FROM dimension
WHERE pos = :pos
AND dataset_id = :dataset_id;

-- :name get-dims-by-vals
-- :command :query
-- :result :many
-- :doc Return the dimensions corresponding to a map of :v*:vals/:pos/:dataset_id. NB, the position of the dimension in the data structure definition is denoted by pos.
SELECT 
  dimension_id
FROM dimension
WHERE val IN (:v*:vals)
AND pos = :pos
AND dataset_id = :dataset_id;

-- :name get-series
-- :command :query
-- :result :many
-- :doc Return the series corresponding to a map of :dataset_id.
SELECT series_id
FROM series 
WHERE dataset_id=:dataset_id;

-- :name match-single-series
-- :command :query
-- :result :one
-- :doc Match series to a map of :v*:dimension_ids.
SELECT 
  series_id, 
  array_agg(dimension_id) AS dims 
FROM series_dimension 
WHERE dimension_id IN (:v*:dimension_ids) 
GROUP BY series_id 
HAVING sort(array_agg(dimension_id))=sort(ARRAY[:v*:dimension_ids]::INT[]);

-- :name match-series
-- :command :query
-- :result :many
-- :doc Match series to a map of :v*:dimension_ids.
SELECT series_id
FROM series_dimension
WHERE dimension_id IN (:v*:dimension_ids)

-- :name get-series-and-attrs
-- :command :query
-- :result :many
-- :doc Return the series along with dimensions and attributes corresponding to a map of :dataset_id.
SELECT
  series_and_dimensions_tmp.series_id,
  series_and_dimensions_tmp.dims,
  series_and_dimensions_tmp.dim_vals,
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
) AS series_and_dimensions_tmp
LEFT JOIN series_attribute ON series_attribute.series_id=series_and_dimensions_tmp.series_id
GROUP BY series_and_dimensions_tmp.series_id, series_and_dimensions_tmp.dims, series_and_dimensions_tmp.dim_vals;

-- :name get-series-and-attrs-from-ids
-- :command :query
-- :result :many
-- :doc Return the series along with dimensions and attributes corresponding to a map of :v*:series_ids.
SELECT
  series_and_dimensions_tmp.series_id,
  series_and_dimensions_tmp.dims,
  series_and_dimensions_tmp.dim_vals,
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
) AS series_and_dimensions_tmp
LEFT JOIN series_attribute ON series_attribute.series_id=series_and_dimensions_tmp.series_id
GROUP BY series_and_dimensions_tmp.series_id, series_and_dimensions_tmp.dims, series_and_dimensions_tmp.dim_vals;

-- :name get-obs
-- :command :query
-- :result :many
-- :doc Return observations corresponding to a map of :series_id.
SELECT DISTINCT ON (time_period) 
  created, 
  time_period::TEXT, 
  obs_value,
  series_id
FROM observation
WHERE series_id=:series_id
ORDER BY time_period, created DESC;

-- :name get-obs-following-release
-- :command :query
-- :result :many
-- :doc Return observations from the requested series that follows the release in the map :release/:series_id.
SELECT observation_id
FROM observation
WHERE series_id=:series_id
AND created > :release::TIMESTAMP;

-- :name get-obs-following-release-by-dataset
-- :command :query
-- :result :one
-- :doc Return an observation from the requested dataset that follows the release in the map :release/:dataset_id.
SELECT
 observation.created 
FROM series
INNER JOIN observation ON observation.series_id=series.series_id
WHERE series.dataset_id=:dataset_id
AND observation.created > :release::TIMESTAMP
LIMIT 1;

-- :name get-obs-and-attrs
-- :command :query
-- :result :many
-- :doc Return the observations along with attributes corresponding to :series_id.
SELECT DISTINCT ON (observation.time_period) 
  observation.observation_id,
  observation.created, 
  observation.time_period::TEXT,
  observation.obs_value,
  observation.series_id,
  array_agg(observation_attribute.attr) AS attrs,
  array_agg(observation_attribute.val) AS vals
FROM observation
INNER JOIN observation_attribute ON observation_attribute.observation_id=observation.observation_id
WHERE observation.series_id=:series_id
GROUP BY 
  observation.observation_id, 
  observation.created, 
  observation.time_period, 
  observation.obs_value,
  observation.series_id
ORDER BY observation.time_period, observation.created DESC;

-- :name get-obs-and-attrs-by-release
-- :command :query
-- :result :many
-- :doc Return the observations along with attributes corresponding to :series_id/:release.
SELECT DISTINCT ON (observation.time_period) 
  observation.time_period::TEXT,
  observation.obs_value,
  array_agg(observation_attribute.attr) AS attrs,
  array_agg(observation_attribute.val) AS vals
FROM observation
INNER JOIN observation_attribute ON observation_attribute.observation_id=observation.observation_id
WHERE observation.series_id=:series_id
AND created <= :release::TIMESTAMP 
GROUP BY observation.time_period, observation.obs_value, observation.created
ORDER BY observation.time_period, observation.created DESC;



--------------------------------------------------------------------------------
-- AUTHORISATION QUERIES
--------------------------------------------------------------------------------


-- :name get-user
-- :command :query
-- :result :one
-- :doc Return user information corresponding to the supplied :username.
SELECT *
FROM authentication
WHERE username=upper(:username);

-- :name get-users
-- :command :query
-- :result :many
-- :doc Return user information corresponding to the supplied :username.
SELECT *
FROM authentication;

-- :name get-dataset-roles
-- :command :query
-- :result :many
-- :doc Return the data set roles for a given :user_id & :dataset_id.
SELECT *
FROM role;

-- :name get-dataset-role
-- :command :query
-- :result :one
-- :doc Return the data set roles for a given :user_id & :dataset_id.
SELECT *
FROM role
WHERE user_id=:user_id
AND dataset_id=:dataset_id;

-- :name get-providers
-- :command :query
-- :result :many
-- :doc Return the provider IDs registered to the given :user_id.
SELECT * 
FROM provider
WHERE user_id=:user_id;

-- :name get-provider
-- :command :query
-- :result one
-- :doc Return the provider ID corresponding to :user_id & :agencyid & :id.
SELECT * 
FROM provider
WHERE user_id=:user_id
AND agencyid=:agencyid
AND id=:id;

-- :name get-roles
-- :command :query
-- :result :many
-- :doc Return the provider IDs registered to the given :user_id.
SELECT 
  role.role,
  dataset.agencyid,
  dataset.id,
  dataset.version
FROM role
INNER JOIN dataset ON role.dataset_id=dataset.dataset_id
WHERE role.user_id=:user_id;

-- :name get-role
-- :command :query
-- :result :one
-- :doc Return role corresponding to :role & :user_id & :dataset_id.
SELECT * 
FROM role
WHERE role=:role::ROLE_ENUM
AND user_id=:user_id
AND dataset_id=:dataset_id;



--------------------------------------------------------------------------------
-- LOG QUERIES
--------------------------------------------------------------------------------


-- :name get-data-set-log
-- :command :query
-- :result :many
-- :doc Return data set log.
SELECT *
FROM dataset_log;

-- :name get-data-set-log-by-date
-- :command :query
-- :result :many
-- :doc Return data set log starting at :startdate.
SELECT *
FROM dataset_log
WHERE modified > :startdate::TIMESTAMP;

-- :name get-usr-log
-- :command :query
-- :result :many
-- :doc Return user log.
SELECT *
FROM usr_log;

-- :name get-usr-log-by-date
-- :command :query
-- :result :many
-- :doc Return user log starting at :startdate.
SELECT *
FROM usr_log
WHERE modified > :startdate::TIMESTAMP;
