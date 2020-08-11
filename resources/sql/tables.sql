


--------------------------------------------------------------------------------
-- DATA TABLES
--------------------------------------------------------------------------------


-- :name create-dataset-table
-- :command :execute
-- :result :raw
-- :doc Create dataset table, if it doesn't already exist.
CREATE TABLE IF NOT EXISTS dataset (
  dataset_id SERIAL PRIMARY KEY,
  agencyid VARCHAR(50),
  id VARCHAR(50),
  version VARCHAR(5),
  has_release_attr BOOLEAN DEFAULT FALSE,
  UNIQUE (agencyid, id, version)
);

-- :name drop-dataset-table
-- :command :execute
-- :result :raw
-- :doc Drop dataset table, if it exists.
DROP TABLE IF EXISTS dataset;

-- :name create-dataset-attr-table
-- :command :execute
-- :result :raw
-- :doc Create dataset_attribute table, if it doesn't already exist.
CREATE TABLE IF NOT EXISTS dataset_attribute (
  dataset_attribute_id SERIAL PRIMARY KEY,
  attr VARCHAR(50) NOT NULL,
  val TEXT NOT NULL,
  dataset_id INT REFERENCES dataset(dataset_id) ON DELETE CASCADE,
  UNIQUE (attr, val, dataset_id)
);

-- :name drop-dataset-attr-table
-- :command :execute
-- :result :raw
-- :doc Drop dataset_attribute table, if it exists.
DROP TABLE IF EXISTS dataset_attribute;

-- :name create-release-table
-- :command :execute
-- :result :raw
-- :doc Create release table, if it doesn't already exist.
CREATE TABLE IF NOT EXISTS release (
  release_id SERIAL PRIMARY KEY,
  release TIMESTAMP NOT NULL,
  description TEXT NOT NULL,
  dataset_id INT REFERENCES dataset(dataset_id) ON DELETE CASCADE, 
  UNIQUE (release, dataset_id),
  UNIQUE (description, dataset_id)
);

-- :name drop-release-table
-- :command :execute
-- :result :raw
-- :doc Drop release table, if it exists.
DROP TABLE IF EXISTS release;

-- :name create-dimension-table
-- :command :execute
-- :result :raw
-- :doc Create dimension table, if it doesn't already exist.
CREATE TABLE IF NOT EXISTS dimension (
  dimension_id SERIAL PRIMARY KEY,
  pos INT NOT NULL,
  dim VARCHAR(50) NOT NULL,
  val VARCHAR(50) NOT NULL,
  dataset_id INT REFERENCES dataset(dataset_id) ON DELETE CASCADE,
  UNIQUE (dim, val, dataset_id)
);

-- :name drop-dimension-table
-- :command :execute
-- :result :raw
-- :doc Drop dimension table, if it exists.
DROP TABLE IF EXISTS dimension;

-- :name create-series-table
-- :command :execute
-- :result :raw
-- :doc Create series table, if it doesn't already exist.
CREATE TABLE IF NOT EXISTS series (
  series_id SERIAL PRIMARY KEY,
  dimension_ids INT[] UNIQUE NOT NULL,
  dataset_id INT REFERENCES dataset(dataset_id) ON DELETE CASCADE
);

-- :name drop-series-table
-- :command :execute
-- :result :raw
-- :doc Drop series table, if it exists.
DROP TABLE IF EXISTS series;

-- :name create-series-dimension-table
-- :command :execute
-- :result :raw
-- :doc Create series_dimension table, if it doesn't already exist.
CREATE TABLE IF NOT EXISTS series_dimension (
  series_id INT REFERENCES series(series_id) ON DELETE CASCADE,
  dimension_id INT REFERENCES dimension(dimension_id) ON DELETE CASCADE,
  PRIMARY KEY (series_id, dimension_id)
);

-- :name drop-series-dimension-table
-- :command :execute
-- :result :raw
-- :doc Drop series_dimension table, if it exists.
DROP TABLE IF EXISTS series_dimension;

-- :name create-series-attr-table
-- :command :execute
-- :result :raw
-- :doc Create series_attribute table, if it doesn't already exist.
CREATE TABLE IF NOT EXISTS series_attribute (
  series_attribute_id SERIAL PRIMARY KEY,
  attr VARCHAR(50) NOT NULL,
  val TEXT NOT NULL,
  series_id INTEGER REFERENCES series(series_id) ON DELETE CASCADE,
  UNIQUE (attr, val, series_id)
);

-- :name drop-series-attr-table
-- :command :execute
-- :result :raw
-- :doc Drop series_attribute table, if it exists.
DROP TABLE IF EXISTS series_attribute;

-- :name create-observation-table
-- :command :execute
-- :result :raw
-- :doc Create observation table, if it doesn't already exist. 
CREATE TABLE IF NOT EXISTS observation (
  observation_id SERIAL PRIMARY KEY,
  created TIMESTAMP NOT NULL DEFAULT current_timestamp,
  time_period DATE NOT NULL,
  obs_value FLOAT,
  series_id INTEGER REFERENCES series(series_id) ON DELETE CASCADE,
  UNIQUE (series_id, time_period, created)
);

-- :name drop-observation-table
-- :command :execute
-- :result :raw
-- :doc Drop observation table, if it exists.
DROP TABLE IF EXISTS observation;

-- :name create-observation-attr-table
-- :command :execute
-- :result :raw
-- :doc Create observation_attribute table, if it doesn't already exist.
CREATE TABLE IF NOT EXISTS observation_attribute (
  observation_attribute_id SERIAL PRIMARY KEY,
  attr VARCHAR(50) NOT NULL,
  val TEXT NOT NULL,
  observation_id INTEGER REFERENCES observation(observation_id) ON DELETE CASCADE,
  UNIQUE (attr, val, observation_id)
);

-- :name drop-observation-attr-table
-- :command :execute
-- :result :raw
-- :doc Drop observation_attribute table, if it exists.
DROP TABLE IF EXISTS observation_attribute;



--------------------------------------------------------------------------------
-- AUTHORISATION TABLES
--------------------------------------------------------------------------------


-- :name create-authentication-table
-- :command :execute
-- :result :raw
-- :doc Create authentication table, if it doesn't already exist.
CREATE TABLE IF NOT EXISTS authentication (
  user_id SERIAL PRIMARY KEY, 
  username VARCHAR(50) UNIQUE NOT NULL CHECK (username=upper(username)),
  password VARCHAR(50) NOT NULL,
  firstname VARCHAR(50) NOT NULL,
  lastname  VARCHAR(50) NOT NULL,
  email TEXT NOT NULL,
  is_admin BOOLEAN DEFAULT FALSE NOT NULL
);

-- :name drop-authentication-table
-- :command :execute
-- :result :raw
-- :doc Drop authentication table, if it exists.
DROP TABLE IF EXISTS authentication;

-- :name create-role-table
-- :command :execute
-- :result :raw
-- :doc Create data set roles table, if it doesn't already exist.
CREATE TABLE IF NOT EXISTS role (
  role ROLE_ENUM DEFAULT 'user' NOT NULL,
  user_id INTEGER REFERENCES authentication(user_id) ON DELETE CASCADE NOT NULL,
  dataset_id INTEGER REFERENCES dataset(dataset_id) ON DELETE CASCADE NOT NULL,
  PRIMARY KEY (user_id, dataset_id)
);

-- :name drop-role-table
-- :command :execute
-- :result :raw
-- :doc Drop data set roles table, if it exists.
DROP TABLE IF EXISTS role;

-- :name create-provider-table
-- :command :execute
-- :result :raw
-- :doc Create data providers table, if it doesn't already exist.
CREATE TABLE IF NOT EXISTS provider (
  provider_id SERIAL PRIMARY KEY,
  agencyid VARCHAR(50) NOT NULL,
  id VARCHAR(50) NOT NULL,
  user_id INTEGER REFERENCES authentication(user_id) ON DELETE CASCADE NOT NULL
);

-- :name drop-provider-table
-- :command :execute
-- :result :raw
-- :doc Drop data providers table, if it exists.
DROP TABLE IF EXISTS provider;




--------------------------------------------------------------------------------
-- LOG TABLES
--------------------------------------------------------------------------------


-- :name create-dataset-log-table
-- :command :execute
-- :result :raw
-- :doc Create table that logs changes to datasets by user ID, if it doesn't already exist.
CREATE TABLE IF NOT EXISTS dataset_log (
  log_id SERIAL PRIMARY KEY,
  modified TIMESTAMP DEFAULT current_timestamp NOT NULL,
  action ACTION_ENUM NOT NULL,
  username VARCHAR(50) NOT NULL CHECK (username=upper(username)),
  agencyid VARCHAR(50) NOT NULL,
  id VARCHAR(50) NOT NULL,
  version VARCHAR(5) NOT NULL
);

-- :name drop-dataset-log-table
-- :command :execute
-- :result :raw
-- :doc Drop dataset log table, if it exists.
DROP TABLE IF EXISTS dataset_log;

-- :name create-usr-log-table
-- :command :execute
-- :result :raw
-- :doc Create table that logs changes to users by user ID, if it doesn't already exist.
CREATE TABLE IF NOT EXISTS usr_log (
  log_id SERIAL PRIMARY KEY,
  modified TIMESTAMP DEFAULT current_timestamp NOT NULL,
  action USR_ACTION_ENUM NOT NULL,
  admin VARCHAR(50) NOT NULL CHECK (admin=upper(admin)),
  usr VARCHAR(50) NOT NULL CHECK (usr=upper(usr))
);

-- :name drop-usr-log-table
-- :command :execute
-- :result :raw
-- :doc Drop user log table, if it exists.
DROP TABLE IF EXISTS usr_log;
