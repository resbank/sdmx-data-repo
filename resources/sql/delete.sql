


--------------------------------------------------------------------------------
-- DATA DELETIONS
--------------------------------------------------------------------------------


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
WHERE release=:release::TIMESTAMP
AND dataset_id=:dataset_id;



--------------------------------------------------------------------------------
-- AUTHORISATION DELETIONS
--------------------------------------------------------------------------------


-- :name delete-user
-- :command :execute
-- :result :affected
-- :doc Delete user.
DELETE FROM authentication
WHERE username=upper(:username);

-- :name delete-role
-- :command :execute
-- :result :affected
-- :doc Delete data set role.
DELETE FROM role
WHERE role=:role
AND user_id=:user_id
AND dataset_id=:dataset_id;

-- :name delete-provider
-- :command :execute
-- :result :affected
-- :doc Delete data provider.
DELETE FROM provider
WHERE agencyid=:agencyid
AND id=:id
AND user_id=:user_id;
