-- :name create-role-type
-- :command :execute
-- :result :raw
-- :doc Create an enumerated type, role_enum.
CREATE TYPE role_enum AS ENUM ('user', 'owner');

-- :name drop-role-type
-- :command :execute
-- :result :raw
-- :doc Drop enumerated type, role_enum.
DROP TYPE role_enum;

-- :name create-action-type
-- :command :execute
-- :result :raw
-- :doc Create an enumerated type, action_enum.
CREATE TYPE action_enum AS ENUM ('create', 'upload', 'delete', 'hist_upload', 'release', 'rollback');

-- :name drop-action-type
-- :command :execute
-- :result :raw
-- :doc Drop enumerated type, action_enum.
DROP TYPE action_enum;

-- :name create-usr-action-type
-- :command :execute
-- :result :raw
-- :doc Create an enumerated type, usr_action_enum.
CREATE TYPE usr_action_enum AS ENUM ('create', 'update', 'delete', 'change_role', 'add_provider');

-- :name drop-usr-action-type
-- :command :execute
-- :result :raw
-- :doc Drop enumerated type, usr_action_enum.
DROP TYPE usr_action_enum;
