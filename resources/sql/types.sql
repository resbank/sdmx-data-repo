-- :name create-role-type
-- :command :execute
-- :result :raw
-- :doc Create an enumerated type, role_enum.
CREATE TYPE role_enum AS ENUM ('user', 'owner', 'admin');

-- :name create-action-type
-- :command :execute
-- :result :raw
-- :doc Create an enumerated type, action_enum.
CREATE TYPE action_enum AS ENUM ('delete', 'upload', 'hist_upload', 'release', 'rollback');

-- :name create-usr-action-type
-- :command :execute
-- :result :raw
-- :doc Create an enumerated type, usr_action_enum.
CREATE TYPE usr_action_enum AS ENUM ('create', 'update', 'delete', 'change_role', 'add_provider');
