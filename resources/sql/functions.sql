-- :name creat-array-sort-fn
-- :command :execute
-- :result :raw
-- :doc Create function for sorting arrays.
CREATE OR REPLACE FUNCTION array_sort (ANYARRAY)
RETURNS ANYARRAY LANGUAGE SQL
AS $$
SELECT ARRAY(SELECT unnest($1) ORDER BY 1)
$$;

-- :name create-intarray-extension
-- :command :execute
-- :result :raw
-- :doc Add int array extension.
CREATE EXTENSION IF NOT EXISTS intarray;
