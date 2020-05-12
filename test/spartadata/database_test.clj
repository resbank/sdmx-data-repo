(ns spartadata.database-test
  (:require [clojure.data.xml :as xml]
            [clojure.data.zip.xml :as zip-xml]
            [clojure.test :refer :all]
            [clojure.zip :as zip]
            [environ.core :refer [env]]
            [hugsql.core :as sql]
            [java-time]
            [spartadata.database.delete :refer [delete-dataset]]
            [spartadata.database.retrieve :refer [retrieve-data-message]]
            [spartadata.database.rollback :refer [rollback-release]]
            [spartadata.database.upload :refer [upload-data-message]]
            [spartadata.database.upload-historical :refer [upload-historical-data-message]]))

(sql/def-db-fns "sql/query.sql")

(xml/alias-uri 'ns1 "urn:sdmx:org.sdmx.infomodel.datastructure.Dataflow=SARB.ERD:TEST(1.0):compact")

(defn get-test-obs [xml-data dim1 dim2] 
  (->> (zip-xml/xml-> xml-data 
                      ::ns1/DataSet 
                      ::ns1/Series 
                      (zip-xml/attr= :TEST_DIM1 dim1) 
                      (zip-xml/attr= :TEST_DIM2 dim2) 
                      ::ns1/Obs)
       (map zip/node)
       (map :attrs)
       (map #(if-not (instance? java.lang.Double (:OBS_VALUE %)) 
               (assoc % :OBS_VALUE (Double/parseDouble (:OBS_VALUE %)))
               (identity %)))))

(deftest dataset-initialisation
  (testing "Testing the database"
    (let [dbconn {:connection-uri (:sdmx-postgres env)}
          dataset {:agencyid "SARB.ERD" :id "TEST" :version "1.0"}
          xml-file1 (slurp "resources/test/test_message_1.xml")
          xml-file2 (slurp "resources/test/test_message_2.xml")
          xml-file3 (slurp "resources/test/test_message_3.xml")]
      ; 1. Upload data message of uninitialised dataset
      ; => Test that the first data message can be recovered
      ;    Conditions: do not specify release (current unreleased data)
      (upload-historical-data-message dbconn dataset xml-file1 "2010-01-01T00:00:00" :release-description "First release")
      (let [dataset-id (get-dataset-id dbconn dataset)
            latest-release (:embargo (get-latest-release dbconn dataset-id))]
        (let [from-file (-> xml-file1 xml/parse-str zip/xml-zip)
              from-database (zip/xml-zip (retrieve-data-message dbconn {:flow-ref dataset :key nil} :validate? true))]
          (is (= (get-test-obs from-file "1001" "2001")
                 (get-test-obs from-database "1001" "2001"))))
        ; 2. Upload next data message with additional time periods and some values changed from the previous data message 
        ;    Conditions: The union and difference of the first and second data messages should not be null sets
        ;                release = false
        ; => Test that the second data message can be recovered (current unreleased data)
        ;    Conditions: do not specify release
        ; => Test that the first data message can be recovered (first release)
        ;    Conditions: specify release
        (upload-data-message dbconn dataset xml-file2)
        (let [from-file (-> xml-file1 xml/parse-str zip/xml-zip)
              from-database (zip/xml-zip (retrieve-data-message dbconn {:flow-ref dataset :key nil} :validate? true :release latest-release))]
          (is (= (get-test-obs from-file "1001" "2001")
                 (get-test-obs from-database "1001" "2001"))))
        (let [from-file (-> xml-file2 xml/parse-str zip/xml-zip)
              from-database (zip/xml-zip (retrieve-data-message dbconn {:flow-ref dataset :key nil} :validate? true))]
          (is (and (= (get-test-obs from-file "1001" "2001")
                      (get-test-obs from-database "1001" "2001"))
                   (= (get-test-obs from-file "1002" "2004")
                      (get-test-obs from-database "1002" "2004")))))
        ; 3. Upload next data message and again with additional time periods with some values changed from the previous data message 
        ;    Conditions: The union and difference of the second and third data messages should not be null sets
        ;                release = true
        ; => Test that the third data message can be recovered (second release)
        ;    Conditions: specify release
        ; => Test that the first data message can be recovered (first release)
        ;    Conditions: specify release
        (upload-data-message dbconn dataset xml-file3 :release? true :release-description "Second release")
        (let [from-file (-> xml-file1 xml/parse-str zip/xml-zip)
              from-database (zip/xml-zip (retrieve-data-message dbconn {:flow-ref dataset :key nil} :validate? true :release (java-time/sql-timestamp "2010-01-31T00:00:00")))]
          (is (= (get-test-obs from-file "1001" "2001")
                 (get-test-obs from-database "1001" "2001"))))
        (let [latest-release (:embargo (get-latest-release dbconn dataset-id))
              from-file (-> xml-file3 xml/parse-str zip/xml-zip)
              from-database (zip/xml-zip (retrieve-data-message dbconn {:flow-ref dataset :key nil} :validate? true :release latest-release))]
          (is (and (= (get-test-obs from-file "1001" "2001")
                      (get-test-obs from-database "1001" "2001"))
                   (= (get-test-obs from-file "1002" "2004")
                      (get-test-obs from-database "1002" "2004"))
                   (= (get-test-obs from-file "1002" "2001")
                      (get-test-obs from-database "1002" "2001"))))))
      ; 4. Rollback release
      ; => Test that the second data message can be recovered   
      ;    Conditions: do not specify release

      ; 5. Delete dataset
      (delete-dataset dbconn dataset)
      (close-datasource (:datasource dbconn)))))
