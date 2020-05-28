(ns spartadata.sdmxapi-test
  (:require [clj-http.client :as client]
            [clojure.data.xml :as xml]
            [clojure.data.zip.xml :as zip-xml]
            [clojure.test :refer :all]
            [clojure.zip :as zip]
            [user]))

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

(deftest sdmx-api
  (testing "Testing the database"
    (user/go)
    (let [agencyid "SARB.ERD"
          id "TEST"
          version "1.0"]
      ; 1. Upload data message of uninitialised dataset
      (client/post (str "http://localhost:3030/sdmxapi/data/upload/historical/" (clojure.string/join "/" [agencyid id version])) 
                   {:query-params {:release "2010-01-01T00:00:00" :releaseDescription "First release"}
                    :multipart [{:name "file" :content (clojure.java.io/file (clojure.java.io/resource "test_message_1.xml"))}]})
      ; => Test that the first data message can be recovered
      ;    Conditions: do not specify release (current unreleased data)
      (let [from-file (-> "test_message_1.xml" clojure.java.io/resource clojure.java.io/input-stream  xml/parse zip/xml-zip)
            from-database (-> (client/get (str "http://localhost:3030/sdmxapi/data/" (clojure.string/join "," [agencyid id version]))
                                          {:query-params {:format "sdmx-2.0"}}) :body xml/parse-str zip/xml-zip )]
        (is (= (get-test-obs from-file "1001" "2001")
               (get-test-obs from-database "1001" "2001"))))
      ; 2. Upload next data message with additional time periods and some values changed from the previous data message 
      (client/post (str "http://localhost:3030/sdmxapi/data/upload/" (clojure.string/join "/" [agencyid id version])) 
                   {:multipart [{:name "file" :content (-> "test_message_2.xml" clojure.java.io/resource clojure.java.io/file)}]})
      ; => Test that the first data message can be recovered (first release)
      ;    Conditions: specify release
      (let [from-file (-> "test_message_1.xml" clojure.java.io/resource clojure.java.io/input-stream xml/parse zip/xml-zip)
            from-database (-> (client/get (str "http://localhost:3030/sdmxapi/data/" (clojure.string/join "," [agencyid id version]))
                                          {:query-params {:format "sdmx-2.0" :release "2010-01-01T00:00:00"}}) :body xml/parse-str zip/xml-zip )]
        (is (= (get-test-obs from-file "1001" "2001")
               (get-test-obs from-database "1001" "2001"))))
      ; => Test that the second data message can be recovered (current unreleased data)
      ;    Conditions: do not specify release
      (let [from-file (-> "test_message_2.xml" clojure.java.io/resource clojure.java.io/input-stream xml/parse zip/xml-zip)
            from-database (-> (client/get (str "http://localhost:3030/sdmxapi/data/" (clojure.string/join "," [agencyid id version]))
                                          {:query-params {:format "sdmx-2.0"}}) :body xml/parse-str zip/xml-zip )]
        (is (and (= (get-test-obs from-file "1001" "2001")
                    (get-test-obs from-database "1001" "2001"))
                 (= (get-test-obs from-file "1002" "2004")
                    (get-test-obs from-database "1002" "2004")))))
      ; 3. Upload next data message as release and again with additional time periods with some values changed from the previous data message 
      (client/post (str "http://localhost:3030/sdmxapi/data/upload/" (clojure.string/join "/" [agencyid id version])) 
                   {:query-params {:releaseDescription "Second release"} 
                    :multipart [{:name "file" :content (-> "test_message_3.xml" clojure.java.io/resource clojure.java.io/file)}]
                    :debug true})
      ; => Test that the first data message can be recovered (first release)
      ;    Conditions: specify release
      (let [from-file (-> "test_message_1.xml" clojure.java.io/resource clojure.java.io/input-stream xml/parse zip/xml-zip)
            from-database (-> (client/get (str "http://localhost:3030/sdmxapi/data/" (clojure.string/join "," [agencyid id version]))
                                          {:query-params {:format "sdmx-2.0" :release "2010-01-01T00:00:00"}}) :body xml/parse-str zip/xml-zip )]
        (is (= (get-test-obs from-file "1001" "2001")
               (get-test-obs from-database "1001" "2001"))))
      ; => Test that the third data message can be recovered (second release)
      ;    Conditions: do not specify release
      (let [from-file (-> "test_message_3.xml" clojure.java.io/resource clojure.java.io/input-stream xml/parse zip/xml-zip)
            from-database (-> (client/get (str "http://localhost:3030/sdmxapi/data/" (clojure.string/join "," [agencyid id version]))
                                          {:query-params {:format "sdmx-2.0"}}) :body xml/parse-str zip/xml-zip )]
        (is (and 
              (= (get-test-obs from-file "1002" "2004")
                 (get-test-obs from-database "1002" "2004"))
              (= (get-test-obs from-file "1002" "2001")
                 (get-test-obs from-database "1002" "2001")))))
      ; 4. Rollback release
      ; => Test that the first data message can be recovered   
      ;    Conditions: do not specify release
      (client/post (str "http://localhost:3030/sdmxapi/data/rollback/" (clojure.string/join "/" [agencyid id version])))
      (let [from-file (-> "test_message_1.xml" clojure.java.io/resource clojure.java.io/input-stream  xml/parse zip/xml-zip)
            from-database (-> (client/get (str "http://localhost:3030/sdmxapi/data/" (clojure.string/join "," [agencyid id version]))
                                          {:query-params {:format "sdmx-2.0"}}) :body xml/parse-str zip/xml-zip )]
        (is (= (get-test-obs from-file "1001" "2001")
               (get-test-obs from-database "1001" "2001"))))
      ; 5. Rollback first data message, dataset should have only empty series
      (client/post (str "http://localhost:3030/sdmxapi/data/rollback/" (clojure.string/join "/" [agencyid id version]))))
    user/halt))
