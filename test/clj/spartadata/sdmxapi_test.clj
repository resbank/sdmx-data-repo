(ns spartadata.sdmxapi-test
  (:require [clj-http.client :as client]
            [clojure.data.xml :as xml]
            [clojure.data.zip.xml :as zip-xml]
            [clojure.test :refer :all]
            [clojure.zip :as zip]
            [user]))



;; XML namespace


(xml/alias-uri 'ns1 "urn:sdmx:org.sdmx.infomodel.datastructure.Dataflow=SARB.ERD:TEST(1.0):compact")



;; Helper functions


(defn get-test-obs [xml-data dim1 dim2] 
  "Return the time series observations contained in the XML data looked up along the given dimensions"
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



;; Globals


(def agencyid "SARB.ERD")
(def id "TEST")
(def version "1.0")



;; Test suite


(deftest sdmx-api

  ;; Spin up the server.
  (user/go)

  ;; Test 1
  (testing "Historical data set uploads correctly as first release."
    ; Upload data message of (possibly) uninitialised dataset.
    (client/post (str "http://localhost:3030/sdmxapi/data/upload/historical/" (clojure.string/join "/" [agencyid id version])) 
                 {:query-params {:release "2010-01-01T00:00:00" :releaseDescription "First release"}
                  :multipart [{:name "file" :content (clojure.java.io/file (clojure.java.io/resource "test_message_1.xml"))}]})
    ; Test that the first data message can be recovered.
    ; Conditions: do not specify release (current unreleased data).
    (let [from-file (-> "test_message_1.xml" clojure.java.io/resource clojure.java.io/input-stream  xml/parse zip/xml-zip)
          from-database (-> (client/get (str "http://localhost:3030/sdmxapi/data/" (clojure.string/join "," [agencyid id version]))
                                        {:query-params {:format "sdmx-2.0"}}) :body xml/parse-str zip/xml-zip )]
      (is (= (get-test-obs from-file "1001" "2001")
             (get-test-obs from-database "1001" "2001")))))

  ;; Test 2
  (testing "First release can be recovered after adding further (unreleased) data."
    ; Upload next data message with additional time periods and some values changed from the previous data message.
    (client/post (str "http://localhost:3030/sdmxapi/data/upload/" (clojure.string/join "/" [agencyid id version])) 
                 {:multipart [{:name "file" :content (-> "test_message_2.xml" clojure.java.io/resource clojure.java.io/file)}]})
    ; Test that the first data message can be recovered (first release).
    ; Conditions: specify release.
    (let [from-file (-> "test_message_1.xml" clojure.java.io/resource clojure.java.io/input-stream xml/parse zip/xml-zip)
          from-database (-> (client/get (str "http://localhost:3030/sdmxapi/data/" (clojure.string/join "," [agencyid id version]))
                                        {:query-params {:format "sdmx-2.0" :releaseDescription "First release"}}) :body xml/parse-str zip/xml-zip )]
      (is (= (get-test-obs from-file "1001" "2001")
             (get-test-obs from-database "1001" "2001")))))

  ;; Test 3
  (testing "Unreleased data has been correctly updated from the first release."
    ; Test that the second data message can be recovered (current unreleased data).
    ; Conditions: do not specify release, specify key.
    (let [from-file (-> "test_message_2.xml" clojure.java.io/resource clojure.java.io/input-stream xml/parse zip/xml-zip)
          from-database (-> (client/get (str "http://localhost:3030/sdmxapi/data/" (clojure.string/join "," [agencyid id version]) "/.1001")
                                        {:query-params {:format "sdmx-2.0"}}) :body xml/parse-str zip/xml-zip )]
      (is (= (get-test-obs from-file "1001" "2001")
             (get-test-obs from-database "1001" "2001")))))

  ;; Test 4
  (testing "First release can be recovered after adding further data and releasing it as second release"
    ; Upload next data message as release and again with additional time periods with some values changed from the previous data message .
    (client/post (str "http://localhost:3030/sdmxapi/data/upload/" (clojure.string/join "/" [agencyid id version])) 
                 {:query-params {:releaseDescription "Second release"} 
                  :multipart [{:name "file" :content (-> "test_message_3.xml" clojure.java.io/resource clojure.java.io/file)}]})
    ; Test that the first data message can be recovered (first release).
    ; Conditions: specify release (incomplete description).
    (let [from-file (-> "test_message_1.xml" clojure.java.io/resource clojure.java.io/input-stream xml/parse zip/xml-zip)
          from-database (-> (client/get (str "http://localhost:3030/sdmxapi/data/" (clojure.string/join "," [agencyid id version]))
                                        {:query-params {:format "sdmx-2.0" :releaseDescription "First"}}) :body xml/parse-str zip/xml-zip )]
      (is (= (get-test-obs from-file "1001" "2001")
             (get-test-obs from-database "1001" "2001")))))

  ;; Test 5
  (testing "Second release can be validated, recovered and has been correctly updated from the first release."
    ; Test that the third data message can be recovered (second release).
    ; Conditions: specify release (incomplete description).
    (let [from-file (-> "test_message_3.xml" clojure.java.io/resource clojure.java.io/input-stream xml/parse zip/xml-zip)
          from-database (-> (client/get (str "http://localhost:3030/sdmxapi/data/" (clojure.string/join "," [agencyid id version]))
                                        {:query-params {:format "sdmx-2.0" :releaseDescription "Sec release" :validate "true"}}) :body xml/parse-str zip/xml-zip )]
      (is (= (get-test-obs from-file "1002" "2004")
             (get-test-obs from-database "1002" "2004")))
      (is (= (get-test-obs from-file "1002" "2001")
             (get-test-obs from-database "1002" "2001")))))

  ;; Test 6
  (testing "Avaliable releases enquiry returns both the first and second release."
    ; Test that the third data message can be recovered (second release).
    ; Conditions: specify release (incomplete description).
    (let [available-releases (-> (client/get (str "http://localhost:3030/sdmxapi/data/availablereleases/" (clojure.string/join "/" [agencyid id version]))
                                             {:query-params {:format "sdmx-2.0" :releaseDescription "Sec release"}}) :body xml/parse-str zip/xml-zip )]
      (is (= (->> (zip-xml/xml-> available-releases  
                                 :Releases 
                                 :Release)
                  (map zip/node)
                  (map #(get-in % [:attrs :Description]) ))
             (seq ["Second release" "First release" "Initial release"])))))

  ;; Test 7
  (testing "Rollback the second release and return the first release without specifying the release"
      ; Rollback release.
      ; Test that the first data message can be recovered.
      ; Conditions: do not specify release.
      (client/post (str "http://localhost:3030/sdmxapi/data/rollback/" (clojure.string/join "/" [agencyid id version])))
      (let [from-file (-> "test_message_1.xml" clojure.java.io/resource clojure.java.io/input-stream  xml/parse zip/xml-zip)
            from-database (-> (client/get (str "http://localhost:3030/sdmxapi/data/" (clojure.string/join "," [agencyid id version]))
                                          {:query-params {:format "sdmx-2.0"}}) :body xml/parse-str zip/xml-zip )]
        (is (= (get-test-obs from-file "1001" "2001")
               (get-test-obs from-database "1001" "2001"))))
      ; Rollback first data message (clean up).
      (client/post (str "http://localhost:3030/sdmxapi/data/rollback/" (clojure.string/join "/" [agencyid id version]))))

  ;; Take the server down.
  (user/halt)
)
