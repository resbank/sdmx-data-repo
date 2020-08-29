(ns spartadata.model.sdmx.retrieve
  (:require [clojure.data.xml :as xml]
            [clojure.data.zip.xml :as zip-xml]
            [clojure.string :as string]
            [clojure.zip :as zip]
            [hugsql.core :as sql]
            [java-time]
            [spartadata.sdmx.errors :refer [sdmx-error]]
            [spartadata.sdmx.util :refer [levenshtein-distance]]
            [spartadata.sdmx.validate :refer [validate-data]]))



;; Import needed SQL functions


(sql/def-db-fns "sql/query.sql")
(sql/def-db-fns "sql/update.sql")

(declare get-series-and-attrs)
(declare get-series-and-attrs-from-ids)
(declare get-series-dims-by-dataset)

;; Define globals


(xml/alias-uri 'messg "http://www.SDMX.org/resources/SDMXML/schemas/v2_0/message")

(def message-counter (atom 0))



;; SDMX type hierarchy


(derive :application/vnd.sdmx.compact+xml_version-2.0 :application/xml)



;; Retrieve data message


(declare format-response)
(declare compile-data-message)
(declare format-data-message)
(declare retrieve-datasets)
(declare format-dataset)
(declare retrieve-series)
(declare format-series)
(declare retrieve-observations)
(declare format-observation)

(defn retrieve-data-message
  [db content-type query dimensions {validate? :validate :or {validate? false} :as options}]
  (let [data-message (compile-data-message db query dimensions (assoc options :format content-type))]
    (if (= "Error" (name (or (:tag data-message) :nil))) 
      (format-response data-message options)
      (if validate? 
        (if (= 1 (count query))
          (if-let [validation-error (validate-data (:datastructure (first query)) data-message (assoc options :format content-type))]
            (format-response validation-error (assoc options :format content-type))
            (format-response data-message (assoc options :format content-type)))
          (format-response (sdmx-error 1001 "Validation not supported. Multiple dataflows/structures") options))
        (format-response data-message (assoc options :format content-type))))))

(defmulti format-response (fn [data-message options] (if (and (:tag data-message) 
                                                              (= "Error" (name (:tag data-message))))  
                                                       :error 
                                                       (-> (:format options)
                                                           (clojure.string/replace #";" "_")
                                                           (clojure.string/replace #"=" "-")
                                                           keyword))))

(defmethod format-response :error
  [data-message _]
  {:error 500
   :content-type "application/xml"
   :content data-message})

(defmethod format-response :application/json
  [data-message _]
  {:error 0
   :content data-message})

(defmethod format-response :application/xml
  [data-message _]
  {:error 0
   :content-type "application/xml"
   :content (xml/emit-str data-message)})

(defmethod format-response :application/vnd.sdmx.compact+xml_version-2.0
  [data-message _]
  {:error 0
   :content-type "application/vnd.sdmx.compact+xml;version=2.0"
   :content (xml/emit-str data-message)})

;; Compile data message


(defn compile-data-message [db query dimensions options]
  (let [dataset-messages (retrieve-datasets db query dimensions options)]
    (if-not (empty? dataset-messages) 
      (format-data-message dataset-messages options) 
      (sdmx-error 100 (str "No data exist for query: Target: Dataflow"
                           " - Agency Id: " (string/join "+" (map #(get-in % [:datastructure :agencyid]) query))
                           " - Maintainable Id: " (string/join "+" (map #(get-in % [:datastructure :id]) query))
                           " - Version: " (string/join "+" (map #(get-in % [:datastructure :version]) query)))))))

(defmulti format-data-message (fn [dataset-messages options] (-> (:format options)
                                                                 (clojure.string/replace #";" "_")
                                                                 (clojure.string/replace #"=" "-")
                                                                 keyword)))

(defmethod format-data-message :application/json
  [dataset-messages options]
  {:Header {:ID (str "IREF" (swap! message-counter inc))
            :Test false
            :Prepared (str (java-time/local-date-time))
            :Sender {:id "SARB.ERD"}
            :Receiver {:id "ANONYMOUS"}}
   :DataSets dataset-messages})

(defmethod format-data-message :application/xml
  [dataset-messages options]
  (xml/element ::messg/CompactData {}
               (xml/element ::messg/Header {}
                            (xml/element ::messg/ID {} (str "IREF" (swap! message-counter inc)))
                            (xml/element ::messg/Test {} false)
                            (xml/element ::messg/Prepared {} (str (java-time/local-date-time)))
                            (xml/element ::messg/Sender {:id "SARB.ERD"})
                            (xml/element ::messg/Receiver {:id "ANONYMOUS"}))
               dataset-messages))

(defmethod format-data-message :default
  [dataset-messages options]
  (sdmx-error 1002 "Format not supported. Unable to complete request."))

;; Collect datasets


(defn retrieve-datasets [db queries dimensions options]
  (for [query queries
        :let [dataset (get-dataset-and-attrs db (:datastructure query))]
        :when dataset]
    (if-let [description (:releaseDescription options)]
      (let [release (first (sort-by #(levenshtein-distance (:description %) description) < (get-releases db dataset)))]
        (format-dataset db dataset dimensions (-> options 
                                                  (assoc :query query)
                                                  (assoc :releaseDescription (:description release))
                                                  (assoc :release (-> release
                                                                      :release
                                                                      java-time/local-date-time
                                                                      str)))))
      (let [release (first (get-releases db dataset))]
        (format-dataset db dataset dimensions (-> options 
                                                  (assoc :query query)
                                                  (assoc :releaseDescription (:description release))
                                                  (dissoc :release)))))))

(defmulti format-dataset (fn [_ _ _ options] (-> (:format options)
                                                 (clojure.string/replace #";" "_")
                                                 (clojure.string/replace #"=" "-")
                                                 keyword)))

(defmethod format-dataset :application/json
  [db {attrs :attrs values :vals :as dataset} dimensions options]
  (merge (if (:has_release_attr dataset)
           (if (contains? options :release) 
             {:RELEASE (:releaseDescription options)}
             {:RELEASE (str "Unreleased, previous release: " (:releaseDescription options))}) 
           {})
         (if (first (.getArray attrs)) 
           (zipmap (mapv keyword (.getArray attrs)) (into [] (.getArray values))) 
           {})
         {:Series (retrieve-series db dataset dimensions options)}))

(defmethod format-dataset :application/xml
  [db {attrs :attrs values :vals :as dataset} dimensions {{:keys [agencyid id version]} :datastructure :as options}]
  (let [ns1 (str "urn:sdmx:org.sdmx.infomodel.datastructure.Dataflow=" agencyid ":" id "(" version "):compact")]
    (apply xml/element 
           (concat [(xml/qname ns1 "DataSet") 
                    (merge (if (:has_release_attr dataset)
                             (if (contains? options :release) 
                               {:RELEASE (:releaseDescription options)}
                               {:RELEASE (str "Unreleased, previous release: " (:releaseDescription options))}) 
                             {})
                           (if (first (.getArray attrs)) 
                             (zipmap (mapv keyword (.getArray attrs)) (into [] (.getArray values))) 
                             {}))
                    (retrieve-series db dataset dimensions (assoc options :ns1 ns1))]))))

(defmethod format-dataset :default
  [_ _ _ _]
  (sdmx-error 1002 "Format not supported. Unable to complete request."))



;; Collect series

(defn retrieve-series 
  [db dataset series-key {query :query :as options}]
  (let [{{dimensions :dimensions} :datastructure {constraint :constraint} :contentconstraint} query
        query-dimensions (cond
                           (and constraint (not= "all" series-key))
                           (reduce-kv (fn [m k v]
                                        (update m k #(if (seq %) (clojure.set/intersection % v) v)))
                                      (zipmap (map #(keyword (:id %)) dimensions) series-key)
                                      constraint)

                           (and  constraint (= "all" series-key))
                           constraint

                           (and (not constraint) (not= "all" series-key))
                           (zipmap (map #(keyword (:id %)) dimensions) series-key)

                           :else
                           nil)
        series-dimensions (group-by #(keyword (:dim %))
                                    (get-series-dims-by-dataset db dataset))]
    (pmap #(format-series db % options)
          (if query-dimensions
            (->> (reduce-kv (fn [m k v]
                              (update m k (fn [series] (filter #(contains? v (:val %)) series))))
                            series-dimensions
                            query-dimensions)
                 vals
                 (map #(map :series_id %))
                 (map #(into #{} %))
                 (filter seq)
                 (reduce clojure.set/intersection)
                 (hash-map :series_ids)
                 (get-series-and-attrs-from-ids db))
            (get-series-and-attrs db dataset)))))

(defmulti format-series (fn [_ _ options] (-> (:format options)
                                              (clojure.string/replace #";" "_")
                                              (clojure.string/replace #"=" "-")
                                              keyword)))

(defmethod format-series :application/json
  [db {series-id :series_id dims :dims dim-values :dim_vals attrs :attrs attr-values :attr_vals} options] 
  (merge (if (first (.getArray attrs)) (zipmap (mapv keyword (.getArray attrs)) (into [] (.getArray attr-values))) {})
         (zipmap (mapv keyword (.getArray dims)) (into [] (.getArray dim-values)))
         {:Obs (retrieve-observations db series-id options)}))

(defmethod format-series :application/xml
  [db {series-id :series_id dims :dims dim-values :dim_vals attrs :attrs attr-values :attr_vals} {ns1 :ns1 :as options}] 
  (apply xml/element 
         (concat [(xml/qname ns1 "Series") 
                  (merge (if (first (.getArray attrs)) (zipmap (mapv keyword (.getArray attrs)) (into [] (.getArray attr-values))) {})
                         (zipmap (mapv keyword (.getArray dims)) (into [] (.getArray dim-values))))]
                 (retrieve-observations db series-id options))))



;; Collect observations


(defn retrieve-observations [db series-id {release :release :as options}]
  (map #(format-observation % options) 
       (if release  
         (get-obs-and-attrs-by-release db {:release release :series_id series-id})
         (get-obs-and-attrs db {:series_id series-id}))))

(defmulti format-observation (fn [obs options] (-> (:format options)
                                                   (clojure.string/replace #";" "_")
                                                   (clojure.string/replace #"=" "-")
                                                   keyword)))

(defmethod format-observation :application/json 
  [{time-period :time_period obs-value :obs_value attrs :attrs values :vals} _] 
  (into (sorted-map)
        (-> (zipmap (mapv keyword (.getArray attrs)) (into [] (.getArray values))) 
            (assoc :TIME_PERIOD time-period) 
            (assoc :OBS_VALUE obs-value))))

(defmethod format-observation :application/xml
  [{time-period :time_period obs-value :obs_value attrs :attrs values :vals} {ns1 :ns1}] 
  (xml/element (xml/qname ns1 "Obs") 
               (into (sorted-map)
                     (-> (zipmap (mapv keyword (.getArray attrs)) (into [] (.getArray values))) 
                         (assoc :TIME_PERIOD time-period) 
                         (assoc :OBS_VALUE obs-value)))))
