(ns spartadata.middleware.data-query-resolution
  (:require [clojure.data.xml :as xml]
            [clojure.data.zip.xml :as zip-xml]
            [clojure.java.io :as io]
            [clojure.set :as sets]
            [clojure.string :as string]
            [clojure.zip :as zip]
            [environ.core :refer [env]]
            [hugsql.core :as sql]
            [reitit.middleware :as middleware]))



;; Import needed SQL functions


(sql/def-db-fns "sql/query.sql")

(declare get-datasets)
(declare get-providers)



;; Find provision agreements


(xml/alias-uri 'messg "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message")
(xml/alias-uri 'struc "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure")
(xml/alias-uri 'com "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common")

(declare resolve-flow-ref)
(declare resolve-provider-ref)

(defn resolve-query [{flow-ref :flow-ref provider-ref :provider-ref :or {flow-ref "all" provider-ref "all"}}]
  (let [datastructures (reduce merge (resolve-flow-ref flow-ref))
        provisionagreements (reduce merge (resolve-provider-ref provider-ref))]
    (->> provisionagreements
         vals
         (map (fn [agreement] 
           (reduce-kv (fn [m k v]
                        (when (get m k)
                          (assoc m k v)))
                      agreement
                      datastructures)))
         (filterv identity)
         (map (fn [agreement] 
                (reduce-kv (fn [m _ k]
                             (cond 
                               (re-find #"DataStructure" k)
                               (sets/rename-keys m {k :datastructure})

                               (re-find #"DataProvider" k)
                               (sets/rename-keys m {k :dataprovider})
                               
                               (re-find #"ContentConstraint" k)
                               (sets/rename-keys m {k :contentconstraint})))
                           agreement
                           (into [] (keys agreement))))))))

(declare format-datastructure)

(defn resolve-flow-ref [flow-ref]
  (let [flow-ref (as-> (string/split flow-ref #",") $
                   (if (not= 3 (count $)) (concat $ '("latest")) $)
                   (if (not= 3 (count $)) (concat '("all") $) $))]
    (with-open [dataflow-query (io/input-stream (str (:sdmx-registry env) 
                                                     "/sdmxapi/rest/dataflow/" 
                                                     (string/join "/" flow-ref)
                                                     "?references=datastructure"))] 
      (let [datastructures (-> dataflow-query 
                               xml/parse
                               zip/xml-zip 
                               (zip-xml/xml-> ::messg/Structures 
                                              ::struc/DataStructures 
                                              ::struc/DataStructure))]
        (map format-datastructure datastructures)))))

(defn format-datastructure [datastructure]
  (hash-map (-> datastructure
                zip/node
                :attrs
                :urn)
            (merge (-> datastructure
                       zip/node
                       :attrs
                       (select-keys [:agencyID :id :version])
                       (sets/rename-keys {:agencyID :agencyid}))
                   {:dimensions (->> (zip-xml/xml-> datastructure
                                                    ::struc/DataStructureComponents 
                                                    ::struc/DimensionList
                                                    ::struc/Dimension)
                                     (map zip/node)
                                     (map :attrs)
                                     (map #(select-keys % [:id :position]))
                                     (sort-by :position)
                                     (into []))})))

(declare format-contentconstraint)
(declare format-provisionagreement)

(defn resolve-provider-ref [provider-ref]
  (let [provider-ref (as-> (string/split (or provider-ref "all") #",") $
                       (if (not= 2 (count $)) (concat '("all") $) $))
        providers (when-not (= "all" (second provider-ref)) 
                    (set (clojure.string/split (second provider-ref) #"\+")))]
    (with-open [provider-query (io/input-stream (str (:sdmx-registry env) 
                                                     "/sdmxapi/rest/provisionagreement/" 
                                                     (first provider-ref)
                                                     "?references=contentconstraint"))] 
      (let [structures (-> provider-query 
                           xml/parse
                           zip/xml-zip 
                           (zip-xml/xml1-> ::messg/Structures))
            constraints (->> (zip-xml/xml-> structures
                                            ::struc/Constraints
                                            ::struc/ContentConstraint)
                             (map format-contentconstraint) 
                             (reduce merge))]
        (->> (zip-xml/xml-> structures
                            ::struc/ProvisionAgreements
                            ::struc/ProvisionAgreement)
             (filter (fn [agreement]
                       (if providers
                         (contains? providers  
                                    (-> (zip-xml/xml1-> agreement
                                                        ::struc/DataProvider
                                                        :Ref)
                                        zip/node
                                        :attrs
                                        :id))
                         true)))
             (map #(format-provisionagreement % constraints)))))))

(defn format-contentconstraint [contentconstraint]
  (let [provisionagreement (zip-xml/xml1-> contentconstraint
                                           ::struc/ConstraintAttachment
                                           ::struc/ProvisionAgreement
                                           :Ref)
        constraints (zip-xml/xml-> contentconstraint
                                   ::struc/DataKeySet
                                   ::struc/Key)]
    (hash-map (-> provisionagreement
                  zip/node
                  :attrs
                  (select-keys [:agencyID :id :version])
                  vals
                  (as-> $ (apply #(str "urn:sdmx:org.sdmx.infomodel"
                                       ".registry.ProvisionAgreement="
                                       %1 ":" %2 "(" %3 ")") 
                                 $)))
              (hash-map (-> contentconstraint
                            zip/node
                            :attrs
                            :urn)
                        (hash-map :constraint
                                  (reduce #(merge-with into %1 %2)
                                          (map (fn [constraint] 
                                                 (zipmap (map keyword 
                                                              (->> (zip-xml/xml-> constraint 
                                                                                  ::com/KeyValue)
                                                                   (map #(-> % zip/node :attrs :id)))) 
                                                         (->> (zip-xml/xml-> constraint 
                                                                             ::com/KeyValue
                                                                             ::com/Value)
                                                              (map #(-> % zip/node :content))
                                                              (map #(into #{} %)))))
                                               constraints)))))))

(defn format-provisionagreement [provisionagreement contentconstraints]
  (let [dataflow (zip-xml/xml1-> provisionagreement
                                 ::struc/StructureUsage
                                 :Ref)
        dataprovider (zip-xml/xml1-> provisionagreement
                                     ::struc/DataProvider
                                     :Ref)]
    (as-> (hash-map (-> provisionagreement
                        zip/node
                        :attrs
                        :urn)
                    (hash-map (-> dataflow
                                  zip/node
                                  :attrs
                                  (select-keys [:agencyID :id :version])
                                  vals
                                  (as-> $ (apply #(str "urn:sdmx:org.sdmx.infomodel"
                                                       ".datastructure.DataStructure="
                                                       %1 ":" %2 "(" %3 ")") 
                                                 $)))
                              (-> dataflow
                                  zip/node
                                  :attrs
                                  (select-keys [:agencyID :id :version])
                                  (sets/rename-keys {:agencyID :agencyid}))

                              (-> dataprovider
                                  zip/node
                                  :attrs
                                  (select-keys [:agencyID :id])
                                  vals
                                  (as-> $ (apply #(str "urn:sdmx:org.sdmx.infomodel"
                                                       ".base.DataProvider="
                                                       %1 ":DATA_PROVIDERS(1.0)." %2) 
                                                 $))) 
                              (-> dataprovider
                                  zip/node
                                  :attrs
                                  (select-keys [:agencyID :id])
                                  (sets/rename-keys {:agencyID :agencyid})))) $
      (reduce-kv (fn [m k v]
                   (if (get m k)
                     (update m k #(merge % v))
                     m)) 
                 $ 
                 contentconstraints))))



;; Reitit middleware


(defn wrap-resolve-data-query [handler]
  (fn [request]
    (if-let [user (:identity request)]
      (let [datasets (->> (get-datasets {:datasource (:conn request)}) 
                          (map #(select-keys % [:agencyid :id :version]))
                          (into #{}))
            providers (->> (get-providers {:datasource (:conn request)} user) 
                           (map #(select-keys % [:agencyid :id]))
                           (into #{}))
            queries (map (fn [query]
                           (cond-> query
                             (not (contains? datasets 
                                             (select-keys (:datastructure query) 
                                                          [:agencyid :id :version]))) 
                             (assoc :datastructure nil)

                             (not (contains? providers 
                                             (select-keys (:dataprovider query) 
                                                          [:agencyid :id :version]))) 
                             (assoc :dataprovider nil)))
                         (resolve-query (get-in request [:parameters :path])))]
        (-> request
            (assoc :sdmx-query queries)
            handler))
      (-> request
          handler))))

(def resolve-data-query
  (middleware/map->Middleware
    {:name ::sdmx-infomodel
     :description "Resolve data query according to SDMX information model."
     :wrap wrap-resolve-data-query}))
