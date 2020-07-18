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

(defn filter-dataflows [flow-ref datasets]
  (let [flow-ref (as-> (string/split flow-ref #",") $
                   (if (not= 3 (count $)) (concat $ '("latest")) $)
                   (if (not= 3 (count $)) (concat '("all") $) $))
        datasets (->> datasets
                      (map #(select-keys % [:agencyid :id :version]))
                      (into #{}))]
    (with-open [dataflow-query (io/input-stream (str (:sdmx-registry env) 
                                                     "/sdmxapi/rest/dataflow/" 
                                                     (string/join "/" flow-ref)))] 
      (->> (-> dataflow-query 
               xml/parse
               zip/xml-zip 
               (zip-xml/xml-> ::messg/Structures 
                              ::struc/Dataflows 
                              ::struc/Dataflow 
                              ::struc/Structure
                              :Ref))
           (map zip/node)
           (map :attrs)
           (map #(select-keys % [:agencyID :id :version]))
           (map #(sets/rename-keys % {:agencyID :agencyid}))
           (filter (partial contains? datasets))))))

(defn filter-data-providers [provider-ref providers]
  (let [provider-ref (as-> (string/split (or provider-ref "all") #",") $
                       (if (not= 2 (count $)) (concat '("all") $) $))
        providers (if (not= "all" (second provider-ref)) 
                    (sets/intersection 
                      (into #{} (map :id providers))
                      (into #{} (clojure.string/split (second provider-ref) #"\+")))
                    (into #{} (map :id providers)))]
    (with-open [provider-query (io/input-stream (str (:sdmx-registry env) 
                                                     "/sdmxapi/rest/dataproviderscheme/" 
                                                     (first provider-ref)))] 
      (let [provider-schemes (-> provider-query 
                                 xml/parse
                                 zip/xml-zip 
                                 (zip-xml/xml-> ::messg/Structures 
                                                ::struc/OrganisationSchemes 
                                                ::struc/DataProviderScheme))]
        (reduce concat
                (for [scheme provider-schemes
                      :let [agencyid (zip-xml/xml1-> scheme (zip-xml/attr :agencyID)) 
                            providerids (zip-xml/xml-> scheme ::struc/DataProvider
                                                       (zip-xml/attr :id))]]
                  (->> providerids
                       (map #(hash-map :agencyid agencyid :id %))
                       (filter #(contains? providers (:id %))))))))))

(defn get-provision-agreements []
  (with-open [agreements (io/input-stream (str (:sdmx-registry env) 
                                               "/sdmxapi/rest/provisionagreement"))] 
    (-> agreements 
        xml/parse
        zip/xml-zip 
        (zip-xml/xml-> ::messg/Structures 
                       ::struc/ProvisionAgreements 
                       ::struc/ProvisionAgreement))))

(defn filter-provision-agreements-by [query filter-by]
  (fn []
    (let [filter-key (get {:data-providers ::struc/DataProvider
                           :dataflows ::struc/StructureUsage}
                          (key (first filter-by)))]
      (->> (query)
           (filter (fn [loc] 
                     (->> (-> (zip-xml/xml1-> loc 
                                              filter-key 
                                              :Ref)
                              zip/node
                              :attrs
                              (select-keys [:agencyID :id :version])
                              (sets/rename-keys {:agencyID :agencyid}))
                          (contains? (into #{} (val (first filter-by)))))))))))

(defn format-provision-agreements [query]
  (fn []
    (->> (query) 
         (map zip/node))))

(defn filter-provision-agreements [dataflows providers]
 ((-> get-provision-agreements
    (filter-provision-agreements-by {:dataflows dataflows})
    (filter-provision-agreements-by {:data-providers providers})
    format-provision-agreements)))



;; Reitit middleware

(defn wrap-resolve-data-query [handler connection-pool]
  (fn [request]
    (when-let [user (:identity request)]
      (let [dataflows (filter-dataflows (get-in request [:parameters :path :flow-ref])
                                        (get-datasets {:datasource connection-pool}))
            providers (filter-data-providers (get-in request [:parameters :path :provider-ref])
                                             (get-providers {:datasource connection-pool} user))
            agreements (filter-provision-agreements dataflows
                                                    providers)]
        (-> request
            (assoc :sdmx-data-query agreements)
            handler)))))

(def resolve-data-query
  (middleware/map->Middleware
    {:name ::sdmx-infomodel
     :description "Resolve data query according to SDMX information model."
     :wrap wrap-resolve-data-query}))
