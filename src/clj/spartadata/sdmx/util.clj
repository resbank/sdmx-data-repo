;; Utility functions used throughout the application


(ns spartadata.sdmx.util
  (:require [clojure.data.xml :as xml]
            [clojure.data.zip.xml :as zip-xml]
            [clojure.java.io :as io]
            [clojure.set :as sets]
            [clojure.string :as string]
            [clojure.zip :as zip]
            [environ.core :refer [env]]))



;; Calculate the string edit distance, used to match the release description in the REST API


(defn levenshtein-distance 
  "Calculate the edit distance between two strings (Wagner–Fischer algorithm)"
  [s t]
  ;; Iterate over the input string (columns)
  (loop [index (map inc (range (count s)))
         col (range (inc (count t)))]
    (if-not index
      (last col)
      (recur (next index) 
             ;; Iterate over the target string (rows)
             (loop [[b & t] t
                    [head & remain] col
                    a (nth s (dec (first index)))
                    next-col [(first index)]]
               (if (empty? remain)
                 next-col
                 (recur t 
                        remain 
                        a 
                        (conj next-col
                              (min (+ (last next-col) 1) 
                                   (+ (first remain) 1)
                                   (+ head (if (= a b) 0 1)))))))))))



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
