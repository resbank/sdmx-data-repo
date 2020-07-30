(ns spartadata.model.enquire
  (:require [clojure.data.xml :as xml]
            [hugsql.core :as sql]
            [java-time]
            [spartadata.sdmx.errors :refer [sdmx-error]]
            [spartadata.sdmx.util :refer [levenshtein-distance]]))



;; Import needed SQL functions


(sql/def-db-fns "sql/query.sql")



;; Retrieve releases


(defn fetch-release [db dataflow {newest :newest oldest :oldest after :after before :before includes :includes description :description}]
  (let [{:keys [agencyid id version]} dataflow
        dataset (get-dataset db dataflow)]
    (if dataset
      (let [releases (cond->> (get-releases db dataset)
                       before (filter #(java-time/before? (java-time/local-date-time (:release %)) (java-time/local-date-time before)))
                       after (filter #(java-time/after? (java-time/local-date-time (:release %)) (java-time/local-date-time after)))
                       includes (filter #(clojure.string/includes? (:description %) includes)))]
        (if (empty? releases)
          {:error 100
           :content-type "application/xml"
           :content (xml/emit-str (sdmx-error 100 "No results found."))}
          {:error 0
           :content-type "application/xml"
           :content (xml/emit-str (xml/element :Releases {}
                                               (for [release (cond 
                                                               newest [(first releases)]
                                                               oldest [(last releases)]
                                                               description [(first (sort-by #(levenshtein-distance (:description %) description) < releases))]
                                                               :else releases)]
                                                 (xml/element :Release {:Description (:description release) :Date (:release release)}))))}))
      {:error 100
       :content-type "application/xml"
       :content (xml/emit-str (sdmx-error 100 (str "No data exist for query: Target: Dataflow"
                                                   " - Agency Id: " agencyid 
                                                   " - Maintainable Id: " id
                                                   " - Version: " version)))})))

