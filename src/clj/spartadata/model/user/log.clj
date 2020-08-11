(ns spartadata.model.user.log
  (:require [clojure.data.xml :as xml]
            [clojure.string :as string]
            [hugsql.core :as sql]
            [spartadata.sdmx.errors :refer [sdmx-error]]))



;; Import needed SQL functions


(sql/def-db-fns "sql/query.sql")

(declare get-data-log)
(declare get-data-log-by-date)
(declare get-user-log)
(declare get-user-log-by-date)


;; Log query functions

(defmulti format-dataset-log 
  (fn [content-type _]
    (-> content-type
        (clojure.string/replace #";" "_")
        (clojure.string/replace #"=" "-")
        keyword)))

(defmethod format-dataset-log :application/json
  [_ log-entries]
  (if (first log-entries)
    {:error 0
     :content {:log log-entries}}
    {:error 100
     :content {:error {:code 100
                       :errormessage "Not found. No log entries returned"}}}))

(defmethod format-dataset-log :application/xml
  [_ log-entries]
  (if (first log-entries)
    {:error 0
     :content-type "application/xml"
     :content (->> (map (fn [entry]
                          (xml/element :LogEntry {}
                                       (xml/element :DateModified {} (:modified entry))
                                       (xml/element :Action {} (:action entry))
                                       (xml/element :Username {} (:username entry))
                                       (xml/element :AgencyID {} (:agencyid entry))
                                       (xml/element :id {} (:id entry))
                                       (xml/element :Version {} (:version entry))))
                        log-entries)
                   (xml/element :LogEntries {})
                   (xml/emit-str))}
    {:error 100
     :content-type "application/xml"
     :content (xml/emit-str (sdmx-error 100 "Not found. No log entries returned."))}))

(defn retrieve-data-log [db content-type date]
  (format-dataset-log content-type
                      (if (:afterDateTime date)
                        (get-data-log-by-date db date)
                        (get-data-log db))))



;; Log query functions


(defmulti format-user-log 
  (fn [content-type _]
    (-> content-type
        (clojure.string/replace #";" "_")
        (clojure.string/replace #"=" "-")
        keyword)))

(defmethod format-user-log :application/json
  [_ log-entries]
  (if (first log-entries)
    {:error 0
     :content {:log log-entries}}
    {:error 100
     :content {:error {:code 100
                       :errormessage "Not found. No log entries returned"}}}))

(defmethod format-user-log :application/xml
  [_ log-entries]
  (if (first log-entries)
    {:error 0
     :content-type "application/xml"
     :content (->> (map (fn [entry]
                          (xml/element :LogEntry {}
                                       (xml/element :DateModified {} (:modified entry))
                                       (xml/element :Action {} (:action entry))
                                       (xml/element :Administrator {} (:admin_user entry))
                                       (xml/element :User {} (:target_user entry))))
                        log-entries)
                   (xml/element :LogEntries {})
                   (xml/emit-str))}
    {:error 100
     :content-type "application/xml"
     :content (xml/emit-str (sdmx-error 100 "Not found. No log entries returned."))}))

(defn retrieve-user-log [db content-type date]
  (format-dataset-log content-type
                      (if (:afterDateTime date)
                        (get-user-log-by-date db date)
                        (get-user-log db))))
