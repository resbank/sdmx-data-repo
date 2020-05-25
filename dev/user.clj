(ns user
  (:require [environ.core :refer [env]]
            [hikari-cp.core :refer [make-datasource close-datasource]]
            [integrant.core :as ig]
            [integrant.repl :as ig-repl]
            [ring.adapter.jetty :as jetty]
            [spartadata.routes :as reitit]))

(def system-config (ig/read-string (slurp (clojure.java.io/resource "dev-config.edn"))))

(defmethod ig/init-key :system/jetty [_ {:keys [port join? handler]}]
  (println "server running on port: " port)
  (jetty/run-jetty handler {:port port :join? join?}))

(defmethod ig/halt-key! :system/jetty [_ server]
  (.stop server))

(defmethod ig/init-key :system/handler [_ {connection-pool :cp}]
  (reitit/router connection-pool nil))

(defmethod ig/init-key :system/connection-pool [_ _]
  (make-datasource {:jdbc-url (:sdmx-postgres env)}))

(defmethod ig/halt-key! :system/connection-pool [_ cp]
  (close-datasource cp))

(ig-repl/set-prep! (constantly system-config))

(def go ig-repl/go)
(def halt ig-repl/halt)
(def reset ig-repl/reset)
