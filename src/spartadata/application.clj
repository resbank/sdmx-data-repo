(ns spartadata.application
  (:require [environ.core :refer [env]]
            [hikari-cp.core :refer [make-datasource close-datasource]]
            [integrant.core :as ig]
            [reitit.ring :as ring]
            [ring.adapter.jetty :as jetty]
            [spartadata.routes :refer [router]]))

(def system-config (ig/read-string (slurp "resources/config.edn")))

(defmethod ig/init-key :system/jetty [_ {:keys [port join? handler]}]
  (println "server running on port: " port)
  (jetty/run-jetty handler {:port port :join? join?}))

(defmethod ig/halt-key! :system/jetty [_ server]
  (.stop server))

(defmethod ig/init-key :system/handler [_ {connection-pool :cp}]
  (router connection-pool))

(defmethod ig/init-key :system/connection-pool [_ _]
  (make-datasource {:jdbc-url (:db env)}))

(defmethod ig/halt-key! :system/connection-pool [_ cp]
  (close-datasource cp))

(defn -main []
  (ig/init system-config))
