(ns spartadata.application
  (:require
    [reitit.ring :as ring]
    [ring.adapter.jetty :as jetty]
    [integrant.core :as ig]
    [spartadata.routes :refer [router]]))

(def system-config (ig/read-string (slurp "resources/config.edn")))

(defmethod ig/init-key :system/jetty [_ {:keys [port join? handler]}]
  (println "server running in port" port)
  (jetty/run-jetty handler {:port port :join? join?}))

(defmethod ig/halt-key! :system/jetty [_ server]
  (.stop server))

(defmethod ig/init-key :system/handler [_ _]
  (router))

(defn -main []
  (ig/init system-config))
