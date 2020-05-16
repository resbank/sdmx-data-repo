(ns spartadata.application
  (:require [environ.core :refer [env]]
            [hikari-cp.core :refer [make-datasource close-datasource]]
            [integrant.core :as ig]
            [spartadata.routes :as reitit])
  (:import [javax.naming InitialContext]))

(def system-config (ig/read-string (slurp (clojure.java.io/resource "config.edn"))))

(def app (atom nil))

(defn handler [request] 
  (@app request))

(defmethod ig/init-key :system/handler [_ {connection-pool :cp}]
  (reset! app (reitit/router connection-pool "/SpartaData"))
  @app)

(defmethod ig/init-key :system/connection-pool [_ _]
  (make-datasource {:datasource (.lookup (InitialContext.) "java:/comp/env/jdbc/SpartaData")}))

(defmethod ig/halt-key! :system/connection-pool [_ cp]
  (close-datasource cp))

(defn start []
  (alter-var-root #'system-config ig/init))

(defn stop []
  (alter-var-root #'system-config ig/halt!))
