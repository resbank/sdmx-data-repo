(defproject spartadata "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[clojure.java-time "0.3.2"]
                 [com.layerware/hugsql "0.4.9"]
                 [environ "1.1.0"]
                 [hikari-cp "2.10.0"]
                 [integrant "0.8.0"]
                 [metosin/jsonista "0.2.5"]
                 [metosin/muuntaja "0.6.6"]
                 [metosin/reitit "0.4.2"]
                 [org.clojure/clojure "1.10.0"]
                 [org.clojure/data.xml "0.2.0-alpha6"]
                 [org.clojure/data.zip "1.0.0"]
                 [org.clojure/java.jdbc "0.7.10"]
                 [org.clojure/tools.logging "1.0.0"]
                 [org.postgresql/postgresql "42.2.6"]
                 [org.slf4j/slf4j-simple "1.7.21"]
                 [ring/ring-jetty-adapter "1.8.0"]]
  :repl-options {:init-ns user}
  :main ^:skip-aot spartadata.application
  :plugins [[lein-environ "1.1.0"]]
  :target-path "target/%s"
  :jvm-opts ["-Dclojure.tools.logging.factory=clojure.tools.logging.impl/slf4j-factory"]
  :profiles {:dev [:project/dev :profiles/dev]
             :project/dev {:dependencies [[integrant/repl "0.3.1"]] 
                           :source-paths ["dev"]}
             :profiles/dev {}
             :uberjar {:aot :all}}
  :aliases {"init"  ["run" "-m" "spartadata.database.initialise/init"]
            "rollback"  ["run" "-m" "spartadata.database.uninitialise/rollback"]})