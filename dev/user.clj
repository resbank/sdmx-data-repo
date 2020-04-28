(ns user
  (:require [integrant.repl :as ig-repl]
            [spartadata.application]))

(ig-repl/set-prep! (constantly spartadata.application/system-config))

(def go ig-repl/go)
(def halt ig-repl/halt)
(def reset ig-repl/reset)

(comment
  (go)
  (reset)
  (halt))
