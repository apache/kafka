(ns leiningen.run-example
  (:use [leiningen.compile :only (eval-in-project)]))

(defn run-example
  [project & args]
  (eval-in-project project
    `(do
       (require 'kafka.example)
       (kafka.example/run))))

