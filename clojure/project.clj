(defproject kafka-clj "0.1-SNAPSHOT"
  :description "Kafka client for Clojure."
  :url          "http://sna-projects.com/kafka/"
  :dependencies [[org.clojure/clojure	"1.2.0"]
                 [org.clojure/clojure-contrib	"1.2.0"]
                 [log4j "1.2.15" :exclusions [javax.mail/mail
                                              javax.jms/jms
                                              com.sun.jdmk/jmxtools
                                              com.sun.jmx/jmxri]]]
  :disable-deps-clean false
  :warn-on-reflection true
  :source-path "src"
  :test-path "test")
