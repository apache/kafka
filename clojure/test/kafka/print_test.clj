(ns kafka.print-test
  (:use (kafka types print)
        clojure.test))

(deftest test-pack-unpack
  (is (= "test" (unpack (pack "test"))))
  (is (= 123 (unpack (pack 123))))
  (is (= true (unpack (pack true))))
  (is (= [1 2 3] (unpack (pack [1 2 3]))))
  (is (= {:a 1} (unpack (pack {:a 1}))))
  (is (= '(+ 1 2 3) (unpack (pack '(+ 1 2 3))))))

