;; Licensed to the Apache Software Foundation (ASF) under one or more
;; contributor license agreements.  See the NOTICE file distributed with
;; this work for additional information regarding copyright ownership.
;; The ASF licenses this file to You under the Apache License, Version 2.0
;; (the "License"); you may not use this file except in compliance with
;; the License.  You may obtain a copy of the License at
;; 
;;    http://www.apache.org/licenses/LICENSE-2.0
;; 
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.
(ns kafka.buffer-test
  (:use (kafka buffer)
        clojure.test))

(deftest test-put-get
  (with-buffer (buffer 64)
    (put (byte 5))
    (put (short 10))
    (put (int 20))
    (put (long 40))
    (put "test")
    (put (byte-array 3 [(byte 1) (byte 2) (byte 3)]))
    (flip)

    (is (= (get-byte)     (byte 5)))
    (is (= (get-short)    (short 10)))
    (is (= (get-int)      (int 20)))
    (is (= (get-long)     (long 40)))
    (is (= (get-string 4) "test"))
    (let [ba (get-array 3)]
      (is (= (nth ba 0) (byte 1)))
      (is (= (nth ba 1) (byte 2)))
      (is (= (nth ba 2) (byte 3))))))

(deftest test-with-put
  (with-buffer (buffer 64)
    (with-put 4 count
      (put "test 1"))
    (flip)

    (is (= (get-int) (int 6)))
    (is (= (get-string 6) "test 1"))))

(deftest test-length-encoded
  (with-buffer (buffer 64)
    (length-encoded short
      (put "test 1"))
    (length-encoded int
      (put "test 2"))
    (flip)

    (is (= (get-short) (short 6)))
    (is (= (get-string 6) "test 1"))
    (is (= (get-int) (int 6)))
    (is (= (get-string 6) "test 2"))))

