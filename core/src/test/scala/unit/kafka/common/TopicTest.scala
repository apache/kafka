/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package unit.kafka.common

import junit.framework.Assert._
import collection.mutable.ArrayBuffer
import kafka.common.{Topic, InvalidTopicException}
import org.junit.Test

class TopicTest {

  @Test
  def testInvalidTopicNames() {
    val invalidTopicNames = new ArrayBuffer[String]()
    invalidTopicNames += ("", ".", "..")
    var longName = "ATCG"
    for (i <- 1 to 6)
      longName += longName
    invalidTopicNames += longName
    val badChars = Array('/', '\\', ',', '\u0000', ':', "\"", '\'', ';', '*', '?', ' ', '\t', '\r', '\n', '=')
    for (weirdChar <- badChars) {
      invalidTopicNames += "Is" + weirdChar + "illegal"
    }

    for (i <- 0 until invalidTopicNames.size) {
      try {
        Topic.validate(invalidTopicNames(i))
        fail("Should throw InvalidTopicException.")
      }
      catch {
        case e: InvalidTopicException => "This is good."
      }
    }

    val validTopicNames = new ArrayBuffer[String]()
    validTopicNames += ("valid", "TOPIC", "nAmEs", "ar6", "VaL1d", "_0-9_.")
    for (i <- 0 until validTopicNames.size) {
      try {
        Topic.validate(validTopicNames(i))
      }
      catch {
        case e: Exception => fail("Should not throw exception.")
      }
    }
  }
}
