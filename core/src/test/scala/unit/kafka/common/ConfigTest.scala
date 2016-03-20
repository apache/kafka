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

package kafka.common

import org.junit.Assert._
import collection.mutable.ArrayBuffer
import org.junit.Test
import kafka.producer.ProducerConfig
import kafka.consumer.ConsumerConfig

class ConfigTest {

  @Test
  @deprecated("This test is deprecated and it will be removed in a future release.", "0.10.0.0")
  def testInvalidClientIds() {
    val invalidClientIds = new ArrayBuffer[String]()
    val badChars = Array('/', '\\', ',', '\u0000', ':', "\"", '\'', ';', '*', '?', ' ', '\t', '\r', '\n', '=')
    for (weirdChar <- badChars) {
      invalidClientIds += "Is" + weirdChar + "illegal"
    }

    for (i <- 0 until invalidClientIds.size) {
      try {
        ProducerConfig.validateClientId(invalidClientIds(i))
        fail("Should throw InvalidClientIdException.")
      }
      catch {
        case e: InvalidConfigException => "This is good."
      }
    }

    val validClientIds = new ArrayBuffer[String]()
    validClientIds += ("valid", "CLIENT", "iDs", "ar6", "VaL1d", "_0-9_.", "")
    for (i <- 0 until validClientIds.size) {
      try {
        ProducerConfig.validateClientId(validClientIds(i))
      }
      catch {
        case e: Exception => fail("Should not throw exception.")
      }
    }
  }

  @Test
  def testInvalidGroupIds() {
    val invalidGroupIds = new ArrayBuffer[String]()
    val badChars = Array('/', '\\', ',', '\u0000', ':', "\"", '\'', ';', '*', '?', ' ', '\t', '\r', '\n', '=')
    for (weirdChar <- badChars) {
      invalidGroupIds += "Is" + weirdChar + "illegal"
    }

    for (i <- 0 until invalidGroupIds.size) {
      try {
        ConsumerConfig.validateGroupId(invalidGroupIds(i))
        fail("Should throw InvalidGroupIdException.")
      }
      catch {
        case e: InvalidConfigException => "This is good."
      }
    }

    val validGroupIds = new ArrayBuffer[String]()
    validGroupIds += ("valid", "GROUP", "iDs", "ar6", "VaL1d", "_0-9_.", "")
    for (i <- 0 until validGroupIds.size) {
      try {
        ConsumerConfig.validateGroupId(validGroupIds(i))
      }
      catch {
        case e: Exception => fail("Should not throw exception.")
      }
    }
  }
}

