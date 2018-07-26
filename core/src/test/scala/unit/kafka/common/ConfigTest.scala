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
import kafka.consumer.ConsumerConfig

class ConfigTest {

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
        case _: InvalidConfigException => // This is good
      }
    }

    val validGroupIds = new ArrayBuffer[String]()
    validGroupIds += ("valid", "GROUP", "iDs", "ar6", "VaL1d", "_0-9_.", "")
    for (i <- 0 until validGroupIds.size) {
      try {
        ConsumerConfig.validateGroupId(validGroupIds(i))
      }
      catch {
        case _: Exception => fail("Should not throw exception.")
      }
    }
  }
}

