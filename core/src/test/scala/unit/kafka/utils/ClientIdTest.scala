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

package kafka.utils

import junit.framework.Assert._
import collection.mutable.ArrayBuffer
import kafka.common.InvalidClientIdException
import org.junit.Test

class ClientIdTest {

  @Test
  def testInvalidClientIds() {
    val invalidclientIds = new ArrayBuffer[String]()
    var longName = "ATCG"
    for (i <- 1 to 6)
      longName += longName
    invalidclientIds += longName
    val badChars = Array('/', '\\', ',', '\0', ':', "\"", '\'', ';', '*', '?', '.', ' ', '\t', '\r', '\n', '=')
    for (weirdChar <- badChars) {
      invalidclientIds += "Is" + weirdChar + "funny"
    }

    for (i <- 0 until invalidclientIds.size) {
      try {
        ClientId.validate(invalidclientIds(i))
        fail("Should throw InvalidClientIdException.")
      }
      catch {
        case e: InvalidClientIdException => "This is good."
      }
    }

    val validClientIds = new ArrayBuffer[String]()
    validClientIds += ("valid", "CLIENT", "iDs", "ar6", "VaL1d", "_0-9_", "")
    for (i <- 0 until validClientIds.size) {
      try {
        ClientId.validate(validClientIds(i))
      }
      catch {
        case e: Exception => fail("Should not throw exception.")
      }
    }
  }
}