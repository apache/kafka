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

package kafka.api

import org.junit._
import org.scalatest.junit.JUnitSuite
import org.junit.Assert._
import scala.util.Random
import java.nio.ByteBuffer
import kafka.common.KafkaException
import kafka.utils.TestUtils

object ApiUtilsTest {
  val rnd: Random = new Random()
}

class ApiUtilsTest extends JUnitSuite {

  @Test
  def testShortStringNonASCII() {
    // Random-length strings
    for(i <- 0 to 100) {
      // Since we're using UTF-8 encoding, each encoded byte will be one to four bytes long 
      val s: String = ApiUtilsTest.rnd.nextString(math.abs(ApiUtilsTest.rnd.nextInt()) % (Short.MaxValue / 4))  
      val bb: ByteBuffer = ByteBuffer.allocate(ApiUtils.shortStringLength(s))
      ApiUtils.writeShortString(bb, s)
      bb.rewind()
      assertEquals(s, ApiUtils.readShortString(bb))
    }
  }

  @Test
  def testShortStringASCII() {
    // Random-length strings
    for(i <- 0 to 100) {
      val s: String = TestUtils.randomString(math.abs(ApiUtilsTest.rnd.nextInt()) % Short.MaxValue)  
      val bb: ByteBuffer = ByteBuffer.allocate(ApiUtils.shortStringLength(s))
      ApiUtils.writeShortString(bb, s)
      bb.rewind()
      assertEquals(s, ApiUtils.readShortString(bb))
    }

    // Max size string
    val s1: String = TestUtils.randomString(Short.MaxValue)
    val bb: ByteBuffer = ByteBuffer.allocate(ApiUtils.shortStringLength(s1))
    ApiUtils.writeShortString(bb, s1)
    bb.rewind()
    assertEquals(s1, ApiUtils.readShortString(bb))

    // One byte too big
    val s2: String = TestUtils.randomString(Short.MaxValue + 1) 
    try {
      ApiUtils.shortStringLength(s2)
      fail
    } catch {
      case e: KafkaException => {
        // ok
      }
    }
    try {
      ApiUtils.writeShortString(bb, s2)
      fail
    } catch {
      case e: KafkaException => {
        // ok
      }
    }
  }
}
