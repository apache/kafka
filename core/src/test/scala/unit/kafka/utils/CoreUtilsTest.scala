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

import java.util
import java.util.{Base64, UUID}
import java.util.concurrent.locks.ReentrantLock
import java.nio.ByteBuffer
import java.util.regex.Pattern
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import kafka.utils.CoreUtils.inLock
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.utils.Utils
import org.slf4j.event.Level


class CoreUtilsTest extends Logging {

  val clusterIdPattern: Pattern = Pattern.compile("[a-zA-Z0-9_\\-]+")

  @Test
  def testSwallow(): Unit = {
    var loggedMessage: Option[String] = None
    val testLogging: Logging = new Logging {
      override def info(msg: => String, e: => Throwable): Unit = {
        loggedMessage = Some(msg+Level.INFO)
      }
      override def debug(msg: => String, e: => Throwable): Unit = {
        loggedMessage = Some(msg+Level.DEBUG)
      }
      override def warn(msg: => String, e: => Throwable): Unit = {
        loggedMessage = Some(msg+Level.WARN)
      }
      override def error(msg: => String, e: => Throwable): Unit = {
        loggedMessage = Some(msg+Level.ERROR)
      }
      override def trace(msg: => String, e: => Throwable): Unit = {
        loggedMessage = Some(msg+Level.TRACE)
      }
    }

    CoreUtils.swallow(throw new KafkaException("test"), testLogging, Level.TRACE)
    assertEquals(Some("test"+Level.TRACE), loggedMessage)
    CoreUtils.swallow(throw new KafkaException("test"), testLogging, Level.DEBUG)
    assertEquals(Some("test"+Level.DEBUG), loggedMessage)
    CoreUtils.swallow(throw new KafkaException("test"), testLogging, Level.INFO)
    assertEquals(Some("test"+Level.INFO), loggedMessage)
    CoreUtils.swallow(throw new KafkaException("test"), testLogging, Level.WARN)
    assertEquals(Some("test"+Level.WARN),loggedMessage)
    CoreUtils.swallow(throw new KafkaException("test"), testLogging, Level.ERROR)
    assertEquals(Some("test"+Level.ERROR),loggedMessage)
  }

  @Test
  def testReadBytes(): Unit = {
    for (testCase <- List("", "a", "abcd")) {
      val bytes = testCase.getBytes
      assertTrue(util.Arrays.equals(bytes, Utils.readBytes(ByteBuffer.wrap(bytes))))
    }
  }

  @Test
  def testAbs(): Unit = {
    assertEquals(0, Utils.abs(Integer.MIN_VALUE))
    assertEquals(1, Utils.abs(-1))
    assertEquals(0, Utils.abs(0))
    assertEquals(1, Utils.abs(1))
    assertEquals(Integer.MAX_VALUE, Utils.abs(Integer.MAX_VALUE))
  }

  @Test
  def testInLock(): Unit = {
    val lock = new ReentrantLock()
    val result = inLock(lock) {
      assertTrue(lock.isHeldByCurrentThread, "Should be in lock")
      1 + 1
    }
    assertEquals(2, result)
    assertFalse(lock.isLocked, "Should be unlocked")
  }

  @Test
  def testUrlSafeBase64EncodeUUID(): Unit = {

    // Test a UUID that has no + or / characters in base64 encoding [a149b4a3-06e1-4b49-a8cb-8a9c4a59fa46 ->(base64)-> oUm0owbhS0moy4qcSln6Rg==]
    val clusterId1 = Base64.getUrlEncoder.withoutPadding.encodeToString(CoreUtils.getBytesFromUuid(UUID.fromString(
      "a149b4a3-06e1-4b49-a8cb-8a9c4a59fa46")))
    assertEquals(clusterId1, "oUm0owbhS0moy4qcSln6Rg")
    assertEquals(clusterId1.length, 22)
    assertTrue(clusterIdPattern.matcher(clusterId1).matches())

    // Test a UUID that has + or / characters in base64 encoding [d418ec02-277e-4853-81e6-afe30259daec ->(base64)-> 1BjsAid+SFOB5q/jAlna7A==]
    val clusterId2 = Base64.getUrlEncoder.withoutPadding.encodeToString(CoreUtils.getBytesFromUuid(UUID.fromString(
      "d418ec02-277e-4853-81e6-afe30259daec")))
    assertEquals(clusterId2, "1BjsAid-SFOB5q_jAlna7A")
    assertEquals(clusterId2.length, 22)
    assertTrue(clusterIdPattern.matcher(clusterId2).matches())
  }

  @Test
  def testGenerateUuidAsBase64(): Unit = {
    val clusterId = CoreUtils.generateUuidAsBase64()
    assertEquals(clusterId.length, 22)
    assertTrue(clusterIdPattern.matcher(clusterId).matches())
  }
}
