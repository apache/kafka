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

package kafka.tools

import java.io.ByteArrayOutputStream
import java.text.SimpleDateFormat

import kafka.utils.Exit
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows}
import org.junit.jupiter.api.Test

class ConsumerPerformanceTest {

  private val outContent = new ByteArrayOutputStream()
  private val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS")

  @Test
  def testDetailedHeaderMatchBody(): Unit = {
    testHeaderMatchContent(detailed = true, 2,
      () => ConsumerPerformance.printConsumerProgress(1, 1024 * 1024, 0, 1, 0, 0, 1, dateFormat, 1L))
  }

  @Test
  def testNonDetailedHeaderMatchBody(): Unit = {
    testHeaderMatchContent(detailed = false, 2, () => println(s"${dateFormat.format(System.currentTimeMillis)}, " +
      s"${dateFormat.format(System.currentTimeMillis)}, 1.0, 1.0, 1, 1.0, 1, 1, 1.1, 1.1"))
  }

  @Test
  def testConfigBrokerList(): Unit = {
    //Given
    val args: Array[String] = Array(
      "--broker-list", "localhost:9092",
      "--topic", "test",
      "--messages", "10"
    )

    //When
    val config = new ConsumerPerformance.ConsumerPerfConfig(args)

    //Then
    assertEquals("localhost:9092", config.brokerHostsAndPorts)
    assertEquals("test", config.topic)
    assertEquals(10, config.numMessages)
  }

  @Test
  def testConfigBootStrapServer(): Unit = {
    //Given
    val args: Array[String] = Array(
      "--bootstrap-server", "localhost:9092",
      "--topic", "test",
      "--messages", "10",
      "--print-metrics"
    )

    //When
    val config = new ConsumerPerformance.ConsumerPerfConfig(args)

    //Then
    assertEquals("localhost:9092", config.brokerHostsAndPorts)
    assertEquals("test", config.topic)
    assertEquals(10, config.numMessages)
  }

  @Test
  def testBrokerListOverride(): Unit = {
    //Given
    val args: Array[String] = Array(
      "--broker-list", "localhost:9094",
      "--bootstrap-server", "localhost:9092",
      "--topic", "test",
      "--messages", "10"
    )

    //When
    val config = new ConsumerPerformance.ConsumerPerfConfig(args)

    //Then
    assertEquals("localhost:9092", config.brokerHostsAndPorts)
    assertEquals("test", config.topic)
    assertEquals(10, config.numMessages)
  }

  @Test
  def testConfigWithUnrecognizedOption(): Unit = {
    Exit.setExitProcedure((_, message) => throw new IllegalArgumentException(message.orNull))
    //Given
    val args: Array[String] = Array(
      "--broker-list", "localhost:9092",
      "--topic", "test",
      "--messages", "10",
      "--new-consumer"
    )
    try assertThrows(classOf[IllegalArgumentException], () => new ConsumerPerformance.ConsumerPerfConfig(args))
    finally Exit.resetExitProcedure()
  }

  private def testHeaderMatchContent(detailed: Boolean, expectedOutputLineCount: Int, fun: () => Unit): Unit = {
    Console.withOut(outContent) {
      ConsumerPerformance.printHeader(detailed)
      fun()

      val contents = outContent.toString.split("\n")
      assertEquals(expectedOutputLineCount, contents.length)
      val header = contents(0)
      val body = contents(1)

      assertEquals(header.split(",").length, body.split(",").length)
    }
  }
}
