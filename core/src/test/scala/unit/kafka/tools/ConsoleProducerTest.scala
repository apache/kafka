/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.tools

import java.util

import ConsoleProducer.LineMessageReader
import org.apache.kafka.clients.producer.ProducerConfig
import org.junit.{Assert, Test}
import Assert.assertEquals
import kafka.utils.Exit

class ConsoleProducerTest {

  val brokerListValidArgs: Array[String] = Array(
    "--broker-list",
    "localhost:1001,localhost:1002",
    "--topic",
    "t3",
    "--property",
    "parse.key=true",
    "--property",
    "key.separator=#"
  )
  val bootstrapServerValidArgs: Array[String] = Array(
    "--bootstrap-server",
    "localhost:1003,localhost:1004",
    "--topic",
    "t3",
    "--property",
    "parse.key=true",
    "--property",
    "key.separator=#"
  )
  val invalidArgs: Array[String] = Array(
    "--t", // not a valid argument
    "t3"
  )
  val bootstrapServerOverride: Array[String] = Array(
    "--broker-list",
    "localhost:1001",
    "--bootstrap-server",
    "localhost:1002",
    "--topic",
    "t3",
  )
  val clientIdOverride: Array[String] = Array(
    "--broker-list",
    "localhost:1001",
    "--topic",
    "t3",
    "--producer-property",
    "client.id=producer-1"
  )

  @Test
  def testValidConfigsBrokerList(): Unit = {
    val config = new ConsoleProducer.ProducerConfig(brokerListValidArgs)
    val producerConfig = new ProducerConfig(ConsoleProducer.producerProps(config))
    assertEquals(util.Arrays.asList("localhost:1001", "localhost:1002"),
      producerConfig.getList(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG))
  }

  @Test
  def testValidConfigsBootstrapServer(): Unit = {
    val config = new ConsoleProducer.ProducerConfig(bootstrapServerValidArgs)
    val producerConfig = new ProducerConfig(ConsoleProducer.producerProps(config))
    assertEquals(util.Arrays.asList("localhost:1003", "localhost:1004"),
      producerConfig.getList(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG))
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testInvalidConfigs(): Unit = {
    Exit.setExitProcedure((_, message) => throw new IllegalArgumentException(message.orNull))
    try {
      new ConsoleProducer.ProducerConfig(invalidArgs)
    } finally {
      Exit.resetExitProcedure()
    }
  }

  @Test
  def testParseKeyProp(): Unit = {
    val config = new ConsoleProducer.ProducerConfig(brokerListValidArgs)
    val reader = Class.forName(config.readerClass).getDeclaredConstructor().newInstance().asInstanceOf[LineMessageReader]
    reader.init(System.in,ConsoleProducer.getReaderProps(config))
    assert(reader.keySeparator == "#")
    assert(reader.parseKey)
  }

  @Test
  def testBootstrapServerOverride(): Unit = {
    val config = new ConsoleProducer.ProducerConfig(bootstrapServerOverride)
    val producerConfig = new ProducerConfig(ConsoleProducer.producerProps(config))
    assertEquals(util.Arrays.asList("localhost:1002"),
      producerConfig.getList(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG))
  }

  @Test
  def testClientIdOverride(): Unit = {
    val config = new ConsoleProducer.ProducerConfig(clientIdOverride)
    val producerConfig = new ProducerConfig(ConsoleProducer.producerProps(config))
    assertEquals("producer-1",
      producerConfig.getString(ProducerConfig.CLIENT_ID_CONFIG))
  }
}
