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

import java.nio.file.Files
import kafka.tools.ConsoleProducer.LineMessageReader
import kafka.utils.{Exit, TestUtils}
import org.apache.kafka.clients.producer.ProducerConfig
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

import java.util

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
  val batchSizeOverriddenByMaxPartitionMemoryBytesValue: Array[String] = Array(
    "--broker-list",
    "localhost:1001",
    "--bootstrap-server",
    "localhost:1002",
    "--topic",
    "t3",
    "--batch-size",
    "123",
    "--max-partition-memory-bytes",
    "456"
  )
  val btchSizeSetAndMaxPartitionMemoryBytesNotSet: Array[String] = Array(
    "--broker-list",
    "localhost:1001",
    "--bootstrap-server",
    "localhost:1002",
    "--topic",
    "t3",
    "--batch-size",
    "123"
  )
  val batchSizeNotSetAndMaxPartitionMemoryBytesSet: Array[String] = Array(
    "--broker-list",
    "localhost:1001",
    "--bootstrap-server",
    "localhost:1002",
    "--topic",
    "t3",
    "--max-partition-memory-bytes",
    "456"
  )
  val batchSizeDefault: Array[String] = Array(
    "--broker-list",
    "localhost:1001",
    "--bootstrap-server",
    "localhost:1002",
    "--topic",
    "t3"
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

  @Test
  def testInvalidConfigs(): Unit = {
    Exit.setExitProcedure((_, message) => throw new IllegalArgumentException(message.orNull))
    try assertThrows(classOf[IllegalArgumentException], () => new ConsoleProducer.ProducerConfig(invalidArgs))
    finally Exit.resetExitProcedure()
  }

  @Test
  def testParseKeyProp(): Unit = {
    val config = new ConsoleProducer.ProducerConfig(brokerListValidArgs)
    val reader = Class.forName(config.readerClass).getDeclaredConstructor().newInstance().asInstanceOf[LineMessageReader]
    reader.init(System.in, ConsoleProducer.getReaderProps(config))
    assertTrue(reader.keySeparator == "#")
    assertTrue(reader.parseKey)
  }

  @Test
  def testParseReaderConfigFile(): Unit = {
    val propsFile = TestUtils.tempFile()
    val propsStream = Files.newOutputStream(propsFile.toPath)
    propsStream.write("parse.key=true\n".getBytes())
    propsStream.write("key.separator=|".getBytes())
    propsStream.close()

    val args = Array(
      "--bootstrap-server", "localhost:9092",
      "--topic", "test",
      "--property", "key.separator=;",
      "--property", "parse.headers=true",
      "--reader-config", propsFile.getAbsolutePath
    )
    val config = new ConsoleProducer.ProducerConfig(args)
    val reader = Class.forName(config.readerClass).getDeclaredConstructor().newInstance().asInstanceOf[LineMessageReader]
    reader.init(System.in, ConsoleProducer.getReaderProps(config))
    assertEquals(";", reader.keySeparator)
    assertTrue(reader.parseKey)
    assertTrue(reader.parseHeaders)
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

  @Test
  def testDefaultClientId(): Unit = {
    val config = new ConsoleProducer.ProducerConfig(brokerListValidArgs)
    val producerConfig = new ProducerConfig(ConsoleProducer.producerProps(config))
    assertEquals("console-producer",
      producerConfig.getString(ProducerConfig.CLIENT_ID_CONFIG))
  }

  @Test
  def testBatchSizeOverriddenByMaxPartitionMemoryBytesValue(): Unit = {
    val config = new ConsoleProducer.ProducerConfig(batchSizeOverriddenByMaxPartitionMemoryBytesValue)
    val producerConfig = new ProducerConfig(ConsoleProducer.producerProps(config))
    assertEquals(456,
      producerConfig.getInt(ProducerConfig.BATCH_SIZE_CONFIG))
  }

  @Test
  def testBatchSizeSetAndMaxPartitionMemoryBytesNotSet(): Unit = {
    val config = new ConsoleProducer.ProducerConfig(btchSizeSetAndMaxPartitionMemoryBytesNotSet)
    val producerConfig = new ProducerConfig(ConsoleProducer.producerProps(config))
    assertEquals(123,
      producerConfig.getInt(ProducerConfig.BATCH_SIZE_CONFIG))
  }

  @Test
  def testDefaultBatchSize(): Unit = {
    val config = new ConsoleProducer.ProducerConfig(batchSizeDefault)
    val producerConfig = new ProducerConfig(ConsoleProducer.producerProps(config))
    assertEquals(16*1024,
      producerConfig.getInt(ProducerConfig.BATCH_SIZE_CONFIG))
  }

  @Test
  def testBatchSizeNotSetAndMaxPartitionMemoryBytesSet (): Unit = {
    val config = new ConsoleProducer.ProducerConfig(batchSizeNotSetAndMaxPartitionMemoryBytesSet)
    val producerConfig = new ProducerConfig(ConsoleProducer.producerProps(config))
    assertEquals(456,
      producerConfig.getInt(ProducerConfig.BATCH_SIZE_CONFIG))
  }

}
