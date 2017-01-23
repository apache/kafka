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

import kafka.producer.ProducerConfig
import ConsoleProducer.LineMessageReader
import kafka.utils.CommandLineUtils
import kafka.utils.CommandLineUtils.ExitPolicy
import org.apache.kafka.clients.producer.KafkaProducer
import org.junit.Assert.{assertEquals, assertFalse, assertTrue}
import org.junit.{Test}

class ConsoleProducerTest {

  @Test
  def shouldParseValidOldProducerConfig() {
    //Given
    val args: Array[String] = Array(
      "--broker-list", "localhost:1001,localhost:1002",
      "--topic", "test",
      "--old-producer")

    //When
    val config = new ConsoleProducer.ProducerConfig(args)

    //Then
    assertTrue(config.useOldProducer)
    assertEquals("localhost:1001,localhost:1002", config.bootstrapServer)
    assertEquals("test", config.topic)
  }

  @Test
  def shouldParseValidNewProducerConfig() {
    //Given
    val args: Array[String] = Array(
      "--bootstrap-server", "localhost:1001",
      "--topic", "test")

    //When
    val config = new ConsoleProducer.ProducerConfig(args)

    //Then
    assertFalse(config.useOldProducer)
    assertEquals("localhost:1001", config.bootstrapServer)
    assertEquals("test", config.topic)
  }

  @Test
  def testValidConfigsNewProducer() {
    val args: Array[String] = Array(
      "--bootstrap-server",
      "localhost:1001,localhost:1002",
      "--topic",
      "t3",
      "--property",
      "parse.key=true",
      "--property",
      "key.separator=#"
    )

    val config = new ConsoleProducer.ProducerConfig(args)
    // New ProducerConfig constructor is package private, so we can't call it directly
    // Creating new Producer to validate instead
    val producer = new KafkaProducer(ConsoleProducer.getNewProducerProps(config))
    producer.close()
  }

  @Test
  @deprecated("This test has been deprecated and it will be removed in a future release.", "0.10.0.0")
  def testValidConfigsOldProducer() {
    val args: Array[String] = Array(
      "--broker-list",
      "localhost:1001,localhost:1002",
      "--topic",
      "t3",
      "--property",
      "parse.key=true",
      "--property",
      "key.separator=#",
      "--old-producer"
    )

    val config = new ConsoleProducer.ProducerConfig(args)
    new ProducerConfig(ConsoleProducer.getOldProducerProps(config))
  }

  @Test(expected = classOf[joptsimple.OptionException])
  def testInvalidConfigs() {
    val invalidArgs: Array[String] = Array(
      "--t", // not a valid argument
      "t3"
    )
    new ConsoleProducer.ProducerConfig(invalidArgs)
  }

  @Test
  def testParseKeyProp(): Unit = {
    val args: Array[String] = Array(
      "--bootstrap-server",
      "localhost:1001,localhost:1002",
      "--topic",
      "t3",
      "--property",
      "parse.key=true",
      "--property",
      "key.separator=#"
    )

    val config = new ConsoleProducer.ProducerConfig(args)
    val reader = Class.forName(config.readerClass).newInstance().asInstanceOf[LineMessageReader]
    reader.init(System.in,ConsoleProducer.getReaderProps(config))
    assert(reader.keySeparator == "#")
    assert(reader.parseKey)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testBrokerListAndBootstrapServerOptionMissing(): Unit = {
    val args: Array[String] = Array(
      "--topic",
      "producerTest"
    )

    CommandLineUtils.exitPolicy(new ExitPolicy {
      override def exit(msg: String): Nothing = {
        throw new IllegalArgumentException
      }
    })
    new ConsoleProducer.ProducerConfig(args)
  }

}
