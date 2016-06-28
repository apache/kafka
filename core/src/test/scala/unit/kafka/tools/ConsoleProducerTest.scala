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
import org.apache.kafka.clients.producer.KafkaProducer
import org.junit.{Assert, Test}

class ConsoleProducerTest {

  val validArgs: Array[String] = Array(
    "--broker-list",
    "localhost:1001,localhost:1002",
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

  @Test
  def testValidConfigsNewProducer() {
    val config = new ConsoleProducer.ProducerConfig(validArgs)
    // New ProducerConfig constructor is package private, so we can't call it directly
    // Creating new Producer to validate instead
    val producer = new KafkaProducer(ConsoleProducer.getNewProducerProps(config))
    producer.close()
  }

  @Test
  @deprecated("This test has been deprecated and it will be removed in a future release.", "0.10.0.0")
  def testValidConfigsOldProducer() {
    val config = new ConsoleProducer.ProducerConfig(validArgs)
    new ProducerConfig(ConsoleProducer.getOldProducerProps(config))
  }

  @Test
  def testInvalidConfigs() {
    try {
      new ConsoleProducer.ProducerConfig(invalidArgs)
      Assert.fail("Should have thrown an UnrecognizedOptionException")
    } catch {
      case e: joptsimple.OptionException => // expected exception
    }
  }

  @Test
  def testParseKeyProp(): Unit = {
    val config = new ConsoleProducer.ProducerConfig(validArgs)
    val reader = Class.forName(config.readerClass).newInstance().asInstanceOf[LineMessageReader]
    reader.init(System.in,ConsoleProducer.getReaderProps(config))
    assert(reader.keySeparator == "#")
    assert(reader.parseKey)
  }

}
