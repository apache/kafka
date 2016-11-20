/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.tools

import java.io.{PrintStream, FileOutputStream}

import kafka.common.MessageFormatter
import kafka.consumer.{BaseConsumer, BaseConsumerRecord}
import kafka.utils.TestUtils
import org.easymock.EasyMock
import org.junit.Assert._
import org.junit.Test
import org.scalatest.junit.JUnitSuite

class ConsoleConsumerTest extends JUnitSuite {

  @Test
  def shouldLimitReadsToMaxMessageLimit() {
    //Mocks
    val consumer = EasyMock.createNiceMock(classOf[BaseConsumer])
    val formatter = EasyMock.createNiceMock(classOf[MessageFormatter])

    //Stubs
    val record = new BaseConsumerRecord(topic = "foo", partition = 1, offset = 1, key = Array[Byte](), value = Array[Byte]())

    //Expectations
    val messageLimit: Int = 10
    EasyMock.expect(formatter.writeTo(EasyMock.anyObject(), EasyMock.anyObject())).times(messageLimit)
    EasyMock.expect(consumer.receive()).andReturn(record).times(messageLimit)

    EasyMock.replay(consumer)
    EasyMock.replay(formatter)

    //Test
    ConsoleConsumer.process(messageLimit, formatter, consumer, System.out, true)
  }

  @Test
  def shouldStopWhenOutputCheckErrorFails() {
    //Mocks
    val consumer = EasyMock.createNiceMock(classOf[BaseConsumer])
    val formatter = EasyMock.createNiceMock(classOf[MessageFormatter])
    val printStream = EasyMock.createNiceMock(classOf[PrintStream])

    //Stubs
    val record = new BaseConsumerRecord(topic = "foo", partition = 1, offset = 1, key = Array[Byte](), value = Array[Byte]())

    //Expectations
    EasyMock.expect(consumer.receive()).andReturn(record)
    EasyMock.expect(formatter.writeTo(EasyMock.anyObject(), EasyMock.eq(printStream)))
    //Simulate an error on System.out after the first record has been printed
    EasyMock.expect(printStream.checkError()).andReturn(true)

    EasyMock.replay(consumer)
    EasyMock.replay(formatter)
    EasyMock.replay(printStream)

    //Test
    ConsoleConsumer.process(-1, formatter, consumer, printStream, true)

    //Verify
    EasyMock.verify(consumer, formatter, printStream)
  }

  @Test
  def shouldParseValidOldConsumerValidConfig() {
    //Given
    val args: Array[String] = Array(
      "--zookeeper", "localhost:2181",
      "--topic", "test",
      "--from-beginning")

    //When
    val config = new ConsoleConsumer.ConsumerConfig(args)

    //Then
    assertTrue(config.useOldConsumer)
    assertEquals("localhost:2181", config.zkConnectionStr)
    assertEquals("test", config.topicArg)
    assertEquals(true, config.fromBeginning)
  }

  @Test
  def shouldParseValidNewConsumerValidConfig() {
    //Given
    val args: Array[String] = Array(
      "--bootstrap-server", "localhost:9092",
      "--topic", "test",
      "--from-beginning",
      "--new-consumer") //new

    //When
    val config = new ConsoleConsumer.ConsumerConfig(args)

    //Then
    assertFalse(config.useOldConsumer)
    assertEquals("localhost:9092", config.bootstrapServer)
    assertEquals("test", config.topicArg)
    assertEquals(true, config.fromBeginning)
  }

  @Test
  def shouldParseValidNewSimpleConsumerValidConfigWithNumericOffset(): Unit = {
    //Given
    val args: Array[String] = Array(
      "--bootstrap-server", "localhost:9092",
      "--topic", "test",
      "--partition", "0",
      "--offset", "3",
      "--new-consumer") //new

    //When
    val config = new ConsoleConsumer.ConsumerConfig(args)

    //Then
    assertFalse(config.useOldConsumer)
    assertEquals("localhost:9092", config.bootstrapServer)
    assertEquals("test", config.topicArg)
    assertEquals(0, config.partitionArg.get)
    assertEquals(3, config.offsetArg)
    assertEquals(false, config.fromBeginning)

  }

  @Test
  def testDefaultConsumer() {
    //Given
    val args: Array[String] = Array(
      "--bootstrap-server", "localhost:9092",
      "--topic", "test",
      "--from-beginning")

    //When
    val config = new ConsoleConsumer.ConsumerConfig(args)

    //Then
    assertFalse(config.useOldConsumer)
  }

  @Test
  def shouldParseValidNewSimpleConsumerValidConfigWithStringOffset() {
    //Given
    val args: Array[String] = Array(
      "--bootstrap-server", "localhost:9092",
      "--topic", "test",
      "--partition", "0",
      "--offset", "LatEst",
      "--new-consumer") //new

    //When
    val config = new ConsoleConsumer.ConsumerConfig(args)

    //Then
    assertFalse(config.useOldConsumer)
    assertEquals("localhost:9092", config.bootstrapServer)
    assertEquals("test", config.topicArg)
    assertEquals(0, config.partitionArg.get)
    assertEquals(-1, config.offsetArg)
    assertEquals(false, config.fromBeginning)
  }

  @Test
  def shouldParseConfigsFromFile() {
    val propsFile = TestUtils.tempFile()
    val propsStream = new FileOutputStream(propsFile)
    propsStream.write("request.timeout.ms=1000".getBytes())
    propsStream.close()
    val args: Array[String] = Array(
      "--bootstrap-server", "localhost:9092",
      "--topic", "test",
      "--consumer.config", propsFile.getAbsolutePath
    )

    val config = new ConsoleConsumer.ConsumerConfig(args)

    assertEquals("1000", config.consumerProps.getProperty("request.timeout.ms"))
  }
}
