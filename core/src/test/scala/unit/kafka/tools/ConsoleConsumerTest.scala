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

import java.io.{FileOutputStream, PrintStream}

import kafka.common.MessageFormatter
import kafka.consumer.{BaseConsumer, BaseConsumerRecord, NewShinyConsumer}
import kafka.utils.{Exit, TestUtils}
import org.apache.kafka.clients.consumer.{ConsumerRecord, MockConsumer, OffsetResetStrategy}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.test.MockDeserializer
import org.easymock.EasyMock
import org.junit.Assert._
import org.junit.{Before, Test}

import scala.collection.JavaConverters._

class ConsoleConsumerTest {

  @Before
  def setup(): Unit = {
    ConsoleConsumer.messageCount = 0
  }

  @Test
  def shouldResetUnConsumedOffsetsBeforeExitForNewConsumer() {
    val topic = "test"
    val maxMessages: Int = 123
    val totalMessages: Int = 700
    val startOffset: java.lang.Long = 0L

    val mockConsumer = new MockConsumer[Array[Byte], Array[Byte]](OffsetResetStrategy.EARLIEST)
    val tp1 = new TopicPartition(topic, 0)
    val tp2 = new TopicPartition(topic, 1)

    val consumer = new NewShinyConsumer(Some(topic), None, None, None, mockConsumer)

    mockConsumer.rebalance(List(tp1, tp2).asJava)
    mockConsumer.updateBeginningOffsets(Map(tp1 -> startOffset, tp2 -> startOffset).asJava)

    0 until totalMessages foreach { i =>
      // add all records, each partition should have half of `totalMessages`
      mockConsumer.addRecord(new ConsumerRecord[Array[Byte], Array[Byte]](topic, i % 2, i / 2, "key".getBytes, "value".getBytes))
    }

    // Mocks
    val formatter = EasyMock.createNiceMock(classOf[MessageFormatter])

    // Expectations
    EasyMock.expect(formatter.writeTo(EasyMock.anyObject(), EasyMock.anyObject())).times(maxMessages)
    EasyMock.replay(formatter)

    // Test
    ConsoleConsumer.process(maxMessages, formatter, consumer, System.out, skipMessageOnError = false)
    assertEquals(totalMessages, mockConsumer.position(tp1) + mockConsumer.position(tp2))

    consumer.resetUnconsumedOffsets()
    assertEquals(maxMessages, mockConsumer.position(tp1) + mockConsumer.position(tp2))

    EasyMock.verify(formatter)
  }

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
      "--from-beginning")

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
      "--offset", "3")

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

  @Test(expected = classOf[IllegalArgumentException])
  def shouldExitOnUnrecognizedNewConsumerOption(): Unit = {
    Exit.setExitProcedure((_, message) => throw new IllegalArgumentException(message.orNull))

    //Given
    val args: Array[String] = Array(
      "--new-consumer",
      "--bootstrap-server", "localhost:9092",
      "--topic", "test",
      "--from-beginning")

    //When
    try {
      new ConsoleConsumer.ConsumerConfig(args)
    } finally {
      Exit.resetExitProcedure()
    }
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
  def testNewConsumerRemovedOption() {
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
      "--property", "print.value=false")

    //When
    val config = new ConsoleConsumer.ConsumerConfig(args)

    //Then
    assertFalse(config.useOldConsumer)
    assertEquals("localhost:9092", config.bootstrapServer)
    assertEquals("test", config.topicArg)
    assertEquals(0, config.partitionArg.get)
    assertEquals(-1, config.offsetArg)
    assertEquals(false, config.fromBeginning)
    assertEquals(false, config.formatter.asInstanceOf[DefaultMessageFormatter].printValue)
  }

  @Test
  def shouldParseValidOldConsumerConfigWithAutoOffsetResetSmallest() {
    //Given
    val args: Array[String] = Array(
      "--zookeeper", "localhost:2181",
      "--topic", "test",
      "--consumer-property", "auto.offset.reset=smallest")

    //When
    val config = new ConsoleConsumer.ConsumerConfig(args)
    val consumerProperties = ConsoleConsumer.getOldConsumerProps(config)

    //Then
    assertTrue(config.useOldConsumer)
    assertEquals("localhost:2181", config.zkConnectionStr)
    assertEquals("test", config.topicArg)
    assertEquals(false, config.fromBeginning)
    assertEquals("smallest", consumerProperties.getProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG))
  }

  @Test
  def shouldParseValidOldConsumerConfigWithAutoOffsetResetLargest() {
    //Given
    val args: Array[String] = Array(
      "--zookeeper", "localhost:2181",
      "--topic", "test",
      "--consumer-property", "auto.offset.reset=largest")

    //When
    val config = new ConsoleConsumer.ConsumerConfig(args)
    val consumerProperties = ConsoleConsumer.getOldConsumerProps(config)

    //Then
    assertTrue(config.useOldConsumer)
    assertEquals("localhost:2181", config.zkConnectionStr)
    assertEquals("test", config.topicArg)
    assertEquals(false, config.fromBeginning)
    assertEquals("largest", consumerProperties.getProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG))
  }

  @Test
  def shouldSetAutoResetToSmallestWhenFromBeginningConfigured() {
    //Given
    val args = Array(
      "--zookeeper", "localhost:2181",
      "--topic", "test",
      "--from-beginning")

    //When
    val config = new ConsoleConsumer.ConsumerConfig(args)
    val consumerProperties = ConsoleConsumer.getOldConsumerProps(config)

    //Then
    assertTrue(config.useOldConsumer)
    assertEquals("localhost:2181", config.zkConnectionStr)
    assertEquals("test", config.topicArg)
    assertEquals(true, config.fromBeginning)
    assertEquals("smallest", consumerProperties.getProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG))
  }

  @Test
  def shouldParseValidNewConsumerConfigWithAutoOffsetResetLatest() {
    //Given
    val args: Array[String] = Array(
      "--bootstrap-server", "localhost:9092",
      "--topic", "test",
      "--consumer-property", "auto.offset.reset=latest")

    //When
    val config = new ConsoleConsumer.ConsumerConfig(args)
    val consumerProperties = ConsoleConsumer.getNewConsumerProps(config)

    //Then
    assertFalse(config.useOldConsumer)
    assertEquals("localhost:9092", config.bootstrapServer)
    assertEquals("test", config.topicArg)
    assertEquals(false, config.fromBeginning)
    assertEquals("latest", consumerProperties.getProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG))
  }

  @Test
  def shouldParseValidNewConsumerConfigWithAutoOffsetResetEarliest() {
    //Given
    val args: Array[String] = Array(
      "--bootstrap-server", "localhost:9092",
      "--topic", "test",
      "--consumer-property", "auto.offset.reset=earliest")

    //When
    val config = new ConsoleConsumer.ConsumerConfig(args)
    val consumerProperties = ConsoleConsumer.getNewConsumerProps(config)

    //Then
    assertFalse(config.useOldConsumer)
    assertEquals("localhost:9092", config.bootstrapServer)
    assertEquals("test", config.topicArg)
    assertEquals(false, config.fromBeginning)
    assertEquals("earliest", consumerProperties.getProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG))
  }

  @Test
  def shouldParseValidNewConsumerConfigWithAutoOffsetResetAndMatchingFromBeginning() {
    //Given
    val args: Array[String] = Array(
      "--bootstrap-server", "localhost:9092",
      "--topic", "test",
      "--consumer-property", "auto.offset.reset=earliest",
      "--from-beginning")

    //When
    val config = new ConsoleConsumer.ConsumerConfig(args)
    val consumerProperties = ConsoleConsumer.getNewConsumerProps(config)

    //Then
    assertFalse(config.useOldConsumer)
    assertEquals("localhost:9092", config.bootstrapServer)
    assertEquals("test", config.topicArg)
    assertEquals(true, config.fromBeginning)
    assertEquals("earliest", consumerProperties.getProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG))
  }

  @Test
  def shouldParseValidNewConsumerConfigWithNoOffsetReset() {
    //Given
    val args: Array[String] = Array(
      "--bootstrap-server", "localhost:9092",
      "--topic", "test")

    //When
    val config = new ConsoleConsumer.ConsumerConfig(args)
    val consumerProperties = ConsoleConsumer.getNewConsumerProps(config)

    //Then
    assertFalse(config.useOldConsumer)
    assertEquals("localhost:9092", config.bootstrapServer)
    assertEquals("test", config.topicArg)
    assertEquals(false, config.fromBeginning)
    assertEquals("latest", consumerProperties.getProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG))
  }

  @Test(expected = classOf[IllegalArgumentException])
  def shouldExitOnInvalidConfigWithAutoOffsetResetAndConflictingFromBeginningNewConsumer() {
    Exit.setExitProcedure((_, message) => throw new IllegalArgumentException(message.orNull))

    //Given
    val args: Array[String] = Array(
      "--bootstrap-server", "localhost:9092",
      "--topic", "test",
      "--consumer-property", "auto.offset.reset=latest",
      "--from-beginning")

    try {
      val config = new ConsoleConsumer.ConsumerConfig(args)
      ConsoleConsumer.getNewConsumerProps(config)
    } finally {
      Exit.resetExitProcedure()
    }
  }

  @Test(expected = classOf[IllegalArgumentException])
  def shouldExitOnInvalidConfigWithAutoOffsetResetAndConflictingFromBeginningOldConsumer() {
    Exit.setExitProcedure((_, message) => throw new IllegalArgumentException(message.orNull))

    //Given
    val args: Array[String] = Array(
      "--zookeeper", "localhost:2181",
      "--topic", "test",
      "--consumer-property", "auto.offset.reset=largest",
      "--from-beginning")

    try {
      val config = new ConsoleConsumer.ConsumerConfig(args)
      ConsoleConsumer.getOldConsumerProps(config)
    } finally {
      Exit.resetExitProcedure()
    }
  }

  @Test
  def shouldParseConfigsFromFile() {
    val propsFile = TestUtils.tempFile()
    val propsStream = new FileOutputStream(propsFile)
    propsStream.write("request.timeout.ms=1000\n".getBytes())
    propsStream.write("group.id=group1".getBytes())
    propsStream.close()
    val args: Array[String] = Array(
      "--bootstrap-server", "localhost:9092",
      "--topic", "test",
      "--consumer.config", propsFile.getAbsolutePath
    )

    val config = new ConsoleConsumer.ConsumerConfig(args)

    assertEquals("1000", config.consumerProps.getProperty("request.timeout.ms"))
    assertEquals("group1", config.consumerProps.getProperty("group.id"))
  }

  @Test
  def groupIdsProvidedInDifferentPlacesMustMatch() {
    Exit.setExitProcedure((_, message) => throw new IllegalArgumentException(message.orNull))

    // different in all three places
    var propsFile = TestUtils.tempFile()
    var propsStream = new FileOutputStream(propsFile)
    propsStream.write("group.id=group-from-file".getBytes())
    propsStream.close()
    var args: Array[String] = Array(
      "--bootstrap-server", "localhost:9092",
      "--topic", "test",
      "--group", "group-from-arguments",
      "--consumer-property", "group.id=group-from-properties",
      "--consumer.config", propsFile.getAbsolutePath
    )

    try {
      new ConsoleConsumer.ConsumerConfig(args)
      fail("Expected groups ids provided in different places to match")
    } catch {
      case e: IllegalArgumentException => //OK
    }

    // the same in all three places
    propsFile = TestUtils.tempFile()
    propsStream = new FileOutputStream(propsFile)
    propsStream.write("group.id=test-group".getBytes())
    propsStream.close()
    args = Array(
      "--bootstrap-server", "localhost:9092",
      "--topic", "test",
      "--group", "test-group",
      "--consumer-property", "group.id=test-group",
      "--consumer.config", propsFile.getAbsolutePath
    )

    var config = new ConsoleConsumer.ConsumerConfig(args)
    var props = ConsoleConsumer.getNewConsumerProps(config)
    assertEquals("test-group", props.getProperty("group.id"))

    // different via --consumer-property and --consumer.config
    propsFile = TestUtils.tempFile()
    propsStream = new FileOutputStream(propsFile)
    propsStream.write("group.id=group-from-file".getBytes())
    propsStream.close()
    args = Array(
      "--bootstrap-server", "localhost:9092",
      "--topic", "test",
      "--consumer-property", "group.id=group-from-properties",
      "--consumer.config", propsFile.getAbsolutePath
    )

    try {
      new ConsoleConsumer.ConsumerConfig(args)
      fail("Expected groups ids provided in different places to match")
    } catch {
      case e: IllegalArgumentException => //OK
    }

    // different via --consumer-property and --group
    args = Array(
      "--bootstrap-server", "localhost:9092",
      "--topic", "test",
      "--group", "group-from-arguments",
      "--consumer-property", "group.id=group-from-properties"
    )

    try {
      new ConsoleConsumer.ConsumerConfig(args)
      fail("Expected groups ids provided in different places to match")
    } catch {
      case e: IllegalArgumentException => //OK
    }

    // different via --group and --consumer.config
    propsFile = TestUtils.tempFile()
    propsStream = new FileOutputStream(propsFile)
    propsStream.write("group.id=group-from-file".getBytes())
    propsStream.close()
    args = Array(
      "--bootstrap-server", "localhost:9092",
      "--topic", "test",
      "--group", "group-from-arguments",
      "--consumer.config", propsFile.getAbsolutePath
    )

    try {
      new ConsoleConsumer.ConsumerConfig(args)
      fail("Expected groups ids provided in different places to match")
    } catch {
      case e: IllegalArgumentException => //OK
    }

    // via --group only
    args = Array(
      "--bootstrap-server", "localhost:9092",
      "--topic", "test",
      "--group", "group-from-arguments"
    )

    config = new ConsoleConsumer.ConsumerConfig(args)
    props = ConsoleConsumer.getNewConsumerProps(config)
    assertEquals("group-from-arguments", props.getProperty("group.id"))

    Exit.resetExitProcedure()
  }

  @Test
  def testCustomPropertyShouldBePassedToConfigureMethod(): Unit = {
    val args = Array(
      "--bootstrap-server", "localhost:9092",
      "--topic", "test",
      "--property", "print.key=true",
      "--property", "key.deserializer=org.apache.kafka.test.MockDeserializer",
      "--property", "key.deserializer.my-props=abc"
    )
    val config = new ConsoleConsumer.ConsumerConfig(args)
    assertTrue(config.formatter.isInstanceOf[DefaultMessageFormatter])
    assertTrue(config.formatterArgs.containsKey("key.deserializer.my-props"))
    val formatter = config.formatter.asInstanceOf[DefaultMessageFormatter]
    assertTrue(formatter.keyDeserializer.get.isInstanceOf[MockDeserializer])
    assertEquals(1, formatter.keyDeserializer.get.asInstanceOf[MockDeserializer].configs.size)
    assertEquals("abc", formatter.keyDeserializer.get.asInstanceOf[MockDeserializer].configs.get("my-props"))
    assertTrue(formatter.keyDeserializer.get.asInstanceOf[MockDeserializer].isKey)
  }

}
