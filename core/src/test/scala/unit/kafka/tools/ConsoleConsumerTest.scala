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

import java.io.{ByteArrayOutputStream, PrintStream}
import java.util.{HashMap, Optional, Map => JMap}
import java.time.Duration
import kafka.tools.ConsoleConsumer.ConsumerWrapper
import kafka.utils.{Exit, TestUtils}
import org.apache.kafka.clients.consumer.{ConsumerRecord, MockConsumer, OffsetResetStrategy}
import org.apache.kafka.common.{MessageFormatter, TopicPartition}
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.test.MockDeserializer
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers
import ArgumentMatchers._
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.server.util.MockTime
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{BeforeEach, Test}

import scala.jdk.CollectionConverters._

class ConsoleConsumerTest {

  @BeforeEach
  def setup(): Unit = {
    ConsoleConsumer.messageCount = 0
  }

  @Test
  def shouldThrowTimeoutExceptionWhenTimeoutIsReached(): Unit = {
    val topic = "test"
    val time = new MockTime
    val timeoutMs = 1000

    val mockConsumer = mock(classOf[Consumer[Array[Byte], Array[Byte]]])

    when(mockConsumer.poll(Duration.ofMillis(timeoutMs))).thenAnswer { _ =>
      time.sleep(timeoutMs / 2 + 1)
      ConsumerRecords.EMPTY
    }

    val consumer = new ConsumerWrapper(
      topic = Some(topic),
      partitionId = None,
      offset = None,
      includedTopics = None,
      consumer = mockConsumer,
      timeoutMs = timeoutMs,
      time = time
    )

    assertThrows(classOf[TimeoutException], () => consumer.receive())
  }

  @Test
  def shouldResetUnConsumedOffsetsBeforeExit(): Unit = {
    val topic = "test"
    val maxMessages: Int = 123
    val totalMessages: Int = 700
    val startOffset: java.lang.Long = 0L

    val mockConsumer = new MockConsumer[Array[Byte], Array[Byte]](OffsetResetStrategy.EARLIEST)
    val tp1 = new TopicPartition(topic, 0)
    val tp2 = new TopicPartition(topic, 1)

    val consumer = new ConsumerWrapper(Some(topic), None, None, None, mockConsumer)

    mockConsumer.rebalance(List(tp1, tp2).asJava)
    mockConsumer.updateBeginningOffsets(Map(tp1 -> startOffset, tp2 -> startOffset).asJava)

    0 until totalMessages foreach { i =>
      // add all records, each partition should have half of `totalMessages`
      mockConsumer.addRecord(new ConsumerRecord[Array[Byte], Array[Byte]](topic, i % 2, i / 2, "key".getBytes, "value".getBytes))
    }

    val formatter = mock(classOf[MessageFormatter])

    ConsoleConsumer.process(maxMessages, formatter, consumer, System.out, skipMessageOnError = false)
    assertEquals(totalMessages, mockConsumer.position(tp1) + mockConsumer.position(tp2))

    consumer.resetUnconsumedOffsets()
    assertEquals(maxMessages, mockConsumer.position(tp1) + mockConsumer.position(tp2))

    verify(formatter, times(maxMessages)).writeTo(any(), any())
  }

  @Test
  def shouldLimitReadsToMaxMessageLimit(): Unit = {
    val consumer = mock(classOf[ConsumerWrapper])
    val formatter = mock(classOf[MessageFormatter])
    val record = new ConsumerRecord("foo", 1, 1, Array[Byte](), Array[Byte]())

    val messageLimit: Int = 10
    when(consumer.receive()).thenReturn(record)

    ConsoleConsumer.process(messageLimit, formatter, consumer, System.out, true)

    verify(consumer, times(messageLimit)).receive()
    verify(formatter, times(messageLimit)).writeTo(any(), any())

    consumer.cleanup()
  }

  @Test
  def shouldStopWhenOutputCheckErrorFails(): Unit = {
    val consumer = mock(classOf[ConsumerWrapper])
    val formatter = mock(classOf[MessageFormatter])
    val printStream = mock(classOf[PrintStream])

    val record = new ConsumerRecord("foo", 1, 1, Array[Byte](), Array[Byte]())

    when(consumer.receive()).thenReturn(record)
    //Simulate an error on System.out after the first record has been printed
    when(printStream.checkError()).thenReturn(true)

    ConsoleConsumer.process(-1, formatter, consumer, printStream, true)

    verify(formatter).writeTo(any(), ArgumentMatchers.eq(printStream))
    verify(consumer).receive()
    verify(printStream).checkError()

    consumer.cleanup()
  }

  @Test
  def shouldParseValidConsumerValidConfig(): Unit = {
    //Given
    val args: Array[String] = Array(
      "--bootstrap-server", "localhost:9092",
      "--topic", "test",
      "--from-beginning")

    //When
    val config = new ConsoleConsumer.ConsumerConfig(args)

    //Then
    assertEquals("localhost:9092", config.bootstrapServer)
    assertEquals("test", config.topicArg)
    assertEquals(true, config.fromBeginning)
  }

  @Test
  def shouldParseIncludeArgument(): Unit = {
    //Given
    val args: Array[String] = Array(
      "--bootstrap-server", "localhost:9092",
      "--include", "includeTest*",
      "--from-beginning")

    //When
    val config = new ConsoleConsumer.ConsumerConfig(args)

    //Then
    assertEquals("localhost:9092", config.bootstrapServer)
    assertEquals("includeTest*", config.includedTopicsArg)
    assertEquals(true, config.fromBeginning)
  }

  @Test
  def shouldParseWhitelistArgument(): Unit = {
    //Given
    val args: Array[String] = Array(
      "--bootstrap-server", "localhost:9092",
      "--whitelist", "whitelistTest*",
      "--from-beginning")

    //When
    val config = new ConsoleConsumer.ConsumerConfig(args)

    //Then
    assertEquals("localhost:9092", config.bootstrapServer)
    assertEquals("whitelistTest*", config.includedTopicsArg)
    assertEquals(true, config.fromBeginning)
  }

  @Test
  def shouldIgnoreWhitelistArgumentIfIncludeSpecified(): Unit = {
    //Given
    val args: Array[String] = Array(
      "--bootstrap-server", "localhost:9092",
      "--include", "includeTest*",
      "--whitelist", "whitelistTest*",
      "--from-beginning")

    //When
    val config = new ConsoleConsumer.ConsumerConfig(args)

    //Then
    assertEquals("localhost:9092", config.bootstrapServer)
    assertEquals("includeTest*", config.includedTopicsArg)
    assertEquals(true, config.fromBeginning)
  }

  @Test
  def shouldParseValidSimpleConsumerValidConfigWithNumericOffset(): Unit = {
    //Given
    val args: Array[String] = Array(
      "--bootstrap-server", "localhost:9092",
      "--topic", "test",
      "--partition", "0",
      "--offset", "3")

    //When
    val config = new ConsoleConsumer.ConsumerConfig(args)

    //Then
    assertEquals("localhost:9092", config.bootstrapServer)
    assertEquals("test", config.topicArg)
    assertEquals(0, config.partitionArg.get)
    assertEquals(3, config.offsetArg)
    assertEquals(false, config.fromBeginning)

  }

  @Test
  def shouldExitOnUnrecognizedNewConsumerOption(): Unit = {
    Exit.setExitProcedure((_, message) => throw new IllegalArgumentException(message.orNull))

    //Given
    val args: Array[String] = Array(
      "--new-consumer",
      "--bootstrap-server", "localhost:9092",
      "--topic", "test",
      "--from-beginning")

    try assertThrows(classOf[IllegalArgumentException], () => new ConsoleConsumer.ConsumerConfig(args))
    finally Exit.resetExitProcedure()
  }

  @Test
  def shouldParseValidSimpleConsumerValidConfigWithStringOffset(): Unit = {
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
    assertEquals("localhost:9092", config.bootstrapServer)
    assertEquals("test", config.topicArg)
    assertEquals(0, config.partitionArg.get)
    assertEquals(-1, config.offsetArg)
    assertEquals(false, config.fromBeginning)
    assertEquals(false, config.formatter.asInstanceOf[DefaultMessageFormatter].printValue)
  }

  @Test
  def shouldParseValidConsumerConfigWithAutoOffsetResetLatest(): Unit = {
    //Given
    val args: Array[String] = Array(
      "--bootstrap-server", "localhost:9092",
      "--topic", "test",
      "--consumer-property", "auto.offset.reset=latest")

    //When
    val config = new ConsoleConsumer.ConsumerConfig(args)
    val consumerProperties = ConsoleConsumer.consumerProps(config)

    //Then
    assertEquals("localhost:9092", config.bootstrapServer)
    assertEquals("test", config.topicArg)
    assertEquals(false, config.fromBeginning)
    assertEquals("latest", consumerProperties.getProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG))
  }

  @Test
  def shouldParseValidConsumerConfigWithAutoOffsetResetEarliest(): Unit = {
    //Given
    val args: Array[String] = Array(
      "--bootstrap-server", "localhost:9092",
      "--topic", "test",
      "--consumer-property", "auto.offset.reset=earliest")

    //When
    val config = new ConsoleConsumer.ConsumerConfig(args)
    val consumerProperties = ConsoleConsumer.consumerProps(config)

    //Then
    assertEquals("localhost:9092", config.bootstrapServer)
    assertEquals("test", config.topicArg)
    assertEquals(false, config.fromBeginning)
    assertEquals("earliest", consumerProperties.getProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG))
  }

  @Test
  def shouldParseValidConsumerConfigWithAutoOffsetResetAndMatchingFromBeginning(): Unit = {
    //Given
    val args: Array[String] = Array(
      "--bootstrap-server", "localhost:9092",
      "--topic", "test",
      "--consumer-property", "auto.offset.reset=earliest",
      "--from-beginning")

    //When
    val config = new ConsoleConsumer.ConsumerConfig(args)
    val consumerProperties = ConsoleConsumer.consumerProps(config)

    //Then
    assertEquals("localhost:9092", config.bootstrapServer)
    assertEquals("test", config.topicArg)
    assertEquals(true, config.fromBeginning)
    assertEquals("earliest", consumerProperties.getProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG))
  }

  @Test
  def shouldParseValidConsumerConfigWithNoOffsetReset(): Unit = {
    //Given
    val args: Array[String] = Array(
      "--bootstrap-server", "localhost:9092",
      "--topic", "test")

    //When
    val config = new ConsoleConsumer.ConsumerConfig(args)
    val consumerProperties = ConsoleConsumer.consumerProps(config)

    //Then
    assertEquals("localhost:9092", config.bootstrapServer)
    assertEquals("test", config.topicArg)
    assertEquals(false, config.fromBeginning)
    assertEquals("latest", consumerProperties.getProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG))
  }

  @Test
  def shouldExitOnInvalidConfigWithAutoOffsetResetAndConflictingFromBeginning(): Unit = {
    Exit.setExitProcedure((_, message) => throw new IllegalArgumentException(message.orNull))

    //Given
    val args: Array[String] = Array(
      "--bootstrap-server", "localhost:9092",
      "--topic", "test",
      "--consumer-property", "auto.offset.reset=latest",
      "--from-beginning")
    try {
      val config = new ConsoleConsumer.ConsumerConfig(args)
      assertThrows(classOf[IllegalArgumentException], () => ConsoleConsumer.consumerProps(config))
    }
    finally Exit.resetExitProcedure()
  }

  @Test
  def shouldParseConfigsFromFile(): Unit = {
    val propsFile = TestUtils.tempPropertiesFile(Map("request.timeout.ms" -> "1000", "group.id" -> "group1"))
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
  def groupIdsProvidedInDifferentPlacesMustMatch(): Unit = {
    Exit.setExitProcedure((_, message) => throw new IllegalArgumentException(message.orNull))

    // different in all three places
    var propsFile = TestUtils.tempPropertiesFile(Map("group.id" -> "group-from-file"))
    var args: Array[String] = Array(
      "--bootstrap-server", "localhost:9092",
      "--topic", "test",
      "--group", "group-from-arguments",
      "--consumer-property", "group.id=group-from-properties",
      "--consumer.config", propsFile.getAbsolutePath
    )

    assertThrows(classOf[IllegalArgumentException], () => new ConsoleConsumer.ConsumerConfig(args))

    // the same in all three places
    propsFile = TestUtils.tempPropertiesFile(Map("group.id" -> "test-group"))
    args = Array(
      "--bootstrap-server", "localhost:9092",
      "--topic", "test",
      "--group", "test-group",
      "--consumer-property", "group.id=test-group",
      "--consumer.config", propsFile.getAbsolutePath
    )

    var config = new ConsoleConsumer.ConsumerConfig(args)
    var props = ConsoleConsumer.consumerProps(config)
    assertEquals("test-group", props.getProperty("group.id"))

    // different via --consumer-property and --consumer.config
    propsFile = TestUtils.tempPropertiesFile(Map("group.id" -> "group-from-file"))
    args = Array(
      "--bootstrap-server", "localhost:9092",
      "--topic", "test",
      "--consumer-property", "group.id=group-from-properties",
      "--consumer.config", propsFile.getAbsolutePath
    )

    assertThrows(classOf[IllegalArgumentException], () => new ConsoleConsumer.ConsumerConfig(args))

    // different via --consumer-property and --group
    args = Array(
      "--bootstrap-server", "localhost:9092",
      "--topic", "test",
      "--group", "group-from-arguments",
      "--consumer-property", "group.id=group-from-properties"
    )

    assertThrows(classOf[IllegalArgumentException], () => new ConsoleConsumer.ConsumerConfig(args))

    // different via --group and --consumer.config
    propsFile = TestUtils.tempPropertiesFile(Map("group.id" -> "group-from-file"))
    args = Array(
      "--bootstrap-server", "localhost:9092",
      "--topic", "test",
      "--group", "group-from-arguments",
      "--consumer.config", propsFile.getAbsolutePath
    )
    assertThrows(classOf[IllegalArgumentException], () => new ConsoleConsumer.ConsumerConfig(args))

    // via --group only
    args = Array(
      "--bootstrap-server", "localhost:9092",
      "--topic", "test",
      "--group", "group-from-arguments"
    )

    config = new ConsoleConsumer.ConsumerConfig(args)
    props = ConsoleConsumer.consumerProps(config)
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
    val keyDeserializer = formatter.keyDeserializer.get.asInstanceOf[MockDeserializer]
    assertEquals(1, keyDeserializer.configs.size)
    assertEquals("abc", keyDeserializer.configs.get("my-props"))
    assertTrue(keyDeserializer.isKey)
  }

  @Test
  def testCustomConfigShouldBePassedToConfigureMethod(): Unit = {
    val propsFile = TestUtils.tempPropertiesFile(Map("key.deserializer.my-props" -> "abc", "print.key" -> "false"))
    val args = Array(
      "--bootstrap-server", "localhost:9092",
      "--topic", "test",
      "--property", "print.key=true",
      "--property", "key.deserializer=org.apache.kafka.test.MockDeserializer",
      "--formatter-config", propsFile.getAbsolutePath
    )
    val config = new ConsoleConsumer.ConsumerConfig(args)
    assertTrue(config.formatter.isInstanceOf[DefaultMessageFormatter])
    assertTrue(config.formatterArgs.containsKey("key.deserializer.my-props"))
    val formatter = config.formatter.asInstanceOf[DefaultMessageFormatter]
    assertTrue(formatter.keyDeserializer.get.isInstanceOf[MockDeserializer])
    val keyDeserializer = formatter.keyDeserializer.get.asInstanceOf[MockDeserializer]
    assertEquals(1, keyDeserializer.configs.size)
    assertEquals("abc", keyDeserializer.configs.get("my-props"))
    assertTrue(keyDeserializer.isKey)
  }

  @Test
  def shouldParseGroupIdFromBeginningGivenTogether(): Unit = {
    // Start from earliest
    var args: Array[String] = Array(
      "--bootstrap-server", "localhost:9092",
      "--topic", "test",
      "--group", "test-group",
      "--from-beginning")

    var config = new ConsoleConsumer.ConsumerConfig(args)
    assertEquals("localhost:9092", config.bootstrapServer)
    assertEquals("test", config.topicArg)
    assertEquals(-2, config.offsetArg)
    assertEquals(true, config.fromBeginning)

    // Start from latest
    args = Array(
      "--bootstrap-server", "localhost:9092",
      "--topic", "test",
      "--group", "test-group"
    )

    config = new ConsoleConsumer.ConsumerConfig(args)
    assertEquals("localhost:9092", config.bootstrapServer)
    assertEquals("test", config.topicArg)
    assertEquals(-1, config.offsetArg)
    assertEquals(false, config.fromBeginning)
  }

  @Test
  def shouldExitOnGroupIdAndPartitionGivenTogether(): Unit = {
    Exit.setExitProcedure((_, message) => throw new IllegalArgumentException(message.orNull))
    //Given
    val args: Array[String] = Array(
      "--bootstrap-server", "localhost:9092",
      "--topic", "test",
      "--group", "test-group",
      "--partition", "0")

    try assertThrows(classOf[IllegalArgumentException], () => new ConsoleConsumer.ConsumerConfig(args))
    finally Exit.resetExitProcedure()
  }

  @Test
  def shouldExitOnOffsetWithoutPartition(): Unit = {
    Exit.setExitProcedure((_, message) => throw new IllegalArgumentException(message.orNull))
    //Given
    val args: Array[String] = Array(
      "--bootstrap-server", "localhost:9092",
      "--topic", "test",
      "--offset", "10")

    try assertThrows(classOf[IllegalArgumentException], () => new ConsoleConsumer.ConsumerConfig(args))
    finally Exit.resetExitProcedure()
  }

  @Test
  def testDefaultMessageFormatter(): Unit = {
    val record = new ConsumerRecord("topic", 0, 123, "key".getBytes, "value".getBytes)
    val formatter = new DefaultMessageFormatter()
    val configs: JMap[String, String] = new HashMap()

    formatter.configure(configs)
    var out = new ByteArrayOutputStream()
    formatter.writeTo(record, new PrintStream(out))
    assertEquals("value\n", out.toString)

    configs.put("print.key", "true")
    formatter.configure(configs)
    out = new ByteArrayOutputStream()
    formatter.writeTo(record, new PrintStream(out))
    assertEquals("key\tvalue\n", out.toString)

    configs.put("print.partition", "true")
    formatter.configure(configs)
    out = new ByteArrayOutputStream()
    formatter.writeTo(record, new PrintStream(out))
    assertEquals("Partition:0\tkey\tvalue\n", out.toString)

    configs.put("print.timestamp", "true")
    formatter.configure(configs)
    out = new ByteArrayOutputStream()
    formatter.writeTo(record, new PrintStream(out))
    assertEquals("NO_TIMESTAMP\tPartition:0\tkey\tvalue\n", out.toString)

    configs.put("print.offset", "true")
    formatter.configure(configs)
    out = new ByteArrayOutputStream()
    formatter.writeTo(record, new PrintStream(out))
    assertEquals("NO_TIMESTAMP\tPartition:0\tOffset:123\tkey\tvalue\n", out.toString)

    out = new ByteArrayOutputStream()
    val record2 = new ConsumerRecord("topic", 0, 123, 123L, TimestampType.CREATE_TIME, -1, -1, "key".getBytes, "value".getBytes,
      new RecordHeaders(), Optional.empty[Integer])
    formatter.writeTo(record2, new PrintStream(out))
    assertEquals("CreateTime:123\tPartition:0\tOffset:123\tkey\tvalue\n", out.toString)
    formatter.close()
  }

  @Test
  def testNoOpMessageFormatter(): Unit = {
    val record = new ConsumerRecord("topic", 0, 123, "key".getBytes, "value".getBytes)
    val formatter = new NoOpMessageFormatter()

    formatter.configure(new HashMap())
    val out = new ByteArrayOutputStream()
    formatter.writeTo(record, new PrintStream(out))
    assertEquals("", out.toString)
  }

  @Test
  def shouldExitIfNoTopicOrFilterSpecified(): Unit = {
    Exit.setExitProcedure((_, message) => throw new IllegalArgumentException(message.orNull))
    //Given
    val args: Array[String] = Array(
      "--bootstrap-server", "localhost:9092")

    try assertThrows(classOf[IllegalArgumentException], () => new ConsoleConsumer.ConsumerConfig(args))
    finally Exit.resetExitProcedure()
  }

  @Test
  def shouldExitIfTopicAndIncludeSpecified(): Unit = {
    Exit.setExitProcedure((_, message) => throw new IllegalArgumentException(message.orNull))
    //Given
    val args: Array[String] = Array(
      "--bootstrap-server", "localhost:9092",
      "--topic", "test",
      "--include", "includeTest*")

    try assertThrows(classOf[IllegalArgumentException], () => new ConsoleConsumer.ConsumerConfig(args))
    finally Exit.resetExitProcedure()
  }

  @Test
  def shouldExitIfTopicAndWhitelistSpecified(): Unit = {
    Exit.setExitProcedure((_, message) => throw new IllegalArgumentException(message.orNull))
    //Given
    val args: Array[String] = Array(
      "--bootstrap-server", "localhost:9092",
      "--topic", "test",
      "--whitelist", "whitelistTest*")

    try assertThrows(classOf[IllegalArgumentException], () => new ConsoleConsumer.ConsumerConfig(args))
    finally Exit.resetExitProcedure()
  }

  @Test
  def testClientIdOverride(): Unit = {
    //Given
    val args: Array[String] = Array(
      "--bootstrap-server", "localhost:9092",
      "--topic", "test",
      "--from-beginning",
      "--consumer-property", "client.id=consumer-1")

    //When
    val config = new ConsoleConsumer.ConsumerConfig(args)
    val consumerProperties = ConsoleConsumer.consumerProps(config)

    //Then
    assertEquals("consumer-1", consumerProperties.getProperty(ConsumerConfig.CLIENT_ID_CONFIG))
  }

  @Test
  def testDefaultClientId(): Unit = {
    //Given
    val args: Array[String] = Array(
      "--bootstrap-server", "localhost:9092",
      "--topic", "test",
      "--from-beginning")

    //When
    val config = new ConsoleConsumer.ConsumerConfig(args)
    val consumerProperties = ConsoleConsumer.consumerProps(config)

    //Then
    assertEquals("console-consumer", consumerProperties.getProperty(ConsumerConfig.CLIENT_ID_CONFIG))
  }
}
