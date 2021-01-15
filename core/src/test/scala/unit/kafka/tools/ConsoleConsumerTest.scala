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
import java.nio.file.Files
import java.util.{HashMap, Map => JMap}

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
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{BeforeEach, Test}

import scala.jdk.CollectionConverters._

class ConsoleConsumerTest {

  @BeforeEach
  def setup(): Unit = {
    ConsoleConsumer.messageCount = 0
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
    val propsFile = TestUtils.tempFile()
    val propsStream = Files.newOutputStream(propsFile.toPath)
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
  def groupIdsProvidedInDifferentPlacesMustMatch(): Unit = {
    Exit.setExitProcedure((_, message) => throw new IllegalArgumentException(message.orNull))

    // different in all three places
    var propsFile = TestUtils.tempFile()
    var propsStream = Files.newOutputStream(propsFile.toPath)
    propsStream.write("group.id=group-from-file".getBytes())
    propsStream.close()
    var args: Array[String] = Array(
      "--bootstrap-server", "localhost:9092",
      "--topic", "test",
      "--group", "group-from-arguments",
      "--consumer-property", "group.id=group-from-properties",
      "--consumer.config", propsFile.getAbsolutePath
    )

    assertThrows(classOf[IllegalArgumentException], () => new ConsoleConsumer.ConsumerConfig(args))

    // the same in all three places
    propsFile = TestUtils.tempFile()
    propsStream = Files.newOutputStream(propsFile.toPath)
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
    var props = ConsoleConsumer.consumerProps(config)
    assertEquals("test-group", props.getProperty("group.id"))

    // different via --consumer-property and --consumer.config
    propsFile = TestUtils.tempFile()
    propsStream = Files.newOutputStream(propsFile.toPath)
    propsStream.write("group.id=group-from-file".getBytes())
    propsStream.close()
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
    propsFile = TestUtils.tempFile()
    propsStream = Files.newOutputStream(propsFile.toPath)
    propsStream.write("group.id=group-from-file".getBytes())
    propsStream.close()
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
    assertEquals(1, formatter.keyDeserializer.get.asInstanceOf[MockDeserializer].configs.size)
    assertEquals("abc", formatter.keyDeserializer.get.asInstanceOf[MockDeserializer].configs.get("my-props"))
    assertTrue(formatter.keyDeserializer.get.asInstanceOf[MockDeserializer].isKey)
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
    val record2 = new ConsumerRecord("topic", 0, 123, 123L, TimestampType.CREATE_TIME, 321L, -1, -1, "key".getBytes, "value".getBytes)
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
  def testChecksumMessageFormatter(): Unit = {
    val record = new ConsumerRecord("topic", 0, 123, "key".getBytes, "value".getBytes)
    val formatter = new ChecksumMessageFormatter()
    val configs: JMap[String, String] = new HashMap()

    formatter.configure(configs)
    var out = new ByteArrayOutputStream()
    formatter.writeTo(record, new PrintStream(out))
    assertEquals("checksum:-1\n", out.toString)

    configs.put("topic", "topic1")
    formatter.configure(configs)
    out = new ByteArrayOutputStream()
    formatter.writeTo(record, new PrintStream(out))
    assertEquals("topic1:checksum:-1\n", out.toString)
  }

}
