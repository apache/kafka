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

import java.io.{ByteArrayOutputStream, Closeable, PrintStream}
import java.nio.charset.StandardCharsets
import java.util

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.internals.{RecordHeader, RecordHeaders}
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.common.serialization.Deserializer
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, MethodSource}

import java.util.Optional
import scala.jdk.CollectionConverters._

class DefaultMessageFormatterTest {
  import DefaultMessageFormatterTest._

  @ParameterizedTest
  @MethodSource(Array("parameters"))
  def testWriteRecord(name: String, record: ConsumerRecord[Array[Byte], Array[Byte]], properties: Map[String, String], expected: String): Unit = {
    withResource(new ByteArrayOutputStream()) { baos =>
      withResource(new PrintStream(baos)) { ps =>
        val formatter = buildFormatter(properties)
        formatter.writeTo(record, ps)
        val actual = new String(baos.toByteArray(), StandardCharsets.UTF_8)
        assertEquals(expected, actual)

      }
    }
  }
}

object DefaultMessageFormatterTest {
  def parameters: java.util.stream.Stream[Arguments] = {
    Seq(
      Arguments.of(
        "print nothing",
        consumerRecord(),
        Map("print.value" -> "false"),
        ""),
      Arguments.of(
        "print key",
        consumerRecord(),
        Map("print.key" -> "true",
          "print.value" -> "false"),
        "someKey\n"),
      Arguments.of(
        "print value",
        consumerRecord(),
        Map(),
        "someValue\n"),
      Arguments.of(
        "print empty timestamp",
        consumerRecord(timestampType = TimestampType.NO_TIMESTAMP_TYPE),
        Map("print.timestamp" -> "true",
            "print.value" -> "false"),
        "NO_TIMESTAMP\n"),
      Arguments.of(
        "print log append time timestamp",
        consumerRecord(timestampType = TimestampType.LOG_APPEND_TIME),
        Map("print.timestamp" -> "true",
            "print.value" -> "false"),
        "LogAppendTime:1234\n"),
      Arguments.of(
        "print create time timestamp",
        consumerRecord(timestampType = TimestampType.CREATE_TIME),
        Map("print.timestamp" -> "true",
            "print.value" -> "false"),
        "CreateTime:1234\n"),
      Arguments.of(
        "print partition",
        consumerRecord(),
        Map("print.partition" -> "true",
            "print.value" -> "false"),
        "Partition:9\n"),
      Arguments.of(
        "print offset",
        consumerRecord(),
        Map("print.offset" -> "true",
            "print.value" -> "false"),
        "Offset:9876\n"),
      Arguments.of(
        "print headers",
        consumerRecord(),
        Map("print.headers" -> "true",
            "print.value" -> "false"),
        "h1:v1,h2:v2\n"),
      Arguments.of(
        "print empty headers",
        consumerRecord(headers = Nil),
        Map("print.headers" -> "true",
            "print.value" -> "false"),
        "NO_HEADERS\n"),
      Arguments.of(
        "print all possible fields with default delimiters",
        consumerRecord(),
        Map("print.key" -> "true",
            "print.timestamp" -> "true",
            "print.partition" -> "true",
            "print.offset" -> "true",
            "print.headers" -> "true",
            "print.value" -> "true"),
        "CreateTime:1234\tPartition:9\tOffset:9876\th1:v1,h2:v2\tsomeKey\tsomeValue\n"),
      Arguments.of(
        "print all possible fields with custom delimiters",
        consumerRecord(),
        Map("key.separator" -> "|",
            "line.separator" -> "^",
            "headers.separator" -> "#",
            "print.key" -> "true",
            "print.timestamp" -> "true",
            "print.partition" -> "true",
            "print.offset" -> "true",
            "print.headers" -> "true",
            "print.value" -> "true"),
        "CreateTime:1234|Partition:9|Offset:9876|h1:v1#h2:v2|someKey|someValue^"),
      Arguments.of(
        "print key with custom deserializer",
        consumerRecord(),
        Map("print.key" -> "true",
            "print.headers" -> "true",
            "print.value" -> "true",
            "key.deserializer" -> "kafka.tools.UpperCaseDeserializer"),
        "h1:v1,h2:v2\tSOMEKEY\tsomeValue\n"),
      Arguments.of(
        "print value with custom deserializer",
        consumerRecord(),
        Map("print.key" -> "true",
            "print.headers" -> "true",
            "print.value" -> "true",
            "value.deserializer" -> "kafka.tools.UpperCaseDeserializer"),
        "h1:v1,h2:v2\tsomeKey\tSOMEVALUE\n"),
      Arguments.of(
        "print headers with custom deserializer",
        consumerRecord(),
        Map("print.key" -> "true",
            "print.headers" -> "true",
            "print.value" -> "true",
            "headers.deserializer" -> "kafka.tools.UpperCaseDeserializer"),
        "h1:V1,h2:V2\tsomeKey\tsomeValue\n"),
      Arguments.of(
        "print key and value",
        consumerRecord(),
        Map("print.key" -> "true",
            "print.value" -> "true"),
        "someKey\tsomeValue\n"),
      Arguments.of(
        "print fields in the beginning, middle and the end",
        consumerRecord(),
        Map("print.key" -> "true",
            "print.value" -> "true",
            "print.partition" -> "true"),
        "Partition:9\tsomeKey\tsomeValue\n"),
      Arguments.of(
        "null value without custom null literal",
        consumerRecord(value = null),
        Map("print.key" -> "true"),
        "someKey\tnull\n"),
      Arguments.of(
        "null value with custom null literal",
        consumerRecord(value = null),
        Map("print.key" -> "true",
            "null.literal" -> "NULL"),
        "someKey\tNULL\n"),
    ).asJava.stream()
  }

  private def buildFormatter(propsToSet: Map[String, String]): DefaultMessageFormatter = {
    val formatter = new DefaultMessageFormatter()
    formatter.configure(propsToSet.asJava)
    formatter
  }


  private def header(key: String, value: String) = {
    new RecordHeader(key, value.getBytes(StandardCharsets.UTF_8))
  }

  private def consumerRecord(key: String = "someKey",
                             value: String = "someValue",
                             headers: Iterable[Header] = Seq(header("h1", "v1"), header("h2", "v2")),
                             partition: Int = 9,
                             offset: Long = 9876,
                             timestamp: Long = 1234,
                             timestampType: TimestampType = TimestampType.CREATE_TIME) = {
    new ConsumerRecord[Array[Byte], Array[Byte]](
      "someTopic",
      partition,
      offset,
      timestamp,
      timestampType,
      0,
      0,
      if (key == null) null else key.getBytes(StandardCharsets.UTF_8),
      if (value == null) null else value.getBytes(StandardCharsets.UTF_8),
      new RecordHeaders(headers.asJava),
      Optional.empty[Integer])
  }

  private def withResource[Resource <: Closeable, Result](resource: Resource)(handler: Resource => Result): Result = {
    try {
      handler(resource)
    } finally {
      resource.close()
    }
  }
}

class UpperCaseDeserializer extends Deserializer[String] {
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}
  override def deserialize(topic: String, data: Array[Byte]): String = new String(data, StandardCharsets.UTF_8).toUpperCase
  override def close(): Unit = {}
}
