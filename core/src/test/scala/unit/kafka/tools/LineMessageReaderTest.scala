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

package unit.kafka.tools

import org.apache.kafka.common.KafkaException
import kafka.tools.ConsoleProducer.LineMessageReader
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.internals.RecordHeader
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows}
import org.junit.jupiter.api.Test

import java.io.ByteArrayInputStream
import java.lang
import java.util.Arrays.asList
import java.util.Properties

class LineMessageReaderTest {

  private def defaultTestProps = {
    val props = new Properties()
    props.put("topic", "topic")
    props.put("parse.key", "true")
    props.put("parse.headers", "true")
    props
  }


  @Test
  def testLineReader(): Unit = {
    val lineReader = new LineMessageReader();
    val input =
      "key0\tvalue0\n" +
        "key1\tvalue1"

    val props = defaultTestProps
    props.put("parse.headers", "false")

    lineReader.init(new ByteArrayInputStream(input.getBytes), props)

    val expected0 = producerRecord("key0", "value0")
    val actual0 = lineReader.readMessage()
    assertEquality(expected0, actual0)

    val expected1 = producerRecord("key1", "value1")
    val actual1 = lineReader.readMessage()
    assertEquality(expected1, actual1)

  }

  @Test
  def testLineReaderHeader(): Unit = {
    val lineReader = new LineMessageReader();
    val input = "headerKey0:headerValue0,headerKey1:headerValue1\tkey0\tvalue0\n"

    lineReader.init(new ByteArrayInputStream(input.getBytes), defaultTestProps)

    val expectedHeaders: lang.Iterable[Header] = asList(
      new RecordHeader("headerKey0", "headerValue0".getBytes()),
      new RecordHeader("headerKey1", "headerValue1".getBytes())
    )
    val expected = producerRecord("key0", "value0", expectedHeaders)
    val actual = lineReader.readMessage()

    assertEquality(expected, actual)

  }

  @Test
  def testLineReaderHeaderNoKey(): Unit = {
    val lineReader = new LineMessageReader();
    val input = "headerKey:headerValue\tvalue\n"

    val props = defaultTestProps
    props.put("parse.key", "false")

    lineReader.init(new ByteArrayInputStream(input.getBytes), props)

    val expectedHeaders: lang.Iterable[Header] = asList(new RecordHeader("headerKey", "headerValue".getBytes()))
    val expected = producerRecord(null, "value", expectedHeaders)
    val actual = lineReader.readMessage()

    assertEquality(expected, actual)
  }

  @Test
  def testLineReaderOnlyValue(): Unit = {
    val lineReader = new LineMessageReader();
    val input = "value\n"

    val props = defaultTestProps
    props.put("parse.key", "false")
    props.put("parse.headers", "false")

    lineReader.init(new ByteArrayInputStream(input.getBytes), props)

    val expected = producerRecord(null, "value", null)
    val actual = lineReader.readMessage()

    assertEquality(expected, actual)
  }

  @Test
  def testParseHeaderEnabledWithCustomDelimiterAndVaryingNumberOfKeyValueHeaderPairs(): Unit = {
    val lineReader = new LineMessageReader();
    val props = defaultTestProps
    props.put("key.separator", "#")
    props.put("parse.headers", "true")
    props.put("headers.delimiter", "!")
    props.put("headers.separator", "&")
    props.put("headers.key.separator", ":")


    val input =
      "headerKey0.0:headerValue0.0&headerKey0.1:headerValue0.1!key0#value0\n" +
        "headerKey1.0:headerValue1.0!key1#value1"

    lineReader.init(new ByteArrayInputStream(input.getBytes), props)

    val headers: lang.Iterable[Header] = asList(
      new RecordHeader("headerKey0.0", "headerValue0.0".getBytes()),
      new RecordHeader("headerKey0.1", "headerValue0.1".getBytes())
    )
    val record0 = new ProducerRecord("topic", null, null, "key0", "value0", headers)
    assertEquality(record0, lineReader.readMessage())

    val headers1: lang.Iterable[Header] = asList(new RecordHeader("headerKey1.0", "headerValue1.0".getBytes()))
    val record1 = new ProducerRecord("topic", null, null, "key1", "value1", headers1)
    assertEquality(record1, lineReader.readMessage())

  }

  @Test
  def testMissingHeaderSeparatorInInput(): Unit = {
    val lineReader = new LineMessageReader();
    val input =
      "headerKey0.0:headerValue0.0,headerKey0.1:headerValue0.1\tkey0\tvalue0\n" +
        "headerKey1.0:headerValue1.0[MISSING-DELIMITER]key1\tvalue1"

    lineReader.init(new ByteArrayInputStream(input.getBytes), defaultTestProps)
    lineReader.readMessage()
    val expectedException = assertThrows(classOf[KafkaException], () => lineReader.readMessage())

    assertEquals(
      "Could not parse line 2, most likely line does not match pattern: headerKey0:headerValue0,...,headerKeyN:headerValueN\tkey\tvalue",
      expectedException.getMessage
    )
  }

  @Test
  def testIgnoreErrorInInput(): Unit = {
    val lineReader = new LineMessageReader();
    val input =
      "headerKey0.0:headerValue0.0,headerKey0.1:headerValue0.1[MISSING-DELIMITER]key0\tvalue0\n" +
        "headerKey1.0:headerValue1.0\tkey1\tvalue1"

    val props = defaultTestProps
    props.put("ignore.error", "true")
    lineReader.init(new ByteArrayInputStream(input.getBytes), props)

    val payLoadOnlyDueToError = new ProducerRecord("topic", null, "headerKey0.0:headerValue0.0,headerKey0.1:headerValue0.1[MISSING-DELIMITER]key0\tvalue0")
    assertEquality(payLoadOnlyDueToError, lineReader.readMessage());

    val headers: lang.Iterable[Header] = asList(new RecordHeader("headerKey1.0", "headerValue1.0".getBytes()))
    val record1 = new ProducerRecord("topic", null, null, "key1", "value1", headers)

    assertEquality(record1, lineReader.readMessage())

  }

  //  The equality method of ProducerRecord compares memory references for the header iterator, this is why this custom equality check is used.
  private def assertEquality[K, V](expected: ProducerRecord[K, V], actual: ProducerRecord[Array[Byte], Array[Byte]]): Unit = {
    assertEquals(expected.key(), if (actual.key() == null) null else new String(actual.key()))
    assertEquals(expected.value(), if (actual.value() == null) null else new String(actual.value()))
    assertEquals(expected.headers().toArray.toList, actual.headers().toArray.toList)
  }

  private def producerRecord[K, V](key: K, value: V, headers: lang.Iterable[Header]): ProducerRecord[K, V] = {
    new ProducerRecord("topic", null, null, key, value, headers)
  }

  private def producerRecord[K, V](key: K, value: V): ProducerRecord[K, V] = {
    new ProducerRecord("topic", key, value)
  }


}
