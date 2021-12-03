/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License") you may not use this file except in compliance with
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

import kafka.tools.ConsoleProducer.LineMessageReader
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.KafkaException
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
    val input =
    "key0\tvalue0\n" +
    "key1\tvalue1"

    val props = defaultTestProps
    props.put("parse.headers", "false")

    runTest(props, input, record("key0", "value0"), record("key1", "value1"))
  }

  @Test
  def testLineReaderHeader(): Unit = {

    val input = "headerKey0:headerValue0,headerKey1:headerValue1\tkey0\tvalue0\n"

    val expectedHeaders: lang.Iterable[Header] = asList(
      new RecordHeader("headerKey0", "headerValue0".getBytes()),
      new RecordHeader("headerKey1", "headerValue1".getBytes())
    )

    val expected = record("key0", "value0", expectedHeaders)

    runTest(defaultTestProps, input, expected)
  }

  @Test
  def testLineReaderHeaderNoKey(): Unit = {
    val input = "headerKey:headerValue\tvalue\n"

    val props = defaultTestProps
    props.put("parse.key", "false")

    val expectedHeaders: lang.Iterable[Header] = asList(new RecordHeader("headerKey", "headerValue".getBytes()))
    runTest(defaultTestProps, input, record(null, "value", expectedHeaders))
  }

  @Test
  def testLineReaderOnlyValue(): Unit = {
    val props = defaultTestProps
    props.put("parse.key", "false")
    props.put("parse.headers", "false")

    runTest(props, "value\n", record(null, "value"))
  }

  @Test
  def testParseHeaderEnabledWithCustomDelimiterAndVaryingNumberOfKeyValueHeaderPairs(): Unit = {
    val props = defaultTestProps
    props.put("key.separator", "#")
    props.put("parse.headers", "true")
    props.put("headers.delimiter", "!")
    props.put("headers.separator", "&")
    props.put("headers.key.separator", ":")

    val input =
      "headerKey0.0:headerValue0.0&headerKey0.1:headerValue0.1!key0#value0\n" +
        "headerKey1.0:headerValue1.0!key1#value1"

    val headers: lang.Iterable[Header] = asList(
      new RecordHeader("headerKey0.0", "headerValue0.0".getBytes()),
      new RecordHeader("headerKey0.1", "headerValue0.1".getBytes())
    )

    val record0 = new ProducerRecord("topic", null, null, "key0", "value0", headers)

    val headers1: lang.Iterable[Header] = asList(new RecordHeader("headerKey1.0", "headerValue1.0".getBytes()))
    val record1 = new ProducerRecord("topic", null, null, "key1", "value1", headers1)

    runTest(props, input, record0, record1)

  }

  @Test
  def testMissingHeaderSeparator(): Unit = {
    val lineReader = new LineMessageReader()
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
    val input =
      "headerKey0.0:headerValue0.0,headerKey0.1:headerValue0.1[MISSING-DELIMITER]key0\tvalue0\n" +
        "headerKey1.0:headerValue1.0\tkey1\tvalue1"

    val props = defaultTestProps
    props.put("ignore.error", "true")

    val payLoadOnlyDueToError: ProducerRecord[String, String] =
      new ProducerRecord(
        "topic",
        null,
        "headerKey0.0:headerValue0.0,headerKey0.1:headerValue0.1[MISSING-DELIMITER]key0\tvalue0"
      )

    val headers: lang.Iterable[Header] = asList(new RecordHeader("headerKey1.0", "headerValue1.0".getBytes()))
    val validRecord = new ProducerRecord("topic", null, null, "key1", "value1", headers)

    runTest(props, input, payLoadOnlyDueToError, validRecord)
  }

  @Test
  def testMalformedHeader(): Unit = {
    val lineReader = new LineMessageReader()
    val input =
      "headerKey0.0:,\tkey0\tvalue0\n"

    lineReader.init(new ByteArrayInputStream(input.getBytes), defaultTestProps)
    val expectedException = assertThrows(classOf[KafkaException], () => lineReader.readMessage())

    assertEquals(
      "Could not parse line 1, most likely line does not match pattern: headerKey0:headerValue0,...,headerKeyN:headerValueN\tkey\tvalue",
      expectedException.getMessage
    )
  }

  def runTest(props: Properties, input: String, expectedRecords: ProducerRecord[String, String]*): Unit = {
    val lineReader = new LineMessageReader()
    lineReader.init(new ByteArrayInputStream(input.getBytes), props)
    expectedRecords.foreach(r => assertEquality(r, lineReader.readMessage()))
  }

  //  The equality method of ProducerRecord compares memory references for the header iterator, this is why this custom equality check is used.
  private def assertEquality[K, V](expected: ProducerRecord[K, V], actual: ProducerRecord[Array[Byte], Array[Byte]]): Unit = {
    assertEquals(expected.key(), if (actual.key() == null) null else new String(actual.key()))
    assertEquals(expected.value(), if (actual.value() == null) null else new String(actual.value()))
    assertEquals(expected.headers().toArray.toList, actual.headers().toArray.toList)
  }

  private def record[K, V](key: K, value: V, headers: lang.Iterable[Header]): ProducerRecord[K, V] = {
    new ProducerRecord("topic", null, null, key, value, headers)
  }

  private def record[K, V](key: K, value: V): ProducerRecord[K, V] = {
    new ProducerRecord("topic", key, value)
  }


}
