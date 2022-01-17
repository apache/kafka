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

package kafka.tools

import kafka.tools.ConsoleProducer.LineMessageReader
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.KafkaException
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows}
import org.junit.jupiter.api.Test

import java.io.ByteArrayInputStream
import java.util.Properties

class LineMessageReaderTest {

  private def defaultTestProps = {
    val props = new Properties
    props.put("topic", "topic")
    props.put("parse.key", "true")
    props.put("parse.headers", "true")
    props
  }

  @Test
  def testLineReader(): Unit = {
    val input = "key0\tvalue0\nkey1\tvalue1"

    val props = defaultTestProps
    props.put("parse.headers", "false")

    runTest(props, input, record("key0", "value0"), record("key1", "value1"))
  }

  @Test
  def testLineReaderHeader(): Unit = {
    val input = "headerKey0:headerValue0,headerKey1:headerValue1\tkey0\tvalue0\n"
    val expected = record("key0", "value0", List("headerKey0" -> "headerValue0", "headerKey1" -> "headerValue1"))
    runTest(defaultTestProps, input, expected)
  }

  @Test
  def testMinimalValidInputWithHeaderKeyAndValue(): Unit = {
    runTest(defaultTestProps, ":\t\t", record("", "", List("" -> "")))
  }

  @Test
  def testKeyMissingValue(): Unit = {
    val props = defaultTestProps
    props.put("parse.headers", "false")
    runTest(props, "key\t", record("key", ""))
  }

  @Test
  def testDemarcationsLongerThanOne(): Unit = {
    val props = defaultTestProps
    props.put("key.separator", "\t\t")
    props.put("headers.delimiter", "\t\t")
    props.put("headers.separator", "---")
    props.put("headers.key.separator", "::::")

    runTest(
      props,
      "headerKey0.0::::headerValue0.0---headerKey1.0::::\t\tkey\t\tvalue",
      record("key", "value", List("headerKey0.0" -> "headerValue0.0", "headerKey1.0"-> ""))
    )
  }

  @Test
  def testLineReaderHeaderNoKey(): Unit = {
    val input = "headerKey:headerValue\tvalue\n"

    val props = defaultTestProps
    props.put("parse.key", "false")

    runTest(props, input, record(null, "value", List("headerKey" -> "headerValue")))
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
    props.put("headers.delimiter", "!")
    props.put("headers.separator", "&")
    props.put("headers.key.separator", ":")

    val input =
      "headerKey0.0:headerValue0.0&headerKey0.1:headerValue0.1!key0#value0\n" +
      "headerKey1.0:headerValue1.0!key1#value1"

    val record0 = record("key0", "value0", List("headerKey0.0" -> "headerValue0.0", "headerKey0.1" -> "headerValue0.1"))
    val record1 = record("key1", "value1", List("headerKey1.0" -> "headerValue1.0"))

    runTest(props, input, record0, record1)
  }

  @Test
  def testMissingKeySeparator(): Unit = {
    val lineReader = new LineMessageReader
    val input =
      "headerKey0.0:headerValue0.0,headerKey0.1:headerValue0.1\tkey0\tvalue0\n" +
      "headerKey1.0:headerValue1.0\tkey1[MISSING-DELIMITER]value1"

    lineReader.init(new ByteArrayInputStream(input.getBytes), defaultTestProps)
    lineReader.readMessage()

    val expectedException = assertThrows(classOf[KafkaException], () => lineReader.readMessage())

    assertEquals(
      "No key separator found on line number 2: 'headerKey1.0:headerValue1.0\tkey1[MISSING-DELIMITER]value1'",
      expectedException.getMessage
    )
  }

  @Test
  def testMissingHeaderKeySeparator(): Unit = {
    val lineReader = new LineMessageReader()
    val input = "key[MISSING-DELIMITER]val\tkey0\tvalue0\n"
    lineReader.init(new ByteArrayInputStream(input.getBytes), defaultTestProps)

    val expectedException = assertThrows(classOf[KafkaException], () => lineReader.readMessage())

    assertEquals(
      "No header key separator found in pair 'key[MISSING-DELIMITER]val' on line number 1",
      expectedException.getMessage
    )
  }

  @Test
  def testHeaderDemarcationCollision(): Unit = {
    val props = defaultTestProps
    props.put("headers.delimiter", "\t")
    props.put("headers.separator", "\t")
    props.put("headers.key.separator", "\t")

    assertThrowsOnInvalidPatternConfig(props, "headers.delimiter and headers.separator may not be equal")

    props.put("headers.separator", ",")
    assertThrowsOnInvalidPatternConfig(props, "headers.delimiter and headers.key.separator may not be equal")

    props.put("headers.key.separator", ",")
    assertThrowsOnInvalidPatternConfig(props, "headers.separator and headers.key.separator may not be equal")
  }

  private def assertThrowsOnInvalidPatternConfig(props: Properties, expectedMessage: String): Unit = {
    val exception = assertThrows(classOf[KafkaException], () => new LineMessageReader().init(null, props))
    assertEquals(
      expectedMessage,
      exception.getMessage
    )
  }

  @Test
  def testIgnoreErrorInInput(): Unit = {
    val input =
      "headerKey0.0:headerValue0.0\tkey0\tvalue0\n" +
      "headerKey1.0:headerValue1.0,headerKey1.1:headerValue1.1[MISSING-HEADER-DELIMITER]key1\tvalue1\n" +
      "headerKey2.0:headerValue2.0\tkey2[MISSING-KEY-DELIMITER]value2\n" +
      "headerKey3.0:headerValue3.0[MISSING-HEADER-DELIMITER]key3[MISSING-KEY-DELIMITER]value3\n"

    val props = defaultTestProps
    props.put("ignore.error", "true")

    val validRecord = record("key0", "value0", List("headerKey0.0" -> "headerValue0.0"))

    val missingHeaderDelimiter: ProducerRecord[String, String] =
      record(
        null,
        "value1",
        List("headerKey1.0" -> "headerValue1.0", "headerKey1.1" -> "headerValue1.1[MISSING-HEADER-DELIMITER]key1")
      )

    val missingKeyDelimiter: ProducerRecord[String, String] =
      record(
        null,
        "key2[MISSING-KEY-DELIMITER]value2",
        List("headerKey2.0" -> "headerValue2.0")
      )

    val missingKeyHeaderDelimiter: ProducerRecord[String, String] =
      record(
        null,
        "headerKey3.0:headerValue3.0[MISSING-HEADER-DELIMITER]key3[MISSING-KEY-DELIMITER]value3",
        List()
      )

    runTest(props, input, validRecord, missingHeaderDelimiter, missingKeyDelimiter, missingKeyHeaderDelimiter)
  }

  @Test
  def testMalformedHeaderIgnoreError(): Unit = {
    val input = "key-val\tkey0\tvalue0\n"

    val props = defaultTestProps
    props.put("ignore.error", "true")

    val expected = record("key0", "value0", List("key-val" -> null))

    runTest(props, input, expected)
  }

  def runTest(props: Properties, input: String, expectedRecords: ProducerRecord[String, String]*): Unit = {
    val lineReader = new LineMessageReader
    lineReader.init(new ByteArrayInputStream(input.getBytes), props)
    expectedRecords.foreach(r => assertRecordEquals(r, lineReader.readMessage()))
  }

  //  The equality method of ProducerRecord compares memory references for the header iterator, this is why this custom equality check is used.
  private def assertRecordEquals[K, V](expected: ProducerRecord[K, V], actual: ProducerRecord[Array[Byte], Array[Byte]]): Unit = {
    assertEquals(expected.key, if (actual.key == null) null else new String(actual.key))
    assertEquals(expected.value, if (actual.value == null) null else new String(actual.value))
    assertEquals(expected.headers.toArray.toList, actual.headers.toArray.toList)
  }

  private def record[K, V](key: K, value: V, headers: List[(String, String)]): ProducerRecord[K, V] = {
    val record = new ProducerRecord("topic", key, value)
    headers.foreach(h => record.headers.add(h._1, if (h._2 != null) h._2.getBytes else null))
    record
  }

  private def record[K, V](key: K, value: V): ProducerRecord[K, V] = {
    new ProducerRecord("topic", key, value)
  }
}
