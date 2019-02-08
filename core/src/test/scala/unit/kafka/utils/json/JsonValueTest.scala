/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.utils.json

import com.fasterxml.jackson.databind.{ObjectMapper, JsonMappingException}
import org.junit.Test
import org.junit.Assert._

import kafka.utils.Json

class JsonValueTest {

  private val json = """
    |{
    |  "boolean": false,
    |  "int": 1234,
    |  "long": 3000000000,
    |  "double": 16.244355,
    |  "string": "string",
    |  "number_as_string": "123",
    |  "array": [4.0, 11.1, 44.5],
    |  "object": {
    |    "a": true,
    |    "b": false
    |  },
    |  "null": null
    |}
   """.stripMargin

  private def parse(s: String): JsonValue =
    Json.parseFull(s).getOrElse(sys.error("Failed to parse json: " + s))

  private def assertTo[T: DecodeJson](expected: T, jsonValue: JsonObject => JsonValue): Unit = {
    val parsed = jsonValue(parse(json).asJsonObject)
    assertEquals(Right(expected), parsed.toEither[T])
    assertEquals(expected, parsed.to[T])
  }

  private def assertToFails[T: DecodeJson](jsonValue: JsonObject => JsonValue): Unit = {
    val parsed = jsonValue(parse(json).asJsonObject)
    assertTrue(parsed.toEither[T].isLeft)
    assertThrow[JsonMappingException](parsed.to[T])
  }

  def assertThrow[E <: Throwable : Manifest](body: => Unit): Unit = {
    import scala.util.control.Exception._
    val klass = manifest[E].runtimeClass
    catchingPromiscuously(klass).opt(body).foreach { _ =>
      fail("Expected `" + klass + "` to be thrown, but no exception was thrown")
    }
  }

  @Test
  def testAsJsonObject(): Unit = {
    val parsed = parse(json).asJsonObject
    val obj = parsed("object")
    assertEquals(obj, obj.asJsonObject)
    assertThrow[JsonMappingException](parsed("array").asJsonObject)
  }

  @Test
  def testAsJsonObjectOption(): Unit = {
    val parsed = parse(json).asJsonObject
    assertTrue(parsed("object").asJsonObjectOption.isDefined)
    assertEquals(None, parsed("array").asJsonObjectOption)
  }

  @Test
  def testAsJsonArray(): Unit = {
    val parsed = parse(json).asJsonObject
    val array = parsed("array")
    assertEquals(array, array.asJsonArray)
    assertThrow[JsonMappingException](parsed("object").asJsonArray)
  }

  @Test
  def testAsJsonArrayOption(): Unit = {
    val parsed = parse(json).asJsonObject
    assertTrue(parsed("array").asJsonArrayOption.isDefined)
    assertEquals(None, parsed("object").asJsonArrayOption)
  }

  @Test
  def testJsonObjectGet(): Unit = {
    val parsed = parse(json).asJsonObject
    assertEquals(Some(parse("""{"a":true,"b":false}""")), parsed.get("object"))
    assertEquals(None, parsed.get("aaaaa"))
  }

  @Test
  def testJsonObjectApply(): Unit = {
    val parsed = parse(json).asJsonObject
    assertEquals(parse("""{"a":true,"b":false}"""), parsed("object"))
    assertThrow[JsonMappingException](parsed("aaaaaaaa"))
  }

  @Test
  def testJsonObjectIterator(): Unit = {
    assertEquals(
      Vector("a" -> parse("true"), "b" -> parse("false")),
      parse(json).asJsonObject("object").asJsonObject.iterator.toVector
    )
  }

  @Test
  def testJsonArrayIterator(): Unit = {
    assertEquals(Vector("4.0", "11.1", "44.5").map(parse), parse(json).asJsonObject("array").asJsonArray.iterator.toVector)
  }

  @Test
  def testJsonValueEquals(): Unit = {

    assertEquals(parse(json), parse(json))

    assertEquals(parse("""{"blue": true, "red": false}"""), parse("""{"red": false, "blue": true}"""))
    assertNotEquals(parse("""{"blue": true, "red": true}"""), parse("""{"red": false, "blue": true}"""))

    assertEquals(parse("""[1, 2, 3]"""), parse("""[1, 2, 3]"""))
    assertNotEquals(parse("""[1, 2, 3]"""), parse("""[2, 1, 3]"""))

    assertEquals(parse("1344"), parse("1344"))
    assertNotEquals(parse("1344"), parse("144"))

  }

  @Test
  def testJsonValueHashCode(): Unit = {
    assertEquals(new ObjectMapper().readTree(json).hashCode, parse(json).hashCode)
  }

  @Test
  def testJsonValueToString(): Unit = {
    val js = """{"boolean":false,"int":1234,"array":[4.0,11.1,44.5],"object":{"a":true,"b":false}}"""
    assertEquals(js, parse(js).toString)
  }

  @Test
  def testDecodeBoolean(): Unit = {
    assertTo[Boolean](false, _("boolean"))
    assertToFails[Boolean](_("int"))
  }

  @Test
  def testDecodeString(): Unit = {
    assertTo[String]("string", _("string"))
    assertTo[String]("123", _("number_as_string"))
    assertToFails[String](_("int"))
    assertToFails[String](_("array"))
  }

  @Test
  def testDecodeInt(): Unit = {
    assertTo[Int](1234, _("int"))
    assertToFails[Int](_("long"))
  }

  @Test
  def testDecodeLong(): Unit = {
    assertTo[Long](3000000000L, _("long"))
    assertTo[Long](1234, _("int"))
    assertToFails[Long](_("string"))
  }

  @Test
  def testDecodeDouble(): Unit = {
    assertTo[Double](16.244355, _("double"))
    assertTo[Double](1234.0, _("int"))
    assertTo[Double](3000000000L, _("long"))
    assertToFails[Double](_("string"))
  }

  @Test
  def testDecodeSeq(): Unit = {
    assertTo[Seq[Double]](Seq(4.0, 11.1, 44.5), _("array"))
    assertToFails[Seq[Double]](_("string"))
    assertToFails[Seq[Double]](_("object"))
    assertToFails[Seq[String]](_("array"))
  }

  @Test
  def testDecodeMap(): Unit = {
    assertTo[Map[String, Boolean]](Map("a" -> true, "b" -> false), _("object"))
    assertToFails[Map[String, Int]](_("object"))
    assertToFails[Map[String, String]](_("object"))
    assertToFails[Map[String, Double]](_("array"))
  }

  @Test
  def testDecodeOption(): Unit = {
    assertTo[Option[Int]](None, _("null"))
    assertTo[Option[Int]](Some(1234), _("int"))
    assertToFails[Option[String]](_("int"))
  }

}
