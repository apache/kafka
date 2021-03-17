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
package kafka.utils

import java.nio.charset.StandardCharsets

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.core.{JsonParseException, JsonProcessingException}
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node._
import kafka.utils.JsonTest.TestObject
import kafka.utils.json.JsonValue
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import scala.jdk.CollectionConverters._
import scala.collection.Map

object JsonTest {
  case class TestObject(@JsonProperty("foo") foo: String, @JsonProperty("bar") bar: Int)
}

class JsonTest {

  @Test
  def testJsonParse(): Unit = {
    val jnf = JsonNodeFactory.instance

    assertEquals(Some(JsonValue(new ObjectNode(jnf))), Json.parseFull("{}"))
    assertEquals(Right(JsonValue(new ObjectNode(jnf))), Json.tryParseFull("{}"))
    assertEquals(classOf[Left[JsonProcessingException, JsonValue]], Json.tryParseFull(null).getClass)
    assertThrows(classOf[IllegalArgumentException], () => Json.tryParseBytes(null))

    assertEquals(None, Json.parseFull(""))
    assertEquals(classOf[Left[JsonProcessingException, JsonValue]], Json.tryParseFull("").getClass)

    assertEquals(None, Json.parseFull("""{"foo":"bar"s}"""))
    val tryRes = Json.tryParseFull("""{"foo":"bar"s}""")
    assertTrue(tryRes.isInstanceOf[Left[_, JsonValue]])

    val objectNode = new ObjectNode(
      jnf,
      Map[String, JsonNode]("foo" -> new TextNode("bar"), "is_enabled" -> BooleanNode.TRUE).asJava
    )
    assertEquals(Some(JsonValue(objectNode)), Json.parseFull("""{"foo":"bar", "is_enabled":true}"""))
    assertEquals(Right(JsonValue(objectNode)), Json.tryParseFull("""{"foo":"bar", "is_enabled":true}"""))

    val arrayNode = new ArrayNode(jnf)
    Vector(1, 2, 3).map(new IntNode(_)).foreach(arrayNode.add)
    assertEquals(Some(JsonValue(arrayNode)), Json.parseFull("[1, 2, 3]"))

    // Test with encoder that properly escapes backslash and quotes
    val map = Map("foo1" -> """bar1\,bar2""", "foo2" -> """\bar""").asJava
    val encoded = Json.encodeAsString(map)
    val decoded = Json.parseFull(encoded)
    assertEquals(decoded, Json.parseFull("""{"foo1":"bar1\\,bar2", "foo2":"\\bar"}"""))
  }

  @Test
  def testEncodeAsString(): Unit = {
    assertEquals("null", Json.encodeAsString(null))
    assertEquals("1", Json.encodeAsString(1))
    assertEquals("1", Json.encodeAsString(1L))
    assertEquals("1", Json.encodeAsString(1.toByte))
    assertEquals("1", Json.encodeAsString(1.toShort))
    assertEquals("1.0", Json.encodeAsString(1.0))
    assertEquals(""""str"""", Json.encodeAsString("str"))
    assertEquals("true", Json.encodeAsString(true))
    assertEquals("false", Json.encodeAsString(false))
    assertEquals("[]", Json.encodeAsString(Seq().asJava))
    assertEquals("[null]", Json.encodeAsString(Seq(null).asJava))
    assertEquals("[1,2,3]", Json.encodeAsString(Seq(1,2,3).asJava))
    assertEquals("""[1,"2",[3],null]""", Json.encodeAsString(Seq(1,"2",Seq(3).asJava,null).asJava))
    assertEquals("{}", Json.encodeAsString(Map().asJava))
    assertEquals("""{"a":1,"b":2,"c":null}""", Json.encodeAsString(Map("a" -> 1, "b" -> 2, "c" -> null).asJava))
    assertEquals("""{"a":[1,2],"c":[3,4]}""", Json.encodeAsString(Map("a" -> Seq(1,2).asJava, "c" -> Seq(3,4).asJava).asJava))
    assertEquals("""{"a":[1,2],"b":[3,4],"c":null}""", Json.encodeAsString(Map("a" -> Seq(1,2).asJava, "b" -> Seq(3,4).asJava, "c" -> null).asJava))
    assertEquals(""""str1\\,str2"""", Json.encodeAsString("""str1\,str2"""))
    assertEquals(""""\"quoted\""""", Json.encodeAsString(""""quoted""""))
  }

  @Test
  def testEncodeAsBytes(): Unit = {
    assertEquals("null", new String(Json.encodeAsBytes(null), StandardCharsets.UTF_8))
    assertEquals("1", new String(Json.encodeAsBytes(1), StandardCharsets.UTF_8))
    assertEquals("1", new String(Json.encodeAsBytes(1L), StandardCharsets.UTF_8))
    assertEquals("1", new String(Json.encodeAsBytes(1.toByte), StandardCharsets.UTF_8))
    assertEquals("1", new String(Json.encodeAsBytes(1.toShort), StandardCharsets.UTF_8))
    assertEquals("1.0", new String(Json.encodeAsBytes(1.0), StandardCharsets.UTF_8))
    assertEquals(""""str"""",  new String(Json.encodeAsBytes("str"), StandardCharsets.UTF_8))
    assertEquals("true", new String(Json.encodeAsBytes(true), StandardCharsets.UTF_8))
    assertEquals("false", new String(Json.encodeAsBytes(false), StandardCharsets.UTF_8))
    assertEquals("[]", new String(Json.encodeAsBytes(Seq().asJava), StandardCharsets.UTF_8))
    assertEquals("[null]", new String(Json.encodeAsBytes(Seq(null).asJava), StandardCharsets.UTF_8))
    assertEquals("[1,2,3]", new String(Json.encodeAsBytes(Seq(1,2,3).asJava), StandardCharsets.UTF_8))
    assertEquals("""[1,"2",[3],null]""", new String(Json.encodeAsBytes(Seq(1,"2",Seq(3).asJava,null).asJava), StandardCharsets.UTF_8))
    assertEquals("{}", new String(Json.encodeAsBytes(Map().asJava), StandardCharsets.UTF_8))
    assertEquals("""{"a":1,"b":2,"c":null}""", new String(Json.encodeAsBytes(Map("a" -> 1, "b" -> 2, "c" -> null).asJava), StandardCharsets.UTF_8))
    assertEquals("""{"a":[1,2],"c":[3,4]}""", new String(Json.encodeAsBytes(Map("a" -> Seq(1,2).asJava, "c" -> Seq(3,4).asJava).asJava), StandardCharsets.UTF_8))
    assertEquals("""{"a":[1,2],"b":[3,4],"c":null}""", new String(Json.encodeAsBytes(Map("a" -> Seq(1,2).asJava, "b" -> Seq(3,4).asJava, "c" -> null).asJava), StandardCharsets.UTF_8))
    assertEquals(""""str1\\,str2"""", new String(Json.encodeAsBytes("""str1\,str2"""), StandardCharsets.UTF_8))
    assertEquals(""""\"quoted\""""", new String(Json.encodeAsBytes(""""quoted""""), StandardCharsets.UTF_8))
  }

  @Test
  def testParseTo() = {
    val foo = "baz"
    val bar = 1

    val result = Json.parseStringAs[TestObject](s"""{"foo": "$foo", "bar": $bar}""")

    assertEquals(Right(TestObject(foo, bar)), result)
  }

  @Test
  def testParseToWithInvalidJson() = {
    val result = Json.parseStringAs[TestObject]("{invalid json}")
    assertEquals(Left(classOf[JsonParseException]), result.left.map(_.getClass))
  }
}
