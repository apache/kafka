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
import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node._
import kafka.utils.JsonTest.TestObject
import kafka.utils.json.JsonValue
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConverters._
import scala.collection.Map

object JsonTest {
  case class TestObject(@JsonProperty("foo") foo: String, @JsonProperty("bar") bar: Int)
}

class JsonTest {

  @Test
  def testJsonParse() {
    val jnf = JsonNodeFactory.instance

    assertEquals(Json.parseFull("{}"), Some(JsonValue(new ObjectNode(jnf))))

    assertEquals(Json.parseFull("""{"foo":"bar"s}"""), None)

    val objectNode = new ObjectNode(
      jnf,
      Map[String, JsonNode]("foo" -> new TextNode("bar"), "is_enabled" -> BooleanNode.TRUE).asJava
    )
    assertEquals(Json.parseFull("""{"foo":"bar", "is_enabled":true}"""), Some(JsonValue(objectNode)))

    val arrayNode = new ArrayNode(jnf)
    Vector(1, 2, 3).map(new IntNode(_)).foreach(arrayNode.add)
    assertEquals(Json.parseFull("[1, 2, 3]"), Some(JsonValue(arrayNode)))

    // Test with encoder that properly escapes backslash and quotes
    val map = Map("foo1" -> """bar1\,bar2""", "foo2" -> """\bar""")
    val encoded = Json.legacyEncodeAsString(map)
    val decoded = Json.parseFull(encoded)
    assertEquals(Json.parseFull("""{"foo1":"bar1\\,bar2", "foo2":"\\bar"}"""), decoded)

    // Test strings with non-escaped backslash and quotes. This is to verify that ACLs
    // containing non-escaped chars persisted using 1.0 can be parsed.
    assertEquals(decoded, Json.parseFull("""{"foo1":"bar1\,bar2", "foo2":"\bar"}"""))
  }

  @Test
  def testLegacyEncodeAsString() {
    assertEquals("null", Json.legacyEncodeAsString(null))
    assertEquals("1", Json.legacyEncodeAsString(1))
    assertEquals("1", Json.legacyEncodeAsString(1L))
    assertEquals("1", Json.legacyEncodeAsString(1.toByte))
    assertEquals("1", Json.legacyEncodeAsString(1.toShort))
    assertEquals("1.0", Json.legacyEncodeAsString(1.0))
    assertEquals(""""str"""", Json.legacyEncodeAsString("str"))
    assertEquals("true", Json.legacyEncodeAsString(true))
    assertEquals("false", Json.legacyEncodeAsString(false))
    assertEquals("[]", Json.legacyEncodeAsString(Seq()))
    assertEquals("[1,2,3]", Json.legacyEncodeAsString(Seq(1,2,3)))
    assertEquals("""[1,"2",[3]]""", Json.legacyEncodeAsString(Seq(1,"2",Seq(3))))
    assertEquals("{}", Json.legacyEncodeAsString(Map()))
    assertEquals("""{"a":1,"b":2}""", Json.legacyEncodeAsString(Map("a" -> 1, "b" -> 2)))
    assertEquals("""{"a":[1,2],"c":[3,4]}""", Json.legacyEncodeAsString(Map("a" -> Seq(1,2), "c" -> Seq(3,4))))
    assertEquals(""""str1\\,str2"""", Json.legacyEncodeAsString("""str1\,str2"""))
    assertEquals(""""\"quoted\""""", Json.legacyEncodeAsString(""""quoted""""))

  }

  @Test
  def testEncodeAsString() {
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
  def testEncodeAsBytes() {
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

    assertTrue(result.isRight)
    assertEquals(TestObject(foo, bar), result.right.get)
  }

  @Test
  def testParseToWithInvalidJson() = {
    val result = Json.parseStringAs[TestObject]("{invalid json}")

    assertTrue(result.isLeft)
    assertEquals(classOf[JsonParseException], result.left.get.getClass)
  }
}
