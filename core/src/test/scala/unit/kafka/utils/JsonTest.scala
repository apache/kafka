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

import org.junit.Assert._
import org.junit.Test
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node._
import kafka.utils.json.JsonValue
import scala.collection.JavaConverters._

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
  }

  @Test
  def testJsonEncoding() {
    assertEquals("null", Json.encodeAsString(null))
    assertEquals("1", Json.legacyEncodeAsString(1))
    assertEquals("1", Json.legacyEncodeAsString(1L))
    assertEquals("1", Json.legacyEncodeAsString(1.toByte))
    assertEquals("1", Json.legacyEncodeAsString(1.toShort))
    assertEquals("1.0", Json.legacyEncodeAsString(1.0))
    assertEquals("\"str\"", Json.legacyEncodeAsString("str"))
    assertEquals("true", Json.legacyEncodeAsString(true))
    assertEquals("false", Json.legacyEncodeAsString(false))
    assertEquals("[]", Json.legacyEncodeAsString(Seq()))
    assertEquals("[1,2,3]", Json.legacyEncodeAsString(Seq(1,2,3)))
    assertEquals("[1,\"2\",[3]]", Json.legacyEncodeAsString(Seq(1,"2",Seq(3))))
    assertEquals("{}", Json.legacyEncodeAsString(Map()))
    assertEquals("{\"a\":1,\"b\":2}", Json.legacyEncodeAsString(Map("a" -> 1, "b" -> 2)))
    assertEquals("{\"a\":[1,2],\"c\":[3,4]}", Json.legacyEncodeAsString(Map("a" -> Seq(1,2), "c" -> Seq(3,4))))
  }
  
}
