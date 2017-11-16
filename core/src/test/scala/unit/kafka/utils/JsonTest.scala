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
    assertEquals("null", Json.encode(null))
    assertEquals("1", Json.encode(1))
    assertEquals("1", Json.encode(1L))
    assertEquals("1", Json.encode(1.toByte))
    assertEquals("1", Json.encode(1.toShort))
    assertEquals("1.0", Json.encode(1.0))
    assertEquals("\"str\"", Json.encode("str"))
    assertEquals("true", Json.encode(true))
    assertEquals("false", Json.encode(false))
    assertEquals("[]", Json.encode(Seq()))
    assertEquals("[1,2,3]", Json.encode(Seq(1,2,3)))
    assertEquals("[1,\"2\",[3]]", Json.encode(Seq(1,"2",Seq(3))))
    assertEquals("{}", Json.encode(Map()))
    assertEquals("{\"a\":1,\"b\":2}", Json.encode(Map("a" -> 1, "b" -> 2)))
    assertEquals("{\"a\":[1,2],\"c\":[3,4]}", Json.encode(Map("a" -> Seq(1,2), "c" -> Seq(3,4))))
  }
  
}
