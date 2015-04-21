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

import scala.collection._

import com.fasterxml.jackson.databind.{JsonMappingException, JsonNode}
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}

trait JsonValue {

  protected def node: JsonNode

  def to[T](implicit decodeJson: DecodeJson[T]): T = decodeJson.decode(node)

  def toEither[T](implicit decodeJson: DecodeJson[T]): Either[String, T] = decodeJson.decodeEither(node)

  def asJsonObject: JsonObject =
    asJsonObjectOption.getOrElse(throw new JsonMappingException("Expected JSON object, received " + node))

  def asJsonObjectOption: Option[JsonObject] = this match {
    case j: JsonObject => Some(j)
    case _ => node match {
      case n: ObjectNode => Some(new JsonObject(n))
      case _ => None
    }
  }

  def asJsonArray: JsonArray =
    asJsonArrayOption.getOrElse(throw new JsonMappingException("Expected JSON array, received " + node))

  def asJsonArrayOption: Option[JsonArray] = this match {
    case j: JsonArray => Some(j)
    case _ => node match {
      case n: ArrayNode => Some(new JsonArray(n))
      case _ => None
    }
  }

  override def hashCode: Int = node.hashCode

  override def equals(a: Any): Boolean = a match {
    case a: JsonValue => node == a.node
    case _ => false
  }

  override def toString: String = node.toString

}

object JsonValue {

  def apply(node: JsonNode): JsonValue = node match {
    case n: ObjectNode => new JsonObject(n)
    case n: ArrayNode => new JsonArray(n)
    case _ => new BasicJsonValue(node)
  }

  private class BasicJsonValue private[json] (protected val node: JsonNode) extends JsonValue

}
