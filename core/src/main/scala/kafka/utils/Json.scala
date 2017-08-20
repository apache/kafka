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

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import kafka.utils.json.JsonValue
import scala.collection._

/**
 * Provides methods for parsing JSON with Jackson and encoding to JSON with a simple and naive custom implementation.
 */
object Json {

  private val mapper = new ObjectMapper()

  /**
   * Parse a JSON string into a JsonValue if possible. `None` is returned if `input` is not valid JSON.
   */
  def parseFull(input: String): Option[JsonValue] =
    try Option(mapper.readTree(input)).map(JsonValue(_))
    catch { case _: JsonProcessingException => None }

  /**
   * Encode an object into a JSON string. This method accepts any type T where
   *   T => null | Boolean | String | Number | Map[String, T] | Array[T] | Iterable[T]
   * Any other type will result in an exception.
   * 
   * This method does not properly handle non-ascii characters. 
   */
  def encode(obj: Any): String = {
    obj match {
      case null => "null"
      case b: Boolean => b.toString
      case s: String => "\"" + s + "\""
      case n: Number => n.toString
      case m: Map[_, _] => "{" +
        m.map {
          case (k, v) => encode(k) + ":" + encode(v)
          case elem => throw new IllegalArgumentException(s"Invalid map element '$elem' in $obj")
        }.mkString(",") + "}"
      case a: Array[_] => encode(a.toSeq)
      case i: Iterable[_] => "[" + i.map(encode).mkString(",") + "]"
      case other: AnyRef => throw new IllegalArgumentException(s"Unknown argument of type ${other.getClass}: $other")
    }
  }

}
