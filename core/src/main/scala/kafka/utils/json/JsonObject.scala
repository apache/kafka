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

import com.fasterxml.jackson.databind.JsonMappingException

import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind.node.ObjectNode

import scala.collection.Iterator

/**
 * A thin wrapper over Jackson's `ObjectNode` for a more idiomatic API. See `JsonValue` for more details.
 */
class JsonObject private[json] (protected val node: ObjectNode) extends JsonValue {

  def apply(name: String): JsonValue =
    get(name).getOrElse(throw new JsonMappingException(s"No such field exists: `$name`"))

  def get(name: String): Option[JsonValue] = Option(node.get(name)).map(JsonValue(_))

  def iterator: Iterator[(String, JsonValue)] = node.fields.asScala.map { entry =>
    (entry.getKey, JsonValue(entry.getValue))
  }

}
