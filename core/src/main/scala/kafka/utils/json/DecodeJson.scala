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

import scala.collection.{Factory, Map, Seq}
import scala.jdk.CollectionConverters._
import com.fasterxml.jackson.databind.{JsonMappingException, JsonNode}

/**
 * A type class for parsing JSON. This should typically be used via `JsonValue.apply`.
 */
trait DecodeJson[T] {

  /**
   * Decode the JSON node provided into an instance of `Right[T]`, if possible. Otherwise, return an error message
   * wrapped by an instance of `Left`.
   */
  def decodeEither(node: JsonNode): Either[String, T]

  /**
   * Decode the JSON node provided into an instance of `T`.
   *
   * @throws JsonMappingException if `node` cannot be decoded into `T`.
   */
  def decode(node: JsonNode): T =
    decodeEither(node) match {
      case Right(x) => x
      case Left(x) => throw new JsonMappingException(null, x)
    }

}

/**
 * Contains `DecodeJson` type class instances. That is, we need one instance for each type that we want to be able to
 * to parse into. It is a compiler error to try to parse into a type for which there is no instance.
 */
object DecodeJson {

  implicit object DecodeBoolean extends DecodeJson[Boolean] {
    def decodeEither(node: JsonNode): Either[String, Boolean] =
      if (node.isBoolean) Right(node.booleanValue) else Left(s"Expected `Boolean` value, received $node")
  }

  implicit object DecodeDouble extends DecodeJson[Double] {
    def decodeEither(node: JsonNode): Either[String, Double] =
      if (node.isDouble || node.isLong || node.isInt)
        Right(node.doubleValue)
      else Left(s"Expected `Double` value, received $node")
  }

  implicit object DecodeInt extends DecodeJson[Int] {
    def decodeEither(node: JsonNode): Either[String, Int] =
      if (node.isInt) Right(node.intValue) else Left(s"Expected `Int` value, received $node")
  }

  implicit object DecodeLong extends DecodeJson[Long] {
    def decodeEither(node: JsonNode): Either[String, Long] =
      if (node.isLong || node.isInt) Right(node.longValue) else Left(s"Expected `Long` value, received $node")
  }

  implicit object DecodeString extends DecodeJson[String] {
    def decodeEither(node: JsonNode): Either[String, String] =
      if (node.isTextual) Right(node.textValue) else Left(s"Expected `String` value, received $node")
  }

  implicit def decodeOption[E](implicit decodeJson: DecodeJson[E]): DecodeJson[Option[E]] = (node: JsonNode) => {
    if (node.isNull) Right(None)
    else decodeJson.decodeEither(node).map(Some(_))
  }

  implicit def decodeSeq[E, S[+T] <: Seq[E]](implicit decodeJson: DecodeJson[E], factory: Factory[E, S[E]]): DecodeJson[S[E]] = (node: JsonNode) => {
    if (node.isArray)
      decodeIterator(node.elements.asScala)(decodeJson.decodeEither)
    else Left(s"Expected JSON array, received $node")
  }

  implicit def decodeMap[V, M[K, +V] <: Map[K, V]](implicit decodeJson: DecodeJson[V], factory: Factory[(String, V), M[String, V]]): DecodeJson[M[String, V]] = (node: JsonNode) => {
    if (node.isObject)
      decodeIterator(node.fields.asScala)(e => decodeJson.decodeEither(e.getValue).map(v => (e.getKey, v)))
    else Left(s"Expected JSON object, received $node")
  }

  private def decodeIterator[S, T, C](it: Iterator[S])(f: S => Either[String, T])(implicit factory: Factory[T, C]): Either[String, C] = {
    val result = factory.newBuilder
    while (it.hasNext) {
      f(it.next()) match {
        case Right(x) => result += x
        case Left(x) => return Left(x)
      }
    }
    Right(result.result())
  }

}
