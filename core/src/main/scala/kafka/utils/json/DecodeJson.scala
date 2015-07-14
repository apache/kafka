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
import JavaConverters._
import generic.CanBuildFrom

import com.fasterxml.jackson.databind.{JsonMappingException, JsonNode}

trait DecodeJson[T] {

  def decodeEither(node: JsonNode): Either[String, T]

  def decode(node: JsonNode): T =
    decodeEither(node) match {
      case Right(x) => x
      case Left(x) => throw new JsonMappingException(x)
    }

}

object DecodeJson {

  implicit object DecodeBoolean extends DecodeJson[Boolean] {
    def decodeEither(node: JsonNode): Either[String, Boolean] =
      if (node.isBoolean) Right(node.booleanValue) else Left("Expected `Boolean` value, received " + node)
  }

  implicit object DecodeDouble extends DecodeJson[Double] {
    def decodeEither(node: JsonNode): Either[String, Double] =
      if (node.isDouble || node.isLong || node.isInt)
        Right(node.doubleValue)
      else Left("Expected `Double` value, received " + node)
  }

  implicit object DecodeInt extends DecodeJson[Int] {
    def decodeEither(node: JsonNode): Either[String, Int] =
      if (node.isInt) Right(node.intValue) else Left("Expected `Int` value, received " + node)
  }

  implicit object DecodeLong extends DecodeJson[Long] {
    def decodeEither(node: JsonNode): Either[String, Long] =
      if (node.isLong || node.isInt) Right(node.longValue) else Left("Expected `Long` value, received " + node)
  }

  implicit object DecodeString extends DecodeJson[String] {
    def decodeEither(node: JsonNode): Either[String, String] =
      if (node.isTextual) Right(node.textValue) else Left("Expected `String` value, received " + node)
  }

  implicit def decodeOption[E](implicit decodeJson: DecodeJson[E]): DecodeJson[Option[E]] = new DecodeJson[Option[E]] {
    def decodeEither(node: JsonNode): Either[String, Option[E]] = {
      if (node.isNull) Right(None)
      else decodeJson.decodeEither(node).right.map(Some(_))
    }
  }

  implicit def decodeSeq[E, S[+T] <: Seq[E]](implicit decodeJson: DecodeJson[E], cbf: CanBuildFrom[Nothing, E, S[E]]): DecodeJson[S[E]] = new DecodeJson[S[E]] {
    def decodeEither(node: JsonNode): Either[String, S[E]] = {
      if (node.isArray)
        decodeIterator(node.elements.asScala)(decodeJson.decodeEither)
      else Left("Expected JSON array, received " + node)
    }
  }

  implicit def decodeMap[V, M[K, +V] <: Map[K, V]](implicit decodeJson: DecodeJson[V], cbf: CanBuildFrom[Nothing, (String, V), M[String, V]]): DecodeJson[M[String, V]] = new DecodeJson[M[String, V]] {
    def decodeEither(node: JsonNode): Either[String, M[String, V]] = {
      if (node.isObject)
        decodeIterator(node.fields.asScala)(e => decodeJson.decodeEither(e.getValue).right.map(v => (e.getKey, v)))(cbf)
      else Left("Expected JSON object, received " + node)
    }
  }

  private def decodeIterator[S, T, C](it: Iterator[S])(f: S => Either[String, T])(implicit cbf: CanBuildFrom[Nothing, T, C]): Either[String, C] = {
    val result = cbf()
    while (it.hasNext) {
      f(it.next) match {
        case Right(x) => result += x
        case Left(x) => return Left(x)
      }
    }
    Right(result.result())
  }

}
