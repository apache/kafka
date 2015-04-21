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

import jawn.support.spray.Parser
import kafka.common._
import spray.json._
import scala.collection._

/**
 *  A wrapper that synchronizes JSON in scala, which is not threadsafe.
 */
object Json extends Logging {

  /**
   * Parse a JSON string into an object
   */
  def parseFull(input: String): Option[JsValue] = Parser.parseFromString(input).toOption

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
      case m: Map[_, _] => 
        "{" + 
          m.map(elem => 
            elem match {
            case t: Tuple2[_,_] => encode(t._1) + ":" + encode(t._2)
            case _ => throw new IllegalArgumentException("Invalid map element (" + elem + ") in " + obj)
          }).mkString(",") + 
      "}"
      case a: Array[_] => encode(a.toSeq)
      case i: Iterable[_] => "[" + i.map(encode).mkString(",") + "]"
      case other: AnyRef => throw new IllegalArgumentException("Unknown arguement of type " + other.getClass + ": " + other)
    }
  }

  /** Provides extension methods for `JsObject` */
  implicit class RichJsObject(val value: JsObject) extends AnyVal {
    def fieldOption[T: JsonReader](fieldName: String): Option[T] = value.fields.get(fieldName).flatMap { fieldValue =>
      try Some(fieldValue.convertTo[T])
      catch { case e: DeserializationException => None }
    }
  }

  /** Provides extension methods for `JsValue` */
  implicit class RichJsValue(val value: JsValue) extends AnyVal {

    def asJsObjectOption: Option[JsObject] = value match {
      case v: JsObject => Some(v)
      case _ => None
    }

    def asJsArray: JsArray = value match {
      case v: JsArray => v
      case _ => spray.json.deserializationError("JSON array expected")
    }

    def asJsArrayOption: Option[JsArray] = value match {
      case v: JsArray => Some(v)
      case _ => None
    }

  }

  /* spray-json provides only provides a format for `scala.collection.immutable.Map`, so we include one for
   * `scala.collection.Map`. */
  implicit def collectionMapFormat[K: JsonFormat, V: JsonFormat] = new RootJsonFormat[Map[K, V]] {
    def write(m: Map[K, V]) = {
      val fields: immutable.Map[String, JsValue] = m.map { case (k, v) =>
        k.toJson match {
          case JsString(x) => x -> v.toJson
          case x => throw new SerializationException("Map key must be formatted as JsString, not '" + x + "'")
        }
      }(collection.breakOut)
      JsObject(fields)
    }
    def read(value: JsValue) = value match {
      case x: JsObject => x.fields.map { case (k, v) =>
        (JsString(k).convertTo[K], v.convertTo[V])
      } (collection.breakOut)
      case x => deserializationError("Expected Map as JsObject, but got " + x)
    }
  }

}
