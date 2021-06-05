/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.scala

import java.util

import org.apache.kafka.common.serialization.{Deserializer, Serde, Serdes => JSerdes, Serializer}
import org.apache.kafka.streams.kstream.WindowedSerdes

@deprecated(
  "Use org.apache.kafka.streams.scala.serialization.Serdes. For WindowedSerdes.TimeWindowedSerde, use explicit constructors.",
  "2.7.0"
)
object Serdes {
  implicit def String: Serde[String] = JSerdes.String()
  implicit def Long: Serde[Long] = JSerdes.Long().asInstanceOf[Serde[Long]]
  implicit def JavaLong: Serde[java.lang.Long] = JSerdes.Long()
  implicit def ByteArray: Serde[Array[Byte]] = JSerdes.ByteArray()
  implicit def Bytes: Serde[org.apache.kafka.common.utils.Bytes] = JSerdes.Bytes()
  implicit def Float: Serde[Float] = JSerdes.Float().asInstanceOf[Serde[Float]]
  implicit def JavaFloat: Serde[java.lang.Float] = JSerdes.Float()
  implicit def Double: Serde[Double] = JSerdes.Double().asInstanceOf[Serde[Double]]
  implicit def JavaDouble: Serde[java.lang.Double] = JSerdes.Double()
  implicit def Integer: Serde[Int] = JSerdes.Integer().asInstanceOf[Serde[Int]]
  implicit def JavaInteger: Serde[java.lang.Integer] = JSerdes.Integer()

  implicit def timeWindowedSerde[T](implicit tSerde: Serde[T]): WindowedSerdes.TimeWindowedSerde[T] =
    new WindowedSerdes.TimeWindowedSerde[T](tSerde)

  implicit def sessionWindowedSerde[T](implicit tSerde: Serde[T]): WindowedSerdes.SessionWindowedSerde[T] =
    new WindowedSerdes.SessionWindowedSerde[T](tSerde)

  def fromFn[T >: Null](serializer: T => Array[Byte], deserializer: Array[Byte] => Option[T]): Serde[T] =
    JSerdes.serdeFrom(
      new Serializer[T] {
        override def serialize(topic: String, data: T): Array[Byte] = serializer(data)
        override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()
        override def close(): Unit = ()
      },
      new Deserializer[T] {
        override def deserialize(topic: String, data: Array[Byte]): T = deserializer(data).orNull
        override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()
        override def close(): Unit = ()
      }
    )

  def fromFn[T >: Null](serializer: (String, T) => Array[Byte],
                        deserializer: (String, Array[Byte]) => Option[T]): Serde[T] =
    JSerdes.serdeFrom(
      new Serializer[T] {
        override def serialize(topic: String, data: T): Array[Byte] = serializer(topic, data)
        override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()
        override def close(): Unit = ()
      },
      new Deserializer[T] {
        override def deserialize(topic: String, data: Array[Byte]): T = deserializer(topic, data).orNull
        override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()
        override def close(): Unit = ()
      }
    )
}
