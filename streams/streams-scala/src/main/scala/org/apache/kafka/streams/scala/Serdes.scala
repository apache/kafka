/*
 * Copyright (C) 2018 Lightbend Inc. <https://www.lightbend.com>
 * Copyright (C) 2017-2018 Alexis Seigneurin.
 *
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

import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer, Serdes => JSerdes}
import org.apache.kafka.streams.kstream.WindowedSerdes

object Serdes {
  implicit val String: Serde[String]                             = JSerdes.String()
  implicit val Long: Serde[Long]                                 = JSerdes.Long().asInstanceOf[Serde[Long]]
  implicit val JavaLong: Serde[java.lang.Long]                   = JSerdes.Long()
  implicit val ByteArray: Serde[Array[Byte]]                     = JSerdes.ByteArray()
  implicit val Bytes: Serde[org.apache.kafka.common.utils.Bytes] = JSerdes.Bytes()
  implicit val Float: Serde[Float]                               = JSerdes.Float().asInstanceOf[Serde[Float]]
  implicit val JavaFloat: Serde[java.lang.Float]                 = JSerdes.Float()
  implicit val Double: Serde[Double]                             = JSerdes.Double().asInstanceOf[Serde[Double]]
  implicit val JavaDouble: Serde[java.lang.Double]               = JSerdes.Double()
  implicit val Integer: Serde[Int]                               = JSerdes.Integer().asInstanceOf[Serde[Int]]
  implicit val JavaInteger: Serde[java.lang.Integer]             = JSerdes.Integer()

  implicit def timeWindowedSerde[T]: WindowedSerdes.TimeWindowedSerde[T] = new WindowedSerdes.TimeWindowedSerde[T]()
  implicit def sessionWindowedSerde[T]: WindowedSerdes.SessionWindowedSerde[T] = new WindowedSerdes.SessionWindowedSerde[T]()

  def fromFn[T >: Null](serializer: T => Array[Byte], deserializer: Array[Byte] => Option[T]): Serde[T] =
    JSerdes.serdeFrom(
      new Serializer[T] {
        override def serialize(topic: String, data: T): Array[Byte]                = serializer(data)
        override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()
        override def close(): Unit                                                 = ()
      },
      new Deserializer[T] {
        override def deserialize(topic: String, data: Array[Byte]): T              = deserializer(data).orNull
        override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()
        override def close(): Unit                                                 = ()
      }
    )

  def fromFn[T >: Null](serializer: (String, T) => Array[Byte],
    deserializer: (String, Array[Byte]) => Option[T]): Serde[T] =
    JSerdes.serdeFrom(
      new Serializer[T] {
        override def serialize(topic: String, data: T): Array[Byte]                = serializer(topic, data)
        override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()
        override def close(): Unit                                                 = ()
      },
      new Deserializer[T] {
        override def deserialize(topic: String, data: Array[Byte]): T              = deserializer(topic, data).orNull
        override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()
        override def close(): Unit                                                 = ()
      }
    )
}
