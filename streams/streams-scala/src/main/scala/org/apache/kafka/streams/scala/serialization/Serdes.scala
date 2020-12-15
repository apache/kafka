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
package org.apache.kafka.streams.scala.serialization

import java.nio.ByteBuffer
import java.util
import java.util.UUID

import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer, Serdes => JSerdes}
import org.apache.kafka.streams.kstream.WindowedSerdes

object Serdes extends LowPrioritySerdes {
  implicit def stringSerde: Serde[String] = JSerdes.String()
  implicit def longSerde: Serde[Long] = JSerdes.Long().asInstanceOf[Serde[Long]]
  implicit def javaLongSerde: Serde[java.lang.Long] = JSerdes.Long()
  implicit def byteArraySerde: Serde[Array[Byte]] = JSerdes.ByteArray()
  implicit def bytesSerde: Serde[org.apache.kafka.common.utils.Bytes] = JSerdes.Bytes()
  implicit def byteBufferSerde: Serde[ByteBuffer] = JSerdes.ByteBuffer()
  implicit def shortSerde: Serde[Short] = JSerdes.Short().asInstanceOf[Serde[Short]]
  implicit def javaShortSerde: Serde[java.lang.Short] = JSerdes.Short()
  implicit def floatSerde: Serde[Float] = JSerdes.Float().asInstanceOf[Serde[Float]]
  implicit def javaFloatSerde: Serde[java.lang.Float] = JSerdes.Float()
  implicit def doubleSerde: Serde[Double] = JSerdes.Double().asInstanceOf[Serde[Double]]
  implicit def javaDoubleSerde: Serde[java.lang.Double] = JSerdes.Double()
  implicit def intSerde: Serde[Int] = JSerdes.Integer().asInstanceOf[Serde[Int]]
  implicit def javaIntegerSerde: Serde[java.lang.Integer] = JSerdes.Integer()
  implicit def uuidSerde: Serde[UUID] = JSerdes.UUID()

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

trait LowPrioritySerdes {

  implicit val nullSerde: Serde[Null] = {
    Serdes.fromFn[Null](
      { _: Null =>
        null
      }, { _: Array[Byte] =>
        None
      }
    )
  }
}
