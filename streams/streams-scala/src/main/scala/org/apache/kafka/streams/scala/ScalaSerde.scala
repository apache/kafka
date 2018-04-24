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

import org.apache.kafka.common.serialization.{Serde, Deserializer => JDeserializer, Serializer => JSerializer}

trait ScalaSerde[T] extends Serde[T] {
  override def deserializer(): JDeserializer[T]

  override def serializer(): JSerializer[T]

  override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = ()

  override def close(): Unit = ()
}

trait SimpleScalaSerde[T >: Null] extends Serde[T] with ScalaSerde[T] {
  def serialize(data: T): Array[Byte]
  def deserialize(data: Array[Byte]): Option[T]

  private def outerSerialize(data: T): Array[Byte] = serialize(data)
  private def outerDeserialize(data: Array[Byte]): Option[T] = deserialize(data)

  override def deserializer(): Deserializer[T] = new Deserializer[T] {
    override def deserialize(data: Array[Byte]): Option[T] = outerDeserialize(data)
  }

  override def serializer(): Serializer[T] = new Serializer[T] {
    override def serialize(data: T): Array[Byte] = outerSerialize(data)
  }
}

trait Deserializer[T >: Null] extends JDeserializer[T] {
  override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = ()

  override def close(): Unit = ()

  override def deserialize(topic: String, data: Array[Byte]): T =
    Option(data).flatMap(deserialize).orNull

  def deserialize(data: Array[Byte]): Option[T]
}

trait Serializer[T] extends JSerializer[T] {
  override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = ()

  override def close(): Unit = ()

  override def serialize(topic: String, data: T): Array[Byte] =
    Option(data).map(serialize).orNull

  def serialize(data: T): Array[Byte]
}
