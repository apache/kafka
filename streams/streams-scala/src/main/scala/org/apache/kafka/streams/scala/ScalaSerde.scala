// adopted from Openshine implementation
package org.apache.kafka.streams.scala

import org.apache.kafka.common.serialization.{Serde, Deserializer => JDeserializer, Serializer => JSerializer}

trait ScalaSerde[T] extends Serde[T] {
  override def deserializer(): JDeserializer[T]

  override def serializer(): JSerializer[T]

  override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = ()

  override def close(): Unit = ()
}

trait StatelessScalaSerde[T >: Null] extends Serde[T] with ScalaSerde[T] {
  def serialize(data: T): Array[Byte]
  def deserialize(data: Array[Byte]): Option[T]

  override def deserializer(): Deserializer[T] =
    (data: Array[Byte]) => deserialize(data)

  override def serializer(): Serializer[T] =
    (data: T) => serialize(data)
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
