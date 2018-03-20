/**
 * Copyright (C) 2018 Lightbend Inc. <https://www.lightbend.com>
 */

package org.apache.kafka.streams.scala

import org.apache.kafka.common.serialization.{Serde, Serdes}


/**
 * Implicit values for default serdes
 */
object DefaultSerdes {
  implicit val stringSerde: Serde[String] = Serdes.String()
  implicit val longSerde: Serde[Long] = Serdes.Long().asInstanceOf[Serde[Long]]
  implicit val byteArraySerde: Serde[Array[Byte]] = Serdes.ByteArray()
  implicit val bytesSerde: Serde[org.apache.kafka.common.utils.Bytes] = Serdes.Bytes()
  implicit val floatSerde: Serde[Float] = Serdes.Float().asInstanceOf[Serde[Float]]
  implicit val doubleSerde: Serde[Double] = Serdes.Double().asInstanceOf[Serde[Double]]
  implicit val integerSerde: Serde[Int] = Serdes.Integer().asInstanceOf[Serde[Int]]
}
