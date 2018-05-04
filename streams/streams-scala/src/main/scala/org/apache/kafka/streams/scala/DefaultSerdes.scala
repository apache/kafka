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

import java.nio.ByteBuffer

import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.kstream.WindowedSerdes


/**
 * Implicit values for default serdes.
 * <p>
 * Bring them in scope for default serializers / de-serializers to work.
 */
object DefaultSerdes {
  implicit val stringSerde: Serde[String] = Serdes.String()
  implicit val longSerde: Serde[Long] = Serdes.Long().asInstanceOf[Serde[Long]]
  implicit val byteArraySerde: Serde[Array[Byte]] = Serdes.ByteArray()
  implicit val bytesSerde: Serde[Bytes] = Serdes.Bytes()
  implicit val floatSerde: Serde[Float] = Serdes.Float().asInstanceOf[Serde[Float]]
  implicit val doubleSerde: Serde[Double] = Serdes.Double().asInstanceOf[Serde[Double]]
  implicit val integerSerde: Serde[Int] = Serdes.Integer().asInstanceOf[Serde[Int]]
  implicit val shortSerde: Serde[Short] = Serdes.Short().asInstanceOf[Serde[Short]]
  implicit val byteBufferSerde: Serde[ByteBuffer] = Serdes.ByteBuffer()

  implicit def timeWindowedSerde[T]: WindowedSerdes.TimeWindowedSerde[T] = new WindowedSerdes.TimeWindowedSerde[T]()
  implicit def sessionWindowedSerde[T]: WindowedSerdes.SessionWindowedSerde[T] = new WindowedSerdes.SessionWindowedSerde[T]()
}
