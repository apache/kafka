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
package org.apache.kafka.streams.scala.kstream

import org.apache.kafka.streams.kstream.{TimeWindowedKStream => TimeWindowedKStreamJ, _}
import org.apache.kafka.streams.state.WindowStore
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.FunctionConversions._

/**
 * Wraps the Java class TimeWindowedKStream and delegates method calls to the underlying Java object.
 */
class TimeWindowedKStream[K, V](val inner: TimeWindowedKStreamJ[K, V]) {

  def aggregate[VR](initializer: () => VR,
    aggregator: (K, V, VR) => VR): KTable[Windowed[K], VR] = {

    inner.aggregate(initializer.asInitializer, aggregator.asAggregator)
  }

  def aggregate[VR](initializer: () => VR,
    aggregator: (K, V, VR) => VR,
    materialized: Materialized[K, VR, WindowStore[Bytes, Array[Byte]]]): KTable[Windowed[K], VR] = {

    inner.aggregate(initializer.asInitializer, aggregator.asAggregator, materialized)
  }

  def count(): KTable[Windowed[K], Long] = {
    val c: KTable[Windowed[K], java.lang.Long] = inner.count()
    c.mapValues[Long](Long2long(_))
  }

  def count(store: String, keySerde: Option[Serde[K]] = None): KTable[Windowed[K], Long] = {
    val materialized = {
      val m = Materialized.as[K, java.lang.Long, WindowStore[Bytes, Array[Byte]]](store)
      keySerde.foldLeft(m)((m,serde)=> m.withKeySerde(serde))
    }
    val c: KTable[Windowed[K], java.lang.Long] = inner.count(materialized)
    c.mapValues[Long](Long2long(_))
  }

  def reduce(reducer: (V, V) => V): KTable[Windowed[K], V] = {
    inner.reduce(reducer.asReducer)
  }

  def reduce(reducer: (V, V) => V,
    materialized: Materialized[K, V, WindowStore[Bytes, Array[Byte]]]): KTable[Windowed[K], V] = {

    inner.reduce(reducer.asReducer, materialized)
  }
}
