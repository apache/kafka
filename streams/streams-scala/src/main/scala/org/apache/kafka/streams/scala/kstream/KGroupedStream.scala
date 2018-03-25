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
package kstream

import org.apache.kafka.streams.kstream.{KGroupedStream => KGroupedStreamJ, _}
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.FunctionConversions._


/**
 * Wraps the Java class KGroupedStream and delegates method calls to the underlying Java object.
 */
class KGroupedStream[K, V](inner: KGroupedStreamJ[K, V]) {

  def count(): KTable[K, Long] = {
    val c: KTable[K, java.lang.Long] = inner.count()
    c.mapValues[Long](Long2long _)
  }

  def count(store: String, keySerde: Option[Serde[K]] = None): KTable[K, Long] = {
    val materialized = keySerde.foldLeft(Materialized.as[K, java.lang.Long, ByteArrayKVStore](store))((m,serde)=> m.withKeySerde(serde))

    val c: KTable[K, java.lang.Long] = inner.count(materialized)
    c.mapValues[Long](Long2long _)
  }

  def reduce(reducer: (V, V) => V): KTable[K, V] = {
    inner.reduce((v1, v2) => reducer(v1, v2))
  }

  def reduce(reducer: (V, V) => V,
    materialized: Materialized[K, V, ByteArrayKVStore]): KTable[K, V] = {

    // need this explicit asReducer for Scala 2.11 or else the SAM conversion doesn't take place
    // works perfectly with Scala 2.12 though
    inner.reduce(((v1: V, v2: V) => reducer(v1, v2)).asReducer, materialized)
  }

  def reduce(reducer: (V, V) => V,
    storeName: String)(implicit keySerde: Serde[K], valueSerde: Serde[V]): KTable[K, V] = {

    // need this explicit asReducer for Scala 2.11 or else the SAM conversion doesn't take place
    // works perfectly with Scala 2.12 though
    inner.reduce(((v1: V, v2: V) =>
      reducer(v1, v2)).asReducer,
      Materialized.as[K, V, ByteArrayKVStore](storeName)
        .withKeySerde(keySerde)
        .withValueSerde(valueSerde)
    )
  }

  def aggregate[VR](initializer: () => VR,
    aggregator: (K, V, VR) => VR): KTable[K, VR] = {
    inner.aggregate(initializer.asInitializer, aggregator.asAggregator)
  }

  def aggregate[VR](initializer: () => VR,
    aggregator: (K, V, VR) => VR,
    materialized: Materialized[K, VR, ByteArrayKVStore]): KTable[K, VR] = {
    inner.aggregate(initializer.asInitializer, aggregator.asAggregator, materialized)
  }

  def windowedBy(windows: SessionWindows): SessionWindowedKStream[K, V] =
    inner.windowedBy(windows)

  def windowedBy[W <: Window](windows: Windows[W]): TimeWindowedKStream[K, V] =
    inner.windowedBy(windows)
}
