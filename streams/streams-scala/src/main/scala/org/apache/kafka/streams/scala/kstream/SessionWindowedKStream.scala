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

import org.apache.kafka.streams.kstream.{SessionWindowedKStream => SessionWindowedKStreamJ, _}
import org.apache.kafka.streams.state.SessionStore
import org.apache.kafka.common.utils.Bytes

import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.FunctionConversions._

/**
 * Wraps the Java class SessionWindowedKStream and delegates method calls to the underlying Java object.
 */
class SessionWindowedKStream[K, V](val inner: SessionWindowedKStreamJ[K, V]) {

  def aggregate[VR](initializer: () => VR,
    aggregator: (K, V, VR) => VR,
    merger: (K, VR, VR) => VR): KTable[Windowed[K], VR] = {

    inner.aggregate(initializer.asInitializer, aggregator.asAggregator, merger.asMerger)
  }

  def aggregate[VR](initializer: () => VR,
    aggregator: (K, V, VR) => VR,
    merger: (K, VR, VR) => VR,
    materialized: Materialized[K, VR, SessionStore[Bytes, Array[Byte]]]): KTable[Windowed[K], VR] = {

    inner.aggregate(initializer.asInitializer, aggregator.asAggregator, merger.asMerger, materialized)
  }

  def count(): KTable[Windowed[K], Long] = {
    val c: KTable[Windowed[K], java.lang.Long] = inner.count()
    c.mapValues[Long](Long2long(_))
  }

  def count(materialized: Materialized[K, Long, SessionStore[Bytes, Array[Byte]]]): KTable[Windowed[K], Long] =
    inner.count(materialized)

  def reduce(reducer: (V, V) => V): KTable[Windowed[K], V] = {
    inner.reduce((v1, v2) => reducer(v1, v2))
  }

  def reduce(reducer: (V, V) => V,
    materialized: Materialized[K, V, SessionStore[Bytes, Array[Byte]]]): KTable[Windowed[K], V] = {
    inner.reduce(reducer.asReducer, materialized)
  }
}
