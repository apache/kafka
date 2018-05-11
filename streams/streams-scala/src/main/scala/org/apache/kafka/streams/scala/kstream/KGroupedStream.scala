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
package kstream

import org.apache.kafka.streams.kstream.{KGroupedStream => KGroupedStreamJ, _}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.FunctionConversions._


/**
 * Wraps the Java class KGroupedStream and delegates method calls to the underlying Java object.
 *
 * @param [K] Type of keys
 * @param [V] Type of values
 * @param inner The underlying Java abstraction for KGroupedStream
 *
 * @see `org.apache.kafka.streams.kstream.KGroupedStream`
 */
class KGroupedStream[K, V](val inner: KGroupedStreamJ[K, V]) {

  /**
   * Count the number of records in this stream by the grouped key.
   *
   * @return a [[KTable]] that contains "update" records with unmodified keys and `Long` values that
   * represent the latest (rolling) count (i.e., number of records) for each key
   * @see `org.apache.kafka.streams.kstream.KGroupedStream#count`
   */ 
  def count(): KTable[K, Long] = {
    val c: KTable[K, java.lang.Long] = inner.count()
    c.mapValues[Long](Long2long _)
  }

  /**
   * Count the number of records in this stream by the grouped key.
   * The result is written into a local `KeyValueStore` (which is basically an ever-updating materialized view)
   * provided by the given `materialized`.
   *
   * @param materialized  an instance of `Materialized` used to materialize a state store. 
   * @return a [[KTable]] that contains "update" records with unmodified keys and `Long` values that
   * represent the latest (rolling) count (i.e., number of records) for each key
   * @see `org.apache.kafka.streams.kstream.KGroupedStream#count`
   */ 
  def count(materialized: Materialized[K, Long, ByteArrayKeyValueStore]): KTable[K, Long] = {
    val c: KTable[K, java.lang.Long] =
      inner.count(materialized.asInstanceOf[Materialized[K, java.lang.Long, ByteArrayKeyValueStore]])
    c.mapValues[Long](Long2long _)
  }

  /**
   * Combine the values of records in this stream by the grouped key.
   *
   * @param reducer   a function `(V, V) => V` that computes a new aggregate result. 
   * @return a [[KTable]] that contains "update" records with unmodified keys, and values that represent the
   * latest (rolling) aggregate for each key
   * @see `org.apache.kafka.streams.kstream.KGroupedStream#reduce`
   */ 
  def reduce(reducer: (V, V) => V): KTable[K, V] =
    inner.reduce(reducer.asReducer)

  /**
   * Combine the values of records in this stream by the grouped key.
   *
   * @param reducer   a function `(V, V) => V` that computes a new aggregate result. 
   * @param materialized  an instance of `Materialized` used to materialize a state store. 
   * @return a [[KTable]] that contains "update" records with unmodified keys, and values that represent the
   * latest (rolling) aggregate for each key
   * @see `org.apache.kafka.streams.kstream.KGroupedStream#reduce`
   */ 
  def reduce(reducer: (V, V) => V,
    materialized: Materialized[K, V, ByteArrayKeyValueStore]): KTable[K, V] = {

    // need this explicit asReducer for Scala 2.11 or else the SAM conversion doesn't take place
    // works perfectly with Scala 2.12 though
    inner.reduce(((v1: V, v2: V) => reducer(v1, v2)).asReducer, materialized)
  }

  /**
   * Aggregate the values of records in this stream by the grouped key.
   *
   * @param initializer   an `Initializer` that computes an initial intermediate aggregation result
   * @param aggregator    an `Aggregator` that computes a new aggregate result
   * @return a [[KTable]] that contains "update" records with unmodified keys, and values that represent the
   * latest (rolling) aggregate for each key
   * @see `org.apache.kafka.streams.kstream.KGroupedStream#aggregate`
   */ 
  def aggregate[VR](initializer: () => VR,
    aggregator: (K, V, VR) => VR): KTable[K, VR] =
    inner.aggregate(initializer.asInitializer, aggregator.asAggregator)

  /**
   * Aggregate the values of records in this stream by the grouped key.
   *
   * @param initializer   an `Initializer` that computes an initial intermediate aggregation result
   * @param aggregator    an `Aggregator` that computes a new aggregate result
   * @param materialized  an instance of `Materialized` used to materialize a state store. 
   * @return a [[KTable]] that contains "update" records with unmodified keys, and values that represent the
   * latest (rolling) aggregate for each key
   * @see `org.apache.kafka.streams.kstream.KGroupedStream#aggregate`
   */ 
  def aggregate[VR](initializer: () => VR,
    aggregator: (K, V, VR) => VR,
    materialized: Materialized[K, VR, ByteArrayKeyValueStore]): KTable[K, VR] =
    inner.aggregate(initializer.asInitializer, aggregator.asAggregator, materialized)

  /**
   * Create a new [[SessionWindowedKStream]] instance that can be used to perform session windowed aggregations.
   *
   * @param windows the specification of the aggregation `SessionWindows`
   * @return an instance of [[SessionWindowedKStream]]
   * @see `org.apache.kafka.streams.kstream.KGroupedStream#windowedBy`
   */
  def windowedBy(windows: SessionWindows): SessionWindowedKStream[K, V] =
    inner.windowedBy(windows)

  /**
   * Create a new [[TimeWindowedKStream]] instance that can be used to perform windowed aggregations.
   *
   * @param windows the specification of the aggregation `Windows`
   * @return an instance of [[TimeWindowedKStream]]
   * @see `org.apache.kafka.streams.kstream.KGroupedStream#windowedBy`
   */
  def windowedBy[W <: Window](windows: Windows[W]): TimeWindowedKStream[K, V] =
    inner.windowedBy(windows)
}
