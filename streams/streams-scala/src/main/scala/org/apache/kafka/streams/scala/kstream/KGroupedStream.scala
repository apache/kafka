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

import org.apache.kafka.streams.kstream.internals.KTableImpl
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.kstream.{
  SessionWindows,
  Window,
  Windows,
  KGroupedStream => KGroupedStreamJ,
  KTable => KTableJ
}
import org.apache.kafka.streams.scala.FunctionsCompatConversions.{
  AggregatorFromFunction,
  InitializerFromFunction,
  ReducerFromFunction,
  ValueMapperFromFunction
}

/**
 * Wraps the Java class KGroupedStream and delegates method calls to the underlying Java object.
 *
 * @tparam K Type of keys
 * @tparam V Type of values
 * @param inner The underlying Java abstraction for KGroupedStream
 * @see `org.apache.kafka.streams.kstream.KGroupedStream`
 */
class KGroupedStream[K, V](val inner: KGroupedStreamJ[K, V]) {

  /**
   * Count the number of records in this stream by the grouped key.
   * The result is written into a local `KeyValueStore` (which is basically an ever-updating materialized view)
   * provided by the given `materialized`.
   *
   * @param materialized an instance of `Materialized` used to materialize a state store.
   * @return a [[KTable]] that contains "update" records with unmodified keys and `Long` values that
   *         represent the latest (rolling) count (i.e., number of records) for each key
   * @see `org.apache.kafka.streams.kstream.KGroupedStream#count`
   */
  def count()(implicit materialized: Materialized[K, Long, ByteArrayKeyValueStore]): KTable[K, Long] = {
    val javaCountTable: KTableJ[K, java.lang.Long] =
      inner.count(materialized.asInstanceOf[Materialized[K, java.lang.Long, ByteArrayKeyValueStore]])
    val tableImpl = javaCountTable.asInstanceOf[KTableImpl[K, ByteArrayKeyValueStore, java.lang.Long]]
    new KTable(
      javaCountTable.mapValues[Long](
        ((l: java.lang.Long) => Long2long(l)).asValueMapper,
        Materialized.`with`[K, Long, ByteArrayKeyValueStore](tableImpl.keySerde(), Serdes.longSerde)
      )
    )
  }

  /**
   * Count the number of records in this stream by the grouped key.
   * The result is written into a local `KeyValueStore` (which is basically an ever-updating materialized view)
   * provided by the given `materialized`.
   *
   * @param named        a [[Named]] config used to name the processor in the topology
   * @param materialized an instance of `Materialized` used to materialize a state store.
   * @return a [[KTable]] that contains "update" records with unmodified keys and `Long` values that
   *         represent the latest (rolling) count (i.e., number of records) for each key
   * @see `org.apache.kafka.streams.kstream.KGroupedStream#count`
   */
  def count(named: Named)(implicit materialized: Materialized[K, Long, ByteArrayKeyValueStore]): KTable[K, Long] = {
    val javaCountTable: KTableJ[K, java.lang.Long] =
      inner.count(named, materialized.asInstanceOf[Materialized[K, java.lang.Long, ByteArrayKeyValueStore]])
    val tableImpl = javaCountTable.asInstanceOf[KTableImpl[K, ByteArrayKeyValueStore, java.lang.Long]]
    new KTable(
      javaCountTable.mapValues[Long](
        ((l: java.lang.Long) => Long2long(l)).asValueMapper,
        Materialized.`with`[K, Long, ByteArrayKeyValueStore](tableImpl.keySerde(), Serdes.longSerde)
      )
    )
  }

  /**
   * Combine the values of records in this stream by the grouped key.
   *
   * @param reducer      a function `(V, V) => V` that computes a new aggregate result.
   * @param materialized an instance of `Materialized` used to materialize a state store.
   * @return a [[KTable]] that contains "update" records with unmodified keys, and values that represent the
   *         latest (rolling) aggregate for each key
   * @see `org.apache.kafka.streams.kstream.KGroupedStream#reduce`
   */
  def reduce(reducer: (V, V) => V)(implicit materialized: Materialized[K, V, ByteArrayKeyValueStore]): KTable[K, V] =
    new KTable(inner.reduce(reducer.asReducer, materialized))

  /**
   * Combine the values of records in this stream by the grouped key.
   *
   * @param reducer      a function `(V, V) => V` that computes a new aggregate result.
   * @param named        a [[Named]] config used to name the processor in the topology
   * @param materialized an instance of `Materialized` used to materialize a state store.
   * @return a [[KTable]] that contains "update" records with unmodified keys, and values that represent the
   *         latest (rolling) aggregate for each key
   * @see `org.apache.kafka.streams.kstream.KGroupedStream#reduce`
   */
  def reduce(reducer: (V, V) => V,
             named: Named)(implicit materialized: Materialized[K, V, ByteArrayKeyValueStore]): KTable[K, V] =
    new KTable(inner.reduce(reducer.asReducer, materialized))

  /**
   * Aggregate the values of records in this stream by the grouped key.
   *
   * @param initializer  an `Initializer` that computes an initial intermediate aggregation result
   * @param aggregator   an `Aggregator` that computes a new aggregate result
   * @param materialized an instance of `Materialized` used to materialize a state store.
   * @return a [[KTable]] that contains "update" records with unmodified keys, and values that represent the
   *         latest (rolling) aggregate for each key
   * @see `org.apache.kafka.streams.kstream.KGroupedStream#aggregate`
   */
  def aggregate[VR](initializer: => VR)(aggregator: (K, V, VR) => VR)(
    implicit materialized: Materialized[K, VR, ByteArrayKeyValueStore]
  ): KTable[K, VR] =
    new KTable(inner.aggregate((() => initializer).asInitializer, aggregator.asAggregator, materialized))

  /**
   * Aggregate the values of records in this stream by the grouped key.
   *
   * @param initializer  an `Initializer` that computes an initial intermediate aggregation result
   * @param aggregator   an `Aggregator` that computes a new aggregate result
   * @param named        a [[Named]] config used to name the processor in the topology
   * @param materialized an instance of `Materialized` used to materialize a state store.
   * @return a [[KTable]] that contains "update" records with unmodified keys, and values that represent the
   *         latest (rolling) aggregate for each key
   * @see `org.apache.kafka.streams.kstream.KGroupedStream#aggregate`
   */
  def aggregate[VR](initializer: => VR, named: Named)(aggregator: (K, V, VR) => VR)(
    implicit materialized: Materialized[K, VR, ByteArrayKeyValueStore]
  ): KTable[K, VR] =
    new KTable(inner.aggregate((() => initializer).asInitializer, aggregator.asAggregator, named, materialized))

  /**
   * Create a new [[TimeWindowedKStream]] instance that can be used to perform windowed aggregations.
   *
   * @param windows the specification of the aggregation `Windows`
   * @return an instance of [[TimeWindowedKStream]]
   * @see `org.apache.kafka.streams.kstream.KGroupedStream#windowedBy`
   */
  def windowedBy[W <: Window](windows: Windows[W]): TimeWindowedKStream[K, V] =
    new TimeWindowedKStream(inner.windowedBy(windows))

  /**
   * Create a new [[SessionWindowedKStream]] instance that can be used to perform session windowed aggregations.
   *
   * @param windows the specification of the aggregation `SessionWindows`
   * @return an instance of [[SessionWindowedKStream]]
   * @see `org.apache.kafka.streams.kstream.KGroupedStream#windowedBy`
   */
  def windowedBy(windows: SessionWindows): SessionWindowedKStream[K, V] =
    new SessionWindowedKStream(inner.windowedBy(windows))

  /**
   * Create a new [[CogroupedKStream]] from this grouped KStream to allow cogrouping other [[KGroupedStream]] to it.
   *
   * @param aggregator an `Aggregator` that computes a new aggregate result
   * @return an instance of [[CogroupedKStream]]
   * @see `org.apache.kafka.streams.kstream.KGroupedStream#cogroup`
   */
  def cogroup[VR](aggregator: (K, V, VR) => VR): CogroupedKStream[K, VR] =
    new CogroupedKStream(inner.cogroup(aggregator.asAggregator))

}
