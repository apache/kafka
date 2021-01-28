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

import org.apache.kafka.streams.kstream.{KGroupedTable => KGroupedTableJ}
import org.apache.kafka.streams.scala.FunctionsCompatConversions.{
  AggregatorFromFunction,
  InitializerFromFunction,
  ReducerFromFunction
}

/**
 * Wraps the Java class KGroupedTable and delegates method calls to the underlying Java object.
 *
 * @tparam K Type of keys
 * @tparam V Type of values
 * @param inner The underlying Java abstraction for KGroupedTable
 * @see `org.apache.kafka.streams.kstream.KGroupedTable`
 */
class KGroupedTable[K, V](inner: KGroupedTableJ[K, V]) {

  /**
   * Count number of records of the original [[KTable]] that got [[KTable#groupBy]] to
   * the same key into a new instance of [[KTable]].
   *
   * @param materialized an instance of `Materialized` used to materialize a state store.
   * @return a [[KTable]] that contains "update" records with unmodified keys and `Long` values that
   *         represent the latest (rolling) count (i.e., number of records) for each key
   * @see `org.apache.kafka.streams.kstream.KGroupedTable#count`
   */
  def count()(implicit materialized: Materialized[K, Long, ByteArrayKeyValueStore]): KTable[K, Long] = {
    val c: KTable[K, java.lang.Long] =
      new KTable(inner.count(materialized.asInstanceOf[Materialized[K, java.lang.Long, ByteArrayKeyValueStore]]))
    c.mapValues[Long](Long2long _)
  }

  /**
   * Count number of records of the original [[KTable]] that got [[KTable#groupBy]] to
   * the same key into a new instance of [[KTable]].
   *
   * @param named        a [[Named]] config used to name the processor in the topology
   * @param materialized an instance of `Materialized` used to materialize a state store.
   * @return a [[KTable]] that contains "update" records with unmodified keys and `Long` values that
   *         represent the latest (rolling) count (i.e., number of records) for each key
   * @see `org.apache.kafka.streams.kstream.KGroupedTable#count`
   */
  def count(named: Named)(implicit materialized: Materialized[K, Long, ByteArrayKeyValueStore]): KTable[K, Long] = {
    val c: KTable[K, java.lang.Long] =
      new KTable(inner.count(named, materialized.asInstanceOf[Materialized[K, java.lang.Long, ByteArrayKeyValueStore]]))
    c.mapValues[Long](Long2long _)
  }

  /**
   * Combine the value of records of the original [[KTable]] that got [[KTable#groupBy]]
   * to the same key into a new instance of [[KTable]].
   *
   * @param adder        a function that adds a new value to the aggregate result
   * @param subtractor   a function that removed an old value from the aggregate result
   * @param materialized an instance of `Materialized` used to materialize a state store.
   * @return a [[KTable]] that contains "update" records with unmodified keys, and values that represent the
   *         latest (rolling) aggregate for each key
   * @see `org.apache.kafka.streams.kstream.KGroupedTable#reduce`
   */
  def reduce(adder: (V, V) => V,
             subtractor: (V, V) => V)(implicit materialized: Materialized[K, V, ByteArrayKeyValueStore]): KTable[K, V] =
    new KTable(inner.reduce(adder.asReducer, subtractor.asReducer, materialized))

  /**
   * Combine the value of records of the original [[KTable]] that got [[KTable#groupBy]]
   * to the same key into a new instance of [[KTable]].
   *
   * @param adder        a function that adds a new value to the aggregate result
   * @param subtractor   a function that removed an old value from the aggregate result
   * @param named        a [[Named]] config used to name the processor in the topology
   * @param materialized an instance of `Materialized` used to materialize a state store.
   * @return a [[KTable]] that contains "update" records with unmodified keys, and values that represent the
   *         latest (rolling) aggregate for each key
   * @see `org.apache.kafka.streams.kstream.KGroupedTable#reduce`
   */
  def reduce(adder: (V, V) => V, subtractor: (V, V) => V, named: Named)(
    implicit materialized: Materialized[K, V, ByteArrayKeyValueStore]
  ): KTable[K, V] =
    new KTable(inner.reduce(adder.asReducer, subtractor.asReducer, named, materialized))

  /**
   * Aggregate the value of records of the original [[KTable]] that got [[KTable#groupBy]]
   * to the same key into a new instance of [[KTable]] using default serializers and deserializers.
   *
   * @param initializer  a function that provides an initial aggregate result value
   * @param adder        a function that adds a new record to the aggregate result
   * @param subtractor   an aggregator function that removed an old record from the aggregate result
   * @param materialized an instance of `Materialized` used to materialize a state store.
   * @return a [[KTable]] that contains "update" records with unmodified keys, and values that represent the
   *         latest (rolling) aggregate for each key
   * @see `org.apache.kafka.streams.kstream.KGroupedTable#aggregate`
   */
  def aggregate[VR](initializer: => VR)(adder: (K, V, VR) => VR, subtractor: (K, V, VR) => VR)(
    implicit materialized: Materialized[K, VR, ByteArrayKeyValueStore]
  ): KTable[K, VR] =
    new KTable(
      inner.aggregate((() => initializer).asInitializer, adder.asAggregator, subtractor.asAggregator, materialized)
    )

  /**
   * Aggregate the value of records of the original [[KTable]] that got [[KTable#groupBy]]
   * to the same key into a new instance of [[KTable]] using default serializers and deserializers.
   *
   * @param initializer  a function that provides an initial aggregate result value
   * @param named        a [[Named]] config used to name the processor in the topology
   * @param adder        a function that adds a new record to the aggregate result
   * @param subtractor   an aggregator function that removed an old record from the aggregate result
   * @param materialized an instance of `Materialized` used to materialize a state store.
   * @return a [[KTable]] that contains "update" records with unmodified keys, and values that represent the
   *         latest (rolling) aggregate for each key
   * @see `org.apache.kafka.streams.kstream.KGroupedTable#aggregate`
   */
  def aggregate[VR](initializer: => VR, named: Named)(adder: (K, V, VR) => VR, subtractor: (K, V, VR) => VR)(
    implicit materialized: Materialized[K, VR, ByteArrayKeyValueStore]
  ): KTable[K, VR] =
    new KTable(
      inner.aggregate((() => initializer).asInitializer,
                      adder.asAggregator,
                      subtractor.asAggregator,
                      named,
                      materialized)
    )
}
