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

import org.apache.kafka.streams.kstream.{KGroupedTable => KGroupedTableJ, _}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.FunctionConversions._

/**
 * Wraps the Java class KGroupedTable and delegates method calls to the underlying Java object.
 *
 * @param [K] Type of keys
 * @param [V] Type of values
 * @param inner The underlying Java abstraction for KGroupedTable
 *
 * @see `org.apache.kafka.streams.kstream.KGroupedTable`
 */
class KGroupedTable[K, V](inner: KGroupedTableJ[K, V]) {

  /**
   * Count number of records of the original [[KTable]] that got [[KTable#groupBy]] to
   * the same key into a new instance of [[KTable]].
   *
   * @return a [[KTable]] that contains "update" records with unmodified keys and `Long` values that
   * represent the latest (rolling) count (i.e., number of records) for each key
   * @see `org.apache.kafka.streams.kstream.KGroupedTable#count`
   */
  def count(): KTable[K, Long] = {
    val c: KTable[K, java.lang.Long] = inner.count()
    c.mapValues[Long](Long2long _)
  }

  /**
   * Count number of records of the original [[KTable]] that got [[KTable#groupBy]] to
   * the same key into a new instance of [[KTable]].
   *
   * @param materialized  an instance of `Materialized` used to materialize a state store. 
   * @return a [[KTable]] that contains "update" records with unmodified keys and `Long` values that
   * represent the latest (rolling) count (i.e., number of records) for each key
   * @see `org.apache.kafka.streams.kstream.KGroupedTable#count`
   */
  def count(materialized: Materialized[K, Long, ByteArrayKeyValueStore]): KTable[K, Long] = {
    val c: KTable[K, java.lang.Long] =
      inner.count(materialized.asInstanceOf[Materialized[K, java.lang.Long, ByteArrayKeyValueStore]])
    c.mapValues[Long](Long2long _)
  }

  /**
   * Combine the value of records of the original [[KTable]] that got [[KTable#groupBy]]
   * to the same key into a new instance of [[KTable]].
   *
   * @param adder      a function that adds a new value to the aggregate result
   * @param subtractor a function that removed an old value from the aggregate result
   * @return a [[KTable]] that contains "update" records with unmodified keys, and values that represent the
   * latest (rolling) aggregate for each key
   * @see `org.apache.kafka.streams.kstream.KGroupedTable#reduce`
   */
  def reduce(adder: (V, V) => V,
             subtractor: (V, V) => V): KTable[K, V] =
    // need this explicit asReducer for Scala 2.11 or else the SAM conversion doesn't take place
    // works perfectly with Scala 2.12 though
    inner.reduce(adder.asReducer, subtractor.asReducer)

  /**
   * Combine the value of records of the original [[KTable]] that got [[KTable#groupBy]]
   * to the same key into a new instance of [[KTable]].
   *
   * @param adder      a function that adds a new value to the aggregate result
   * @param subtractor a function that removed an old value from the aggregate result
   * @param materialized  an instance of `Materialized` used to materialize a state store. 
   * @return a [[KTable]] that contains "update" records with unmodified keys, and values that represent the
   * latest (rolling) aggregate for each key
   * @see `org.apache.kafka.streams.kstream.KGroupedTable#reduce`
   */
  def reduce(adder: (V, V) => V,
             subtractor: (V, V) => V,
             materialized: Materialized[K, V, ByteArrayKeyValueStore]): KTable[K, V] =
    // need this explicit asReducer for Scala 2.11 or else the SAM conversion doesn't take place
    // works perfectly with Scala 2.12 though
    inner.reduce(adder.asReducer, subtractor.asReducer, materialized)

  /**
   * Aggregate the value of records of the original [[KTable]] that got [[KTable#groupBy]]
   * to the same key into a new instance of [[KTable]] using default serializers and deserializers.
   *
   * @param initializer a function that provides an initial aggregate result value
   * @param adder       a function that adds a new record to the aggregate result
   * @param subtractor  an aggregator function that removed an old record from the aggregate result
   * @return a [[KTable]] that contains "update" records with unmodified keys, and values that represent the
   * latest (rolling) aggregate for each key
   * @see `org.apache.kafka.streams.kstream.KGroupedTable#aggregate`
   */
  def aggregate[VR](initializer: () => VR,
                    adder: (K, V, VR) => VR,
                    subtractor: (K, V, VR) => VR): KTable[K, VR] =
    inner.aggregate(initializer.asInitializer, adder.asAggregator, subtractor.asAggregator)

  /**
   * Aggregate the value of records of the original [[KTable]] that got [[KTable#groupBy]]
   * to the same key into a new instance of [[KTable]] using default serializers and deserializers.
   *
   * @param initializer a function that provides an initial aggregate result value
   * @param adder       a function that adds a new record to the aggregate result
   * @param subtractor  an aggregator function that removed an old record from the aggregate result
   * @param materialized  an instance of `Materialized` used to materialize a state store. 
   * @return a [[KTable]] that contains "update" records with unmodified keys, and values that represent the
   * latest (rolling) aggregate for each key
   * @see `org.apache.kafka.streams.kstream.KGroupedTable#aggregate`
   */
  def aggregate[VR](initializer: () => VR,
                    adder: (K, V, VR) => VR,
                    subtractor: (K, V, VR) => VR,
                    materialized: Materialized[K, VR, ByteArrayKeyValueStore]): KTable[K, VR] =
    inner.aggregate(initializer.asInitializer, adder.asAggregator, subtractor.asAggregator, materialized)
}
