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

import org.apache.kafka.streams.kstream.{TimeWindowedKStream => TimeWindowedKStreamJ, _}
import org.apache.kafka.streams.state.WindowStore
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.FunctionConversions._

/**
 * Wraps the Java class TimeWindowedKStream and delegates method calls to the underlying Java object.
 *
 * @param [K] Type of keys
 * @param [V] Type of values
 * @param inner The underlying Java abstraction for TimeWindowedKStream
 *
 * @see `org.apache.kafka.streams.kstream.TimeWindowedKStream`
 */
class TimeWindowedKStream[K, V](val inner: TimeWindowedKStreamJ[K, V]) {

  /**
   * Aggregate the values of records in this stream by the grouped key.
   *
   * @param initializer   an initializer function that computes an initial intermediate aggregation result
   * @param aggregator    an aggregator function that computes a new aggregate result
   * @return a [[KTable]] that contains "update" records with unmodified keys, and values that represent the
   * latest (rolling) aggregate for each key
   * @see `org.apache.kafka.streams.kstream.TimeWindowedKStream#aggregate`
   */
  def aggregate[VR](initializer: () => VR,
    aggregator: (K, V, VR) => VR): KTable[Windowed[K], VR] = {

    inner.aggregate(initializer.asInitializer, aggregator.asAggregator)
  }

  /**
   * Aggregate the values of records in this stream by the grouped key.
   *
   * @param initializer   an initializer function that computes an initial intermediate aggregation result
   * @param aggregator    an aggregator function that computes a new aggregate result
   * @param materialized  an instance of `Materialized` used to materialize a state store. 
   * @return a [[KTable]] that contains "update" records with unmodified keys, and values that represent the
   * latest (rolling) aggregate for each key
   * @see `org.apache.kafka.streams.kstream.TimeWindowedKStream#aggregate`
   */
  def aggregate[VR](initializer: () => VR,
    aggregator: (K, V, VR) => VR,
    materialized: Materialized[K, VR, WindowStore[Bytes, Array[Byte]]]): KTable[Windowed[K], VR] = {

    inner.aggregate(initializer.asInitializer, aggregator.asAggregator, materialized)
  }

  /**
   * Count the number of records in this stream by the grouped key and the defined windows.
   *
   * @return a [[KTable]] that contains "update" records with unmodified keys and `Long` values that
   * represent the latest (rolling) count (i.e., number of records) for each key
   * @see `org.apache.kafka.streams.kstream.TimeWindowedKStream#count`
   */ 
  def count(): KTable[Windowed[K], Long] = {
    val c: KTable[Windowed[K], java.lang.Long] = inner.count()
    c.mapValues[Long](Long2long(_))
  }

  /**
   * Count the number of records in this stream by the grouped key and the defined windows.
   *
   * @param materialized  an instance of `Materialized` used to materialize a state store. 
   * @return a [[KTable]] that contains "update" records with unmodified keys and `Long` values that
   * represent the latest (rolling) count (i.e., number of records) for each key
   * @see `org.apache.kafka.streams.kstream.TimeWindowedKStream#count`
   */ 
  def count(materialized: Materialized[K, Long, WindowStore[Bytes, Array[Byte]]]): KTable[Windowed[K], Long] = {
    val c: KTable[Windowed[K], java.lang.Long] = 
      inner.count(materialized.asInstanceOf[Materialized[K, java.lang.Long, WindowStore[Bytes, Array[Byte]]]])
    c.mapValues[Long](Long2long(_))
  }

  /**
   * Combine the values of records in this stream by the grouped key.
   *
   * @param reducer   a function that computes a new aggregate result
   * @return a [[KTable]] that contains "update" records with unmodified keys, and values that represent the
   * latest (rolling) aggregate for each key
   * @see `org.apache.kafka.streams.kstream.TimeWindowedKStream#reduce`
   */
  def reduce(reducer: (V, V) => V): KTable[Windowed[K], V] = {
    inner.reduce(reducer.asReducer)
  }

  /**
   * Combine the values of records in this stream by the grouped key.
   *
   * @param reducer   a function that computes a new aggregate result
   * @param materialized  an instance of `Materialized` used to materialize a state store. 
   * @return a [[KTable]] that contains "update" records with unmodified keys, and values that represent the
   * latest (rolling) aggregate for each key
   * @see `org.apache.kafka.streams.kstream.TimeWindowedKStream#reduce`
   */
  def reduce(reducer: (V, V) => V,
    materialized: Materialized[K, V, WindowStore[Bytes, Array[Byte]]]): KTable[Windowed[K], V] = {

    inner.reduce(reducer.asReducer, materialized)
  }
}
