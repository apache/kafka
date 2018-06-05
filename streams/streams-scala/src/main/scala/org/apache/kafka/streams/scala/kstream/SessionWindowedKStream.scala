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

import org.apache.kafka.streams.kstream.{SessionWindowedKStream => SessionWindowedKStreamJ, _}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.FunctionConversions._

/**
 * Wraps the Java class SessionWindowedKStream and delegates method calls to the underlying Java object.
 *
 * @param [K] Type of keys
 * @param [V] Type of values
 * @param inner The underlying Java abstraction for SessionWindowedKStream
 *
 * @see `org.apache.kafka.streams.kstream.SessionWindowedKStream`
 */
class SessionWindowedKStream[K, V](val inner: SessionWindowedKStreamJ[K, V]) {

  /**
   * Aggregate the values of records in this stream by the grouped key and defined `SessionWindows`.
   *
   * @param initializer    the initializer function
   * @param aggregator     the aggregator function
   * @param merger         the merger function
   * @param materialized  an instance of `Materialized` used to materialize a state store.
   * @return a windowed [[KTable]] that contains "update" records with unmodified keys, and values that represent
   * the latest (rolling) aggregate for each key within a window
   * @see `org.apache.kafka.streams.kstream.SessionWindowedKStream#aggregate`
   */
  def aggregate[VR](initializer: => VR)(aggregator: (K, V, VR) => VR,
                                        merger: (K, VR, VR) => VR)(
    implicit materialized: Materialized[K, VR, ByteArraySessionStore]
  ): KTable[Windowed[K], VR] =
    inner.aggregate((() => initializer).asInitializer, aggregator.asAggregator, merger.asMerger, materialized)

  /**
   * Count the number of records in this stream by the grouped key into `SessionWindows`.
   *
   * @param materialized  an instance of `Materialized` used to materialize a state store. 
   * @return a windowed [[KTable]] that contains "update" records with unmodified keys and `Long` values
   * that represent the latest (rolling) count (i.e., number of records) for each key within a window
   * @see `org.apache.kafka.streams.kstream.SessionWindowedKStream#count`
   */
  def count()(implicit materialized: Materialized[K, Long, ByteArraySessionStore]): KTable[Windowed[K], Long] = {
    val c: KTable[Windowed[K], java.lang.Long] =
      inner.count(materialized.asInstanceOf[Materialized[K, java.lang.Long, ByteArraySessionStore]])
    c.mapValues[Long](Long2long _)
  }

  /**
   * Combine values of this stream by the grouped key into {@link SessionWindows}.
   *
   * @param reducer           a reducer function that computes a new aggregate result. 
   * @param materialized  an instance of `Materialized` used to materialize a state store. 
   * @return a windowed [[KTable]] that contains "update" records with unmodified keys, and values that represent
   * the latest (rolling) aggregate for each key within a window
   * @see `org.apache.kafka.streams.kstream.SessionWindowedKStream#reduce`
   */
  def reduce(reducer: (V, V) => V)(
    implicit materialized: Materialized[K, V, ByteArraySessionStore]
  ): KTable[Windowed[K], V] =
    inner.reduce(reducer.asReducer, materialized)
}
