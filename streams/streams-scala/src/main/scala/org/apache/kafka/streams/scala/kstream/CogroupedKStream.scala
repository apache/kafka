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

import org.apache.kafka.streams.kstream.{
  SessionWindows,
  SlidingWindows,
  Window,
  Windows,
  CogroupedKStream => CogroupedKStreamJ
}
import org.apache.kafka.streams.scala.FunctionsCompatConversions.{AggregatorFromFunction, InitializerFromFunction}

/**
 * Wraps the Java class CogroupedKStream and delegates method calls to the underlying Java object.
 *
 * @tparam KIn  Type of keys
 * @tparam VOut Type of values
 * @param inner The underlying Java abstraction for CogroupedKStream
 * @see `org.apache.kafka.streams.kstream.CogroupedKStream`
 */
class CogroupedKStream[KIn, VOut](val inner: CogroupedKStreamJ[KIn, VOut]) {

  /**
   * Add an already [[KGroupedStream]] to this [[CogroupedKStream]].
   *
   * @param groupedStream a group stream
   * @param aggregator    a function that computes a new aggregate result
   * @return a [[CogroupedKStream]]
   */
  def cogroup[VIn](
    groupedStream: KGroupedStream[KIn, VIn],
    aggregator: (KIn, VIn, VOut) => VOut
  ): CogroupedKStream[KIn, VOut] =
    new CogroupedKStream(inner.cogroup(groupedStream.inner, aggregator.asAggregator))

  /**
   * Aggregate the values of records in these streams by the grouped key and defined window.
   *
   * @param initializer  an `Initializer` that computes an initial intermediate aggregation result.
   *                     Cannot be { @code null}.
   * @param materialized an instance of `Materialized` used to materialize a state store.
   *                     Cannot be { @code null}.
   * @return a [[KTable]] that contains "update" records with unmodified keys, and values that represent the latest
   *         (rolling) aggregate for each key
   * @see `org.apache.kafka.streams.kstream.CogroupedKStream#aggregate`
   */
  def aggregate(initializer: => VOut)(implicit
    materialized: Materialized[KIn, VOut, ByteArrayKeyValueStore]
  ): KTable[KIn, VOut] = new KTable(inner.aggregate((() => initializer).asInitializer, materialized))

  /**
   * Aggregate the values of records in these streams by the grouped key and defined window.
   *
   * @param initializer  an `Initializer` that computes an initial intermediate aggregation result.
   *                     Cannot be { @code null}.
   * @param named        a [[Named]] config used to name the processor in the topology
   * @param materialized an instance of `Materialized` used to materialize a state store.
   *                     Cannot be { @code null}.
   * @return a [[KTable]] that contains "update" records with unmodified keys, and values that represent the latest
   *         (rolling) aggregate for each key
   * @see `org.apache.kafka.streams.kstream.CogroupedKStream#aggregate`
   */
  def aggregate(initializer: => VOut, named: Named)(implicit
    materialized: Materialized[KIn, VOut, ByteArrayKeyValueStore]
  ): KTable[KIn, VOut] = new KTable(inner.aggregate((() => initializer).asInitializer, named, materialized))

  /**
   * Create a new [[TimeWindowedCogroupedKStream]] instance that can be used to perform windowed aggregations.
   *
   * @param windows the specification of the aggregation `Windows`
   * @return an instance of [[TimeWindowedCogroupedKStream]]
   * @see `org.apache.kafka.streams.kstream.CogroupedKStream#windowedBy`
   */
  def windowedBy[W <: Window](windows: Windows[W]): TimeWindowedCogroupedKStream[KIn, VOut] =
    new TimeWindowedCogroupedKStream(inner.windowedBy(windows))

  /**
   * Create a new [[TimeWindowedCogroupedKStream]] instance that can be used to perform sliding windowed aggregations.
   *
   * @param windows the specification of the aggregation `SlidingWindows`
   * @return an instance of [[TimeWindowedCogroupedKStream]]
   * @see `org.apache.kafka.streams.kstream.CogroupedKStream#windowedBy`
   */
  def windowedBy(windows: SlidingWindows): TimeWindowedCogroupedKStream[KIn, VOut] =
    new TimeWindowedCogroupedKStream(inner.windowedBy(windows))

  /**
   * Create a new [[SessionWindowedKStream]] instance that can be used to perform session windowed aggregations.
   *
   * @param windows the specification of the aggregation `SessionWindows`
   * @return an instance of [[SessionWindowedKStream]]
   * @see `org.apache.kafka.streams.kstream.KGroupedStream#windowedBy`
   */
  def windowedBy(windows: SessionWindows): SessionWindowedCogroupedKStream[KIn, VOut] =
    new SessionWindowedCogroupedKStream(inner.windowedBy(windows))

}
