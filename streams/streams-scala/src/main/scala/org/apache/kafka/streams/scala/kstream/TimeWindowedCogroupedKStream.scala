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

import org.apache.kafka.streams.kstream.{TimeWindowedCogroupedKStream => TimeWindowedCogroupedKStreamJ, Windowed}
import org.apache.kafka.streams.scala.FunctionsCompatConversions.InitializerFromFunction

/**
 * Wraps the Java class TimeWindowedCogroupedKStream and delegates method calls to the underlying Java object.
 *
 * @tparam K Type of keys
 * @tparam V Type of values
 * @param inner The underlying Java abstraction for TimeWindowedCogroupedKStream
 * @see `org.apache.kafka.streams.kstream.TimeWindowedCogroupedKStream`
 */
class TimeWindowedCogroupedKStream[K, V](val inner: TimeWindowedCogroupedKStreamJ[K, V]) {

  /**
   * Aggregate the values of records in these streams by the grouped key and defined window.
   *
   * @param initializer  an initializer function that computes an initial intermediate aggregation result
   * @param materialized an instance of `Materialized` used to materialize a state store.
   * @return a [[KTable]] that contains "update" records with unmodified keys, and values that represent the latest
   *         (rolling) aggregate for each key
   * @see `org.apache.kafka.streams.kstream.TimeWindowedCogroupedKStream#aggregate`
   */
  def aggregate(initializer: => V)(implicit
    materialized: Materialized[K, V, ByteArrayWindowStore]
  ): KTable[Windowed[K], V] =
    new KTable(inner.aggregate((() => initializer).asInitializer, materialized))

  /**
   * Aggregate the values of records in these streams by the grouped key and defined window.
   *
   * @param initializer  an initializer function that computes an initial intermediate aggregation result
   * @param named        a [[Named]] config used to name the processor in the topology
   * @param materialized an instance of `Materialized` used to materialize a state store.
   * @return a [[KTable]] that contains "update" records with unmodified keys, and values that represent the latest
   *         (rolling) aggregate for each key
   * @see `org.apache.kafka.streams.kstream.TimeWindowedCogroupedKStream#aggregate`
   */
  def aggregate(initializer: => V, named: Named)(implicit
    materialized: Materialized[K, V, ByteArrayWindowStore]
  ): KTable[Windowed[K], V] =
    new KTable(inner.aggregate((() => initializer).asInitializer, named, materialized))

}
