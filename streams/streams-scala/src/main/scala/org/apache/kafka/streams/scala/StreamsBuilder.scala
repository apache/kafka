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

import java.util.regex.Pattern

import org.apache.kafka.streams.kstream.{GlobalKTable, Materialized}
import org.apache.kafka.streams.processor.{ProcessorSupplier, StateStore}
import org.apache.kafka.streams.state.StoreBuilder
import org.apache.kafka.streams.{Consumed, StreamsBuilder => StreamsBuilderJ, Topology}
import org.apache.kafka.streams.scala.kstream._
import ImplicitConversions._
import scala.collection.JavaConverters._

/**
  * Wraps the Java class StreamsBuilder and delegates method calls to the underlying Java object.
  */
class StreamsBuilder(inner: StreamsBuilderJ = new StreamsBuilderJ) {

  /**
   * Create a [[kstream.KStream]] from the specified topic.
   * <p>
   * The `implicit Consumed` instance provides the values of `auto.offset.reset` strategy, `TimestampExtractor`, 
   * key and value deserializers etc. If the implicit is not found in scope, compiler error will result.
   * <p>
   * A convenient alternative is to have the necessary implicit serdes in scope, which will be implicitly
   * converted to generate an instance of `Consumed`. @see [[ImplicitConversions]]. 
   * {{{
   * // Brings all implicit conversions in scope
   * import ImplicitConversions._
   *
   * // Bring implicit default serdes in scope
   * import Serdes._
   *
   * val builder = new StreamsBuilder()
   *
   * // stream function gets the implicit Consumed which is constructed automatically
   * // from the serdes through the implicits in ImplicitConversions#consumedFromSerde
   * val userClicksStream: KStream[String, Long] = builder.stream(userClicksTopic)
   * }}}
   *
   * @param topic the topic name
   * @return a [[kstream.KStream]] for the specified topic
   */
  def stream[K, V](topic: String)(implicit consumed: Consumed[K, V]): KStream[K, V] =
    inner.stream[K, V](topic, consumed)

  /**
   * Create a [[kstream.KStream]] from the specified topics.
   *
   * @param topics the topic names
   * @return a [[kstream.KStream]] for the specified topics
   * @see #stream(String)
   * @see `org.apache.kafka.streams.StreamsBuilder#stream`
   */
  def stream[K, V](topics: List[String])(implicit consumed: Consumed[K, V]): KStream[K, V] =
    inner.stream[K, V](topics.asJava, consumed)

  /**
   * Create a [[kstream.KStream]] from the specified topic pattern.
   *
   * @param topics the topic name pattern
   * @return a [[kstream.KStream]] for the specified topics
   * @see #stream(String)
   * @see `org.apache.kafka.streams.StreamsBuilder#stream`
   */
  def stream[K, V](topicPattern: Pattern)(implicit consumed: Consumed[K, V]): KStream[K, V] =
    inner.stream[K, V](topicPattern, consumed)

  /**
   * Create a [[kstream.KTable]] from the specified topic.
   * <p>
   * The `implicit Consumed` instance provides the values of `auto.offset.reset` strategy, `TimestampExtractor`, 
   * key and value deserializers etc. If the implicit is not found in scope, compiler error will result.
   * <p>
   * A convenient alternative is to have the necessary implicit serdes in scope, which will be implicitly
   * converted to generate an instance of `Consumed`. @see [[ImplicitConversions]]. 
   * {{{
   * // Brings all implicit conversions in scope
   * import ImplicitConversions._
   *
   * // Bring implicit default serdes in scope
   * import Serdes._
   *
   * val builder = new StreamsBuilder()
   *
   * // stream function gets the implicit Consumed which is constructed automatically
   * // from the serdes through the implicits in ImplicitConversions#consumedFromSerde
   * val userClicksStream: KTable[String, Long] = builder.table(userClicksTopic)
   * }}}
   *
   * @param topic the topic name
   * @return a [[kstream.KTable]] for the specified topic
   * @see `org.apache.kafka.streams.StreamsBuilder#table`
   */
  def table[K, V](topic: String)(implicit consumed: Consumed[K, V]): KTable[K, V] =
    inner.table[K, V](topic, consumed)

  /**
   * Create a [[kstream.KTable]] from the specified topic.
   *
   * @param topic the topic name
   * @param materialized  the instance of `Materialized` used to materialize a state store
   * @return a [[kstream.KTable]] for the specified topic
   * @see #table(String)
   * @see `org.apache.kafka.streams.StreamsBuilder#table`
   */
  def table[K, V](topic: String, materialized: Materialized[K, V, ByteArrayKeyValueStore])
    (implicit consumed: Consumed[K, V]): KTable[K, V] =
    inner.table[K, V](topic, consumed, materialized)

  /**
   * Create a `GlobalKTable` from the specified topic. The serializers from the implicit `Consumed`
   * instance will be used. Input records with `null` key will be dropped.
   *
   * @param topic the topic name
   * @return a `GlobalKTable` for the specified topic
   * @see `org.apache.kafka.streams.StreamsBuilder#globalTable`
   */
  def globalTable[K, V](topic: String)(implicit consumed: Consumed[K, V]): GlobalKTable[K, V] =
    inner.globalTable(topic, consumed)

  /**
   * Create a `GlobalKTable` from the specified topic. The resulting `GlobalKTable` will be materialized 
   * in a local `KeyValueStore` configured with the provided instance of `Materialized`. The serializers 
   * from the implicit `Consumed` instance will be used.
   *
   * @param topic the topic name
   * @param materialized  the instance of `Materialized` used to materialize a state store
   * @return a `GlobalKTable` for the specified topic
   * @see `org.apache.kafka.streams.StreamsBuilder#globalTable`
   */
  def globalTable[K, V](topic: String, materialized: Materialized[K, V, ByteArrayKeyValueStore])
    (implicit consumed: Consumed[K, V]): GlobalKTable[K, V] =
    inner.globalTable(topic, consumed, materialized)

  /**
   * Adds a state store to the underlying `Topology`. The store must still be "connected" to a `Processor`, 
   * `Transformer`, or `ValueTransformer` before it can be used.
   *
   * @param builder the builder used to obtain this state store `StateStore` instance
   * @return the underlying Java abstraction `StreamsBuilder` after adding the `StateStore`
   * @throws TopologyException if state store supplier is already added
   * @see `org.apache.kafka.streams.StreamsBuilder#addStateStore`
   */
  def addStateStore(builder: StoreBuilder[_ <: StateStore]): StreamsBuilderJ = inner.addStateStore(builder)

  /**
   * Adds a global `StateStore` to the topology. Global stores should not be added to `Processor, `Transformer`, 
   * or `ValueTransformer` (in contrast to regular stores).
   *
   * @see `org.apache.kafka.streams.StreamsBuilder#addGlobalStore`
   */ 
  def addGlobalStore(storeBuilder: StoreBuilder[_ <: StateStore],
                     topic: String,
                     consumed: Consumed[_, _],
                     stateUpdateSupplier: ProcessorSupplier[_, _]): StreamsBuilderJ =
    inner.addGlobalStore(storeBuilder, topic, consumed, stateUpdateSupplier)

  def build(): Topology = inner.build()
}
