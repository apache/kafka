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

  def stream[K, V](topic: String)(implicit consumed: Consumed[K, V]): KStream[K, V] =
    inner.stream[K, V](topic, consumed)

  def stream[K, V](topics: List[String])(implicit consumed: Consumed[K, V]): KStream[K, V] =
    inner.stream[K, V](topics.asJava, consumed)

  def stream[K, V](topicPattern: Pattern)(implicit consumed: Consumed[K, V]): KStream[K, V] =
    inner.stream[K, V](topicPattern, consumed)

  def table[K, V](topic: String)(implicit consumed: Consumed[K, V]): KTable[K, V] =
    inner.table[K, V](topic, consumed)

  def table[K, V](topic: String, materialized: Materialized[K, V, ByteArrayKeyValueStore])
    (implicit consumed: Consumed[K, V]): KTable[K, V] =
    inner.table[K, V](topic, consumed, materialized)

  def globalTable[K, V](topic: String)(implicit consumed: Consumed[K, V]): GlobalKTable[K, V] =
    inner.globalTable(topic, consumed)

  def globalTable[K, V](topic: String, materialized: Materialized[K, V, ByteArrayKeyValueStore])
    (implicit consumed: Consumed[K, V]): GlobalKTable[K, V] =
    inner.globalTable(topic, consumed, materialized)

  def addStateStore(builder: StoreBuilder[_ <: StateStore]): StreamsBuilderJ = inner.addStateStore(builder)

  def addGlobalStore(storeBuilder: StoreBuilder[_ <: StateStore],
                     topic: String,
                     consumed: Consumed[_, _],
                     stateUpdateSupplier: ProcessorSupplier[_, _]): StreamsBuilderJ =
    inner.addGlobalStore(storeBuilder, topic, consumed, stateUpdateSupplier)

  def build(): Topology = inner.build()
}
