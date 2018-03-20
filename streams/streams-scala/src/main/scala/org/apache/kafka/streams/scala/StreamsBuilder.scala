/**
  * Copyright (C) 2018 Lightbend Inc. <https://www.lightbend.com>
  */

package org.apache.kafka.streams.scala

import java.util.regex.Pattern

import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.kstream.{GlobalKTable, Materialized}
import org.apache.kafka.streams.processor.{ProcessorSupplier, StateStore}
import org.apache.kafka.streams.state.{KeyValueStore, StoreBuilder}
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

  def table[K, V](topic: String, materialized: Materialized[K, V, KeyValueStore[Bytes, Array[Byte]]])
    (implicit consumed: Consumed[K, V]): KTable[K, V] =
    inner.table[K, V](topic, consumed, materialized)

  def globalTable[K, V](topic: String)(implicit consumed: Consumed[K, V]): GlobalKTable[K, V] =
    inner.globalTable(topic, consumed)

  def globalTable[K, V](topic: String, materialized: Materialized[K, V, KeyValueStore[Bytes, Array[Byte]]])
    (implicit consumed: Consumed[K, V]): GlobalKTable[K, V] =
    inner.globalTable(topic, consumed, materialized)

  def addStateStore(builder: StoreBuilder[_ <: StateStore]): StreamsBuilderJ = inner.addStateStore(builder)

  def addGlobalStore(storeBuilder: StoreBuilder[_ <: StateStore],
                     topic: String,
                     sourceName: String,
                     consumed: Consumed[_, _],
                     processorName: String,
                     stateUpdateSupplier: ProcessorSupplier[_, _]): StreamsBuilderJ =
    inner.addGlobalStore(storeBuilder, topic, sourceName, consumed, processorName, stateUpdateSupplier)

  def build(): Topology = inner.build()
}
