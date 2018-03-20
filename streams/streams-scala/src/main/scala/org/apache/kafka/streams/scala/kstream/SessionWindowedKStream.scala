/**
 * Copyright (C) 2018 Lightbend Inc. <https://www.lightbend.com>
 */

package org.apache.kafka.streams.scala.kstream

import org.apache.kafka.streams.kstream.{SessionWindowedKStream => SessionWindowedKStreamJ, _}
import org.apache.kafka.streams.state.SessionStore
import org.apache.kafka.common.utils.Bytes

import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.FunctionConversions._

/**
 * Wraps the Java class SessionWindowedKStream and delegates method calls to the underlying Java object.
 */
class SessionWindowedKStream[K, V](val inner: SessionWindowedKStreamJ[K, V]) {

  def aggregate[VR](initializer: () => VR,
    aggregator: (K, V, VR) => VR,
    merger: (K, VR, VR) => VR): KTable[Windowed[K], VR] = {

    inner.aggregate(initializer.asInitializer, aggregator.asAggregator, merger.asMerger)
  }

  def aggregate[VR](initializer: () => VR,
    aggregator: (K, V, VR) => VR,
    merger: (K, VR, VR) => VR,
    materialized: Materialized[K, VR, SessionStore[Bytes, Array[Byte]]]): KTable[Windowed[K], VR] = {

    inner.aggregate(initializer.asInitializer, aggregator.asAggregator, merger.asMerger, materialized)
  }

  def count(): KTable[Windowed[K], Long] = {
    val c: KTable[Windowed[K], java.lang.Long] = inner.count()
    c.mapValues[Long](Long2long(_))
  }

  def count(materialized: Materialized[K, Long, SessionStore[Bytes, Array[Byte]]]): KTable[Windowed[K], Long] =
    inner.count(materialized)

  def reduce(reducer: (V, V) => V): KTable[Windowed[K], V] = {
    inner.reduce((v1, v2) => reducer(v1, v2))
  }

  def reduce(reducer: (V, V) => V,
    materialized: Materialized[K, V, SessionStore[Bytes, Array[Byte]]]): KTable[Windowed[K], V] = {
    inner.reduce(reducer.asReducer, materialized)
  }
}
