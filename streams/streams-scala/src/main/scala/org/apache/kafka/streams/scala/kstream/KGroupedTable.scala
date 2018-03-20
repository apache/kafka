/**
 * Copyright (C) 2018 Lightbend Inc. <https://www.lightbend.com>
 */

package org.apache.kafka.streams.scala.kstream

import org.apache.kafka.streams.kstream.{KGroupedTable => KGroupedTableJ, _}
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.FunctionConversions._

/**
 * Wraps the Java class KGroupedTable and delegates method calls to the underlying Java object.
 */
class KGroupedTable[K, V](inner: KGroupedTableJ[K, V]) {

  type ByteArrayKVStore = KeyValueStore[Bytes, Array[Byte]]

  def count(): KTable[K, Long] = {
    val c: KTable[K, java.lang.Long] = inner.count()
    c.mapValues[Long](Long2long(_))
  }

  def count(materialized: Materialized[K, Long, ByteArrayKVStore]): KTable[K, Long] =
    inner.count(materialized)

  def reduce(adder: (V, V) => V,
             subTractor: (V, V) => V): KTable[K, V] = {

    // need this explicit asReducer for Scala 2.11 or else the SAM conversion doesn't take place
    // works perfectly with Scala 2.12 though
    inner.reduce(((v1, v2) => adder(v1, v2)).asReducer, ((v1, v2) => subTractor(v1, v2)).asReducer)
  }

  def reduce(adder: (V, V) => V,
             subtractor: (V, V) => V,
             materialized: Materialized[K, V, ByteArrayKVStore]): KTable[K, V] = {

    // need this explicit asReducer for Scala 2.11 or else the SAM conversion doesn't take place
    // works perfectly with Scala 2.12 though
    inner.reduce(((v1, v2) => adder(v1, v2)).asReducer, ((v1, v2) => subtractor(v1, v2)).asReducer, materialized)
  }

  def aggregate[VR](initializer: () => VR,
                    adder: (K, V, VR) => VR,
                    subtractor: (K, V, VR) => VR): KTable[K, VR] = {

    inner.aggregate(initializer.asInitializer, adder.asAggregator, subtractor.asAggregator)
  }

  def aggregate[VR](initializer: () => VR,
                    adder: (K, V, VR) => VR,
                    subtractor: (K, V, VR) => VR,
                    materialized: Materialized[K, VR, ByteArrayKVStore]): KTable[K, VR] = {

    inner.aggregate(initializer.asInitializer, adder.asAggregator, subtractor.asAggregator, materialized)
  }
}
