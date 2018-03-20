/**
 * Copyright (C) 2018 Lightbend Inc. <https://www.lightbend.com>
 */

package org.apache.kafka.streams.scala.kstream

import org.apache.kafka.streams.kstream.{KTable => KTableJ, _}
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.FunctionConversions._

/**
 * Wraps the Java class KTable and delegates method calls to the underlying Java object.
 */
class KTable[K, V](val inner: KTableJ[K, V]) {

  def filter(predicate: (K, V) => Boolean): KTable[K, V] = {
    inner.filter(predicate(_, _))
  }

  def filter(predicate: (K, V) => Boolean,
    materialized: Materialized[K, V, KeyValueStore[Bytes, Array[Byte]]]): KTable[K, V] = {
    inner.filter(predicate.asPredicate, materialized)
  }

  def filterNot(predicate: (K, V) => Boolean): KTable[K, V] = {
    inner.filterNot(predicate(_, _))
  }

  def filterNot(predicate: (K, V) => Boolean,
    materialized: Materialized[K, V, KeyValueStore[Bytes, Array[Byte]]]): KTable[K, V] = {
    inner.filterNot(predicate.asPredicate, materialized)
  }

  def mapValues[VR](mapper: V => VR): KTable[K, VR] = {
    inner.mapValues[VR](mapper.asValueMapper)
  }

  def mapValues[VR](mapper: V => VR,
    materialized: Materialized[K, VR, KeyValueStore[Bytes, Array[Byte]]]): KTable[K, VR] = {
    inner.mapValues[VR](mapper.asValueMapper, materialized)
  }

  def toStream: KStream[K, V] = inner.toStream

  def toStream[KR](mapper: (K, V) => KR): KStream[KR, V] = {
    inner.toStream[KR](mapper.asKeyValueMapper)
  }

  def groupBy[KR, VR](selector: (K, V) => (KR, VR))(implicit serialized: Serialized[KR, VR]): KGroupedTable[KR, VR] = {
    inner.groupBy(selector.asKeyValueMapper, serialized)
  }

  def join[VO, VR](other: KTable[K, VO],
    joiner: (V, VO) => VR): KTable[K, VR] = {

    inner.join[VO, VR](other.inner, joiner.asValueJoiner)
  }

  def join[VO, VR](other: KTable[K, VO],
    joiner: (V, VO) => VR,
    materialized: Materialized[K, VR, KeyValueStore[Bytes, Array[Byte]]]): KTable[K, VR] = {

    inner.join[VO, VR](other.inner, joiner.asValueJoiner, materialized)
  }

  def leftJoin[VO, VR](other: KTable[K, VO],
    joiner: (V, VO) => VR): KTable[K, VR] = {

    inner.leftJoin[VO, VR](other.inner, joiner.asValueJoiner)
  }

  def leftJoin[VO, VR](other: KTable[K, VO],
    joiner: (V, VO) => VR,
    materialized: Materialized[K, VR, KeyValueStore[Bytes, Array[Byte]]]): KTable[K, VR] = {

    inner.leftJoin[VO, VR](other.inner, joiner.asValueJoiner, materialized)
  }

  def outerJoin[VO, VR](other: KTable[K, VO],
    joiner: (V, VO) => VR): KTable[K, VR] = {

    inner.outerJoin[VO, VR](other.inner, joiner.asValueJoiner)
  }

  def outerJoin[VO, VR](other: KTable[K, VO],
    joiner: (V, VO) => VR,
    materialized: Materialized[K, VR, KeyValueStore[Bytes, Array[Byte]]]): KTable[K, VR] = {

    inner.outerJoin[VO, VR](other.inner, joiner.asValueJoiner, materialized)
  }

  def queryableStoreName: String =
    inner.queryableStoreName
}
