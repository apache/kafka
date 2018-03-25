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

import org.apache.kafka.streams.kstream.{KTable => KTableJ, _}
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
    materialized: Materialized[K, V, ByteArrayKVStore]): KTable[K, V] = {
    inner.filter(predicate.asPredicate, materialized)
  }

  def filterNot(predicate: (K, V) => Boolean): KTable[K, V] = {
    inner.filterNot(predicate(_, _))
  }

  def filterNot(predicate: (K, V) => Boolean,
    materialized: Materialized[K, V, ByteArrayKVStore]): KTable[K, V] = {
    inner.filterNot(predicate.asPredicate, materialized)
  }

  def mapValues[VR](mapper: V => VR): KTable[K, VR] = {
    inner.mapValues[VR](mapper.asValueMapper)
  }

  def mapValues[VR](mapper: V => VR,
    materialized: Materialized[K, VR, ByteArrayKVStore]): KTable[K, VR] = {
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
    materialized: Materialized[K, VR, ByteArrayKVStore]): KTable[K, VR] = {

    inner.join[VO, VR](other.inner, joiner.asValueJoiner, materialized)
  }

  def leftJoin[VO, VR](other: KTable[K, VO],
    joiner: (V, VO) => VR): KTable[K, VR] = {

    inner.leftJoin[VO, VR](other.inner, joiner.asValueJoiner)
  }

  def leftJoin[VO, VR](other: KTable[K, VO],
    joiner: (V, VO) => VR,
    materialized: Materialized[K, VR, ByteArrayKVStore]): KTable[K, VR] = {

    inner.leftJoin[VO, VR](other.inner, joiner.asValueJoiner, materialized)
  }

  def outerJoin[VO, VR](other: KTable[K, VO],
    joiner: (V, VO) => VR): KTable[K, VR] = {

    inner.outerJoin[VO, VR](other.inner, joiner.asValueJoiner)
  }

  def outerJoin[VO, VR](other: KTable[K, VO],
    joiner: (V, VO) => VR,
    materialized: Materialized[K, VR, ByteArrayKVStore]): KTable[K, VR] = {

    inner.outerJoin[VO, VR](other.inner, joiner.asValueJoiner, materialized)
  }

  def queryableStoreName: String =
    inner.queryableStoreName
}
