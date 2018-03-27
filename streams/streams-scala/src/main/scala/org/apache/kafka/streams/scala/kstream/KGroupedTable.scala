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

import org.apache.kafka.streams.kstream.{KGroupedTable => KGroupedTableJ, _}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.FunctionConversions._

/**
 * Wraps the Java class KGroupedTable and delegates method calls to the underlying Java object.
 */
class KGroupedTable[K, V](inner: KGroupedTableJ[K, V]) {

  def count(): KTable[K, Long] = {
    val c: KTable[K, java.lang.Long] = inner.count()
    c.mapValues[Long](Long2long(_))
  }

  def count(materialized: Materialized[K, Long, ByteArrayKeyValueStore]): KTable[K, Long] =
    inner.count(materialized)

  def reduce(adder: (V, V) => V,
             subtractor: (V, V) => V): KTable[K, V] = {

    // need this explicit asReducer for Scala 2.11 or else the SAM conversion doesn't take place
    // works perfectly with Scala 2.12 though
    inner.reduce(adder.asReducer, subtractor.asReducer)
  }

  def reduce(adder: (V, V) => V,
             subtractor: (V, V) => V,
             materialized: Materialized[K, V, ByteArrayKeyValueStore]): KTable[K, V] = {

    // need this explicit asReducer for Scala 2.11 or else the SAM conversion doesn't take place
    // works perfectly with Scala 2.12 though
    inner.reduce(adder.asReducer, subtractor.asReducer, materialized)
  }

  def aggregate[VR](initializer: () => VR,
                    adder: (K, V, VR) => VR,
                    subtractor: (K, V, VR) => VR): KTable[K, VR] = {

    inner.aggregate(initializer.asInitializer, adder.asAggregator, subtractor.asAggregator)
  }

  def aggregate[VR](initializer: () => VR,
                    adder: (K, V, VR) => VR,
                    subtractor: (K, V, VR) => VR,
                    materialized: Materialized[K, VR, ByteArrayKeyValueStore]): KTable[K, VR] = {

    inner.aggregate(initializer.asInitializer, adder.asAggregator, subtractor.asAggregator, materialized)
  }
}
