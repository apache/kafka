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

import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.{
  KGroupedStream => KGroupedStreamJ,
  KGroupedTable => KGroupedTableJ,
  KStream => KStreamJ,
  KTable => KTableJ,
  SessionWindowedKStream => SessionWindowedKStreamJ,
  TimeWindowedKStream => TimeWindowedKStreamJ
}
import org.apache.kafka.streams.processor.StateStore
import org.apache.kafka.streams.scala.kstream._

import scala.language.implicitConversions

/**
 * Implicit conversions between the Scala wrapper objects and the underlying Java
 * objects.
 */
object ImplicitConversions {

  implicit def wrapKStream[K, V](inner: KStreamJ[K, V]): KStream[K, V] =
    new KStream[K, V](inner)

  implicit def wrapKGroupedStream[K, V](inner: KGroupedStreamJ[K, V]): KGroupedStream[K, V] =
    new KGroupedStream[K, V](inner)

  implicit def wrapSessionWindowedKStream[K, V](inner: SessionWindowedKStreamJ[K, V]): SessionWindowedKStream[K, V] =
    new SessionWindowedKStream[K, V](inner)

  implicit def wrapTimeWindowedKStream[K, V](inner: TimeWindowedKStreamJ[K, V]): TimeWindowedKStream[K, V] =
    new TimeWindowedKStream[K, V](inner)

  implicit def wrapKTable[K, V](inner: KTableJ[K, V]): KTable[K, V] =
    new KTable[K, V](inner)

  implicit def wrapKGroupedTable[K, V](inner: KGroupedTableJ[K, V]): KGroupedTable[K, V] =
    new KGroupedTable[K, V](inner)

  implicit def tuple2ToKeyValue[K, V](tuple: (K, V)): KeyValue[K, V] = new KeyValue(tuple._1, tuple._2)

  // we would also like to allow users implicit serdes
  // and these implicits will convert them to `Grouped`, `Produced` or `Consumed`

  implicit def groupedFromSerde[K, V](implicit keySerde: Serde[K], valueSerde: Serde[V]): Grouped[K, V] =
    Grouped.`with`[K, V]

  implicit def consumedFromSerde[K, V](implicit keySerde: Serde[K], valueSerde: Serde[V]): Consumed[K, V] =
    Consumed.`with`[K, V]

  implicit def producedFromSerde[K, V](implicit keySerde: Serde[K], valueSerde: Serde[V]): Produced[K, V] =
    Produced.`with`[K, V]

  implicit def materializedFromSerde[K, V, S <: StateStore](implicit keySerde: Serde[K],
                                                            valueSerde: Serde[V]): Materialized[K, V, S] =
    Materialized.`with`[K, V, S]

  implicit def joinedFromKeyValueOtherSerde[K, V, VO](implicit keySerde: Serde[K],
                                                      valueSerde: Serde[V],
                                                      otherValueSerde: Serde[VO]): Joined[K, V, VO] =
    Joined.`with`[K, V, VO]
}
