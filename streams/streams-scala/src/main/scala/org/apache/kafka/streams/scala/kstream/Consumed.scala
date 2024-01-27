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
package org.apache.kafka.streams.scala.kstream

import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.kstream.{Consumed => ConsumedJ}
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.processor.TimestampExtractor

object Consumed {

  /**
   * Create an instance of [[Consumed]] with the supplied arguments. `null` values are acceptable.
   *
   * @tparam K                 key type
   * @tparam V                 value type
   * @param timestampExtractor the timestamp extractor to used. If `null` the default timestamp extractor from
   *                           config will be used
   * @param resetPolicy        the offset reset policy to be used. If `null` the default reset policy from config
   *                           will be used
   * @param keySerde           the key serde to use.
   * @param valueSerde         the value serde to use.
   * @return a new instance of [[Consumed]]
   */
  def `with`[K, V](
    timestampExtractor: TimestampExtractor,
    resetPolicy: Topology.AutoOffsetReset
  )(implicit keySerde: Serde[K], valueSerde: Serde[V]): ConsumedJ[K, V] =
    ConsumedJ.`with`(keySerde, valueSerde, timestampExtractor, resetPolicy)

  /**
   * Create an instance of [[Consumed]] with key and value [[Serde]]s.
   *
   * @tparam K         key type
   * @tparam V         value type
   * @return a new instance of [[Consumed]]
   */
  def `with`[K, V](implicit keySerde: Serde[K], valueSerde: Serde[V]): ConsumedJ[K, V] =
    ConsumedJ.`with`(keySerde, valueSerde)

  /**
   * Create an instance of [[Consumed]] with a [[TimestampExtractor]].
   *
   * @param timestampExtractor the timestamp extractor to used. If `null` the default timestamp extractor from
   *                           config will be used
   * @tparam K                 key type
   * @tparam V                 value type
   * @return a new instance of [[Consumed]]
   */
  def `with`[K, V](
    timestampExtractor: TimestampExtractor
  )(implicit keySerde: Serde[K], valueSerde: Serde[V]): ConsumedJ[K, V] =
    ConsumedJ.`with`(timestampExtractor).withKeySerde(keySerde).withValueSerde(valueSerde)

  /**
   * Create an instance of [[Consumed]] with a [[Topology.AutoOffsetReset]].
   *
   * @tparam K          key type
   * @tparam V          value type
   * @param resetPolicy the offset reset policy to be used. If `null` the default reset policy from config will be used
   * @return a new instance of [[Consumed]]
   */
  def `with`[K, V](
    resetPolicy: Topology.AutoOffsetReset
  )(implicit keySerde: Serde[K], valueSerde: Serde[V]): ConsumedJ[K, V] =
    ConsumedJ.`with`(resetPolicy).withKeySerde(keySerde).withValueSerde(valueSerde)
}
