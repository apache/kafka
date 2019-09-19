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
import org.apache.kafka.streams.kstream.{StreamJoin => StreamJoinJ}
import org.apache.kafka.streams.state.WindowBytesStoreSupplier

object StreamJoin {

  /**
   * Create an instance of [[org.apache.kafka.streams.kstream.StreamJoin]] with key, value, and otherValue [[Serde]]
   * instances.
   * `null` values are accepted and will be replaced by the default serdes as defined in config.
   *
   * @tparam K              key type
   * @tparam V              value type
   * @tparam VO             other value type
   * @param keySerde        the key serde to use.
   * @param valueSerde      the value serde to use.
   * @param otherValueSerde the otherValue serde to use. If `null` the default value serde from config will be used
   * @return new [[org.apache.kafka.streams.kstream.StreamJoin]] instance with the provided serdes
   */
  def `with`[K, V, VO](implicit keySerde: Serde[K],
                       valueSerde: Serde[V],
                       otherValueSerde: Serde[VO]): StreamJoinJ[K, V, VO] =
    StreamJoinJ.`with`(keySerde, valueSerde, otherValueSerde)

  /**
   * Create an instance of [[org.apache.kafka.streams.kstream.StreamJoin]] with store suppliers for the calling stream
   * and the other stream.  Also adds the key, value, and otherValue [[Serde]]
   * instances.
   * `null` values are accepted and will be replaced by the default serdes as defined in config.
   * @tparam K key type
   * @tparam V value type
   * @tparam VO other value type
   * @param supplier  store supplier to use
   * @param otherSupplier other store supplier to use
   * @param keySerde        the key serde to use.
   * @param valueSerde      the value serde to use.
   * @param otherValueSerde the otherValue serde to use. If `null` the default value serde from config will be used
   * @return new [[org.apache.kafka.streams.kstream.StreamJoin]] instance with the provided store suppliers and serdes
   */
  def `with`[K, V, VO](
    supplier: WindowBytesStoreSupplier,
    otherSupplier: WindowBytesStoreSupplier
  )(implicit keySerde: Serde[K], valueSerde: Serde[V], otherValueSerde: Serde[VO]): StreamJoinJ[K, V, VO] =
    StreamJoinJ
      .`with`(supplier, otherSupplier)
      .withKeySerde(keySerde)
      .withValueSerde(valueSerde)
      .withOtherValueSerde(otherValueSerde)

  /**
   * Create an instance of [[org.apache.kafka.streams.kstream.StreamJoin]] with the name used for naming
   * the state stores involved in the join.  Also adds the key, value, and otherValue [[Serde]]
   * instances.
   * `null` values are accepted and will be replaced by the default serdes as defined in config.
   * @tparam K key type
   * @tparam V value type
   * @tparam VO other value type
   * @param storeName       the name to use as a base name for the state stores of the join
   * @param keySerde        the key serde to use.
   * @param valueSerde      the value serde to use.
   * @param otherValueSerde the otherValue serde to use. If `null` the default value serde from config will be used
   * @return new [[org.apache.kafka.streams.kstream.StreamJoin]] instance with the provided store suppliers and serdes
   */
  def as[K, V, VO](
    storeName: String
  )(implicit keySerde: Serde[K], valueSerde: Serde[V], otherValueSerde: Serde[VO]): StreamJoinJ[K, V, VO] =
    StreamJoinJ.as(storeName).withKeySerde(keySerde).withValueSerde(valueSerde).withOtherValueSerde(otherValueSerde)

}
