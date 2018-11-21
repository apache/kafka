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

import org.apache.kafka.streams.kstream.{Grouped => GroupedJ}
import org.apache.kafka.streams.scala.{KeySerde, ValueSerde}

object Grouped {

  /**
   * Construct a `Grouped` instance with the provided [[KeySerde]] and [[ValueSerde]].
   *
   * @tparam K the key type
   * @tparam V the value type
   * @param keySerde   keySerde that will be used to materialize a stream
   * @param valueSerde valueSerde that will be used to materialize a stream
   * @return a new instance of [[Grouped]] configured with the provided serdes
   */
  def `with`[K, V](implicit keySerde: KeySerde[K], valueSerde: ValueSerde[V]): GroupedJ[K, V] =
    GroupedJ.`with`(keySerde.serde, valueSerde.serde)

  /**
   * Construct a `Grouped` instance with the provided [[KeySerde]] and [[ValueSerde]].
   *
   * @tparam K the key type
   * @tparam V the value type
   * @param name       the name used as part of a potential repartition topic
   * @param keySerde   keySerde that will be used to materialize a stream
   * @param valueSerde valueSerde that will be used to materialize a stream
   * @return a new instance of [[Grouped]] configured with the provided serdes
   */
  def `with`[K, V](name: String)(implicit keySerde: KeySerde[K], valueSerde: ValueSerde[V]): GroupedJ[K, V] =
    GroupedJ.`with`(name, keySerde.serde, valueSerde.serde)

}
