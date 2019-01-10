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
import org.apache.kafka.streams.kstream.{Grouped => GroupedJ}

object Grouped {

  /**
   * Construct a `Grouped` instance with the provided key and value [[Serde]]s.
   * If the [[Serde]] params are `null` the default serdes defined in the configs will be used.
   *
   * @tparam K the key type
   * @tparam V the value type
   * @param keySerde   keySerde that will be used to materialize a stream
   * @param valueSerde valueSerde that will be used to materialize a stream
   * @return a new instance of [[Grouped]] configured with the provided serdes
   */
  def `with`[K, V](implicit keySerde: Serde[K], valueSerde: Serde[V]): GroupedJ[K, V] =
    GroupedJ.`with`(keySerde, valueSerde)

  /**
   * Construct a `Grouped` instance with the provided key and value [[Serde]]s.
   * If the [[Serde]] params are `null` the default serdes defined in the configs will be used.
   *
   * @tparam K the key type
   * @tparam V the value type
   * @param name the name used as part of a potential repartition topic
   * @param keySerde   keySerde that will be used to materialize a stream
   * @param valueSerde valueSerde that will be used to materialize a stream
   * @return a new instance of [[Grouped]] configured with the provided serdes
   */
  def `with`[K, V](name: String)(implicit keySerde: Serde[K], valueSerde: Serde[V]): GroupedJ[K, V] =
    GroupedJ.`with`(name, keySerde, valueSerde)

}
