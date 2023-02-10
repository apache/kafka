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
import org.apache.kafka.streams.kstream.{Joined => JoinedJ}

object Joined {

  /**
   * Create an instance of `org.apache.kafka.streams.kstream.Joined` with key, value, and otherValue Serde
   * instances.
   * `null` values are accepted and will be replaced by the default serdes as defined in config.
   *
   * @tparam K              key type
   * @tparam V              value type
   * @tparam VO             other value type
   * @param keySerde        the key serde to use.
   * @param valueSerde      the value serde to use.
   * @param otherValueSerde the otherValue serde to use. If `null` the default value serde from config will be used
   * @return new `org.apache.kafka.streams.kstream.Joined` instance with the provided serdes
   */
  def `with`[K, V, VO](implicit
    keySerde: Serde[K],
    valueSerde: Serde[V],
    otherValueSerde: Serde[VO]
  ): JoinedJ[K, V, VO] =
    JoinedJ.`with`(keySerde, valueSerde, otherValueSerde)

  /**
   * Create an instance of `org.apache.kafka.streams.kstream.Joined` with key, value, and otherValue Serde
   * instances.
   * `null` values are accepted and will be replaced by the default serdes as defined in config.
   *
   * @tparam K              key type
   * @tparam V              value type
   * @tparam VO             other value type
   * @param name            name of possible repartition topic
   * @param keySerde        the key serde to use.
   * @param valueSerde      the value serde to use.
   * @param otherValueSerde the otherValue serde to use. If `null` the default value serde from config will be used
   * @return new `org.apache.kafka.streams.kstream.Joined` instance with the provided serdes
   */
  // disable spotless scala, which wants to make a mess of the argument lists
  // format: off
  def `with`[K, V, VO](name: String)
                      (implicit keySerde: Serde[K],
                       valueSerde: Serde[V],
                       otherValueSerde: Serde[VO]): JoinedJ[K, V, VO] =
    JoinedJ.`with`(keySerde, valueSerde, otherValueSerde, name)
  // format:on
}
