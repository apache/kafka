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
import org.apache.kafka.streams.kstream.{Produced => ProducedJ}
import org.apache.kafka.streams.processor.StreamPartitioner

object Produced {

  /**
   * Create a Produced instance with provided keySerde and valueSerde.
   *
   * @tparam K         key type
   * @tparam V         value type
   * @param keySerde   Serde to use for serializing the key
   * @param valueSerde Serde to use for serializing the value
   * @return A new [[Produced]] instance configured with keySerde and valueSerde
   * @see KStream#through(String, Produced)
   * @see KStream#to(String, Produced)
   */
  def `with`[K, V](implicit keySerde: Serde[K], valueSerde: Serde[V]): ProducedJ[K, V] =
    ProducedJ.`with`(keySerde, valueSerde)

  /**
   * Create a Produced instance with provided keySerde, valueSerde, and partitioner.
   *
   * @tparam K          key type
   * @tparam V          value type
   * @param partitioner the function used to determine how records are distributed among partitions of the topic,
   *                    if not specified and `keySerde` provides a
   *                    `org.apache.kafka.streams.kstream.internals.WindowedSerializer` for the key
   *                    `org.apache.kafka.streams.kstream.internals.WindowedStreamPartitioner` will be
   *                    used&mdash;otherwise `org.apache.kafka.clients.producer.internals.DefaultPartitioner`
   *                    will be used
   * @param keySerde    Serde to use for serializing the key
   * @param valueSerde  Serde to use for serializing the value
   * @return A new [[Produced]] instance configured with keySerde, valueSerde, and partitioner
   * @see KStream#through(String, Produced)
   * @see KStream#to(String, Produced)
   */
  def `with`[K, V](
    partitioner: StreamPartitioner[K, V]
  )(implicit keySerde: Serde[K], valueSerde: Serde[V]): ProducedJ[K, V] =
    ProducedJ.`with`(keySerde, valueSerde, partitioner)
}
