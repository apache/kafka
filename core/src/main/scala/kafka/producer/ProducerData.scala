/*
 * Copyright 2010 LinkedIn
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package kafka.producer

/**
 * Represents the data to be sent using the Producer send API
 * @param topic the topic under which the message is to be published
 * @param key the key used by the partitioner to pick a broker partition
 * @param data variable length data to be published as Kafka messages under topic
 */
class ProducerData[K, V](private val topic: String,
                         private val key: K,
                         private val data: Seq[V]) {

  def this(t: String, d: Seq[V]) = this(topic = t, key = null.asInstanceOf[K], data = d)

  def this(t: String, d: V) = this(topic = t, key = null.asInstanceOf[K], data = List(d))

  def getTopic: String = topic

  def getKey: K = key

  def getData: Seq[V] = data
}
