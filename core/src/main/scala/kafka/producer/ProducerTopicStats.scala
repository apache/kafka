/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import kafka.metrics.KafkaMetricsGroup
import kafka.common.{ClientIdTopic, ClientIdAllTopics, ClientIdAndTopic}
import kafka.utils.{Pool, threadsafe}
import java.util.concurrent.TimeUnit

@deprecated("This class has been deprecated and will be removed in a future release.", "0.10.0.0")
@threadsafe
class ProducerTopicMetrics(metricId: ClientIdTopic) extends KafkaMetricsGroup {
  val tags = metricId match {
    case ClientIdAndTopic(clientId, topic) => Map("clientId" -> clientId, "topic" -> topic)
    case ClientIdAllTopics(clientId) => Map("clientId" -> clientId)
  }

  val messageRate = newMeter("MessagesPerSec", "messages", TimeUnit.SECONDS, tags)
  val byteRate = newMeter("BytesPerSec", "bytes", TimeUnit.SECONDS, tags)
  val droppedMessageRate = newMeter("DroppedMessagesPerSec", "drops", TimeUnit.SECONDS, tags)
}

/**
 * Tracks metrics for each topic the given producer client has produced data to.
 * @param clientId The clientId of the given producer client.
 */
@deprecated("This class has been deprecated and will be removed in a future release.", "0.10.0.0")
class ProducerTopicStats(clientId: String) {
  private val valueFactory = (k: ClientIdTopic) => new ProducerTopicMetrics(k)
  private val stats = new Pool[ClientIdTopic, ProducerTopicMetrics](Some(valueFactory))
  private val allTopicsStats = new ProducerTopicMetrics(new ClientIdAllTopics(clientId)) // to differentiate from a topic named AllTopics

  def getProducerAllTopicsStats(): ProducerTopicMetrics = allTopicsStats

  def getProducerTopicStats(topic: String): ProducerTopicMetrics = {
    stats.getAndMaybePut(new ClientIdAndTopic(clientId, topic))
  }
}

/**
 * Stores the topic stats information of each producer client in a (clientId -> ProducerTopicStats) map.
 */
@deprecated("This object has been deprecated and will be removed in a future release.", "0.10.0.0")
object ProducerTopicStatsRegistry {
  private val valueFactory = (k: String) => new ProducerTopicStats(k)
  private val globalStats = new Pool[String, ProducerTopicStats](Some(valueFactory))

  def getProducerTopicStats(clientId: String) = {
    globalStats.getAndMaybePut(clientId)
  }

  def removeProducerTopicStats(clientId: String) {
    globalStats.remove(clientId)
  }
}
