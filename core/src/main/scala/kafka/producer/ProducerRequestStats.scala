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

import kafka.metrics.{KafkaTimer, KafkaMetricsGroup}
import java.util.concurrent.TimeUnit
import kafka.utils.Pool
import kafka.common.{ClientIdAllBrokers, ClientIdBroker, ClientIdAndBroker}

@deprecated("This class has been deprecated and will be removed in a future release.", "0.10.0.0")
class ProducerRequestMetrics(metricId: ClientIdBroker) extends KafkaMetricsGroup {
  val tags = metricId match {
    case ClientIdAndBroker(clientId, brokerHost, brokerPort) => Map("clientId" -> clientId, "brokerHost" -> brokerHost, "brokerPort" -> brokerPort.toString)
    case ClientIdAllBrokers(clientId) => Map("clientId" -> clientId)
  }

  val requestTimer = new KafkaTimer(newTimer("ProducerRequestRateAndTimeMs", TimeUnit.MILLISECONDS, TimeUnit.SECONDS, tags))
  val requestSizeHist = newHistogram("ProducerRequestSize", biased = true, tags)
  val throttleTimeStats = newTimer("ProducerRequestThrottleRateAndTimeMs", TimeUnit.MILLISECONDS, TimeUnit.SECONDS, tags)
}

/**
 * Tracks metrics of requests made by a given producer client to all brokers.
 * @param clientId ClientId of the given producer
 */
@deprecated("This class has been deprecated and will be removed in a future release.", "0.10.0.0")
class ProducerRequestStats(clientId: String) {
  private val valueFactory = (k: ClientIdBroker) => new ProducerRequestMetrics(k)
  private val stats = new Pool[ClientIdBroker, ProducerRequestMetrics](Some(valueFactory))
  private val allBrokersStats = new ProducerRequestMetrics(new ClientIdAllBrokers(clientId))

  def getProducerRequestAllBrokersStats(): ProducerRequestMetrics = allBrokersStats

  def getProducerRequestStats(brokerHost: String, brokerPort: Int): ProducerRequestMetrics = {
    stats.getAndMaybePut(new ClientIdAndBroker(clientId, brokerHost, brokerPort))
  }
}

/**
 * Stores the request stats information of each producer client in a (clientId -> ProducerRequestStats) map.
 */
@deprecated("This object has been deprecated and will be removed in a future release.", "0.10.0.0")
object ProducerRequestStatsRegistry {
  private val valueFactory = (k: String) => new ProducerRequestStats(k)
  private val globalStats = new Pool[String, ProducerRequestStats](Some(valueFactory))

  def getProducerRequestStats(clientId: String) = {
    globalStats.getAndMaybePut(clientId)
  }

  def removeProducerRequestStats(clientId: String) {
    globalStats.remove(clientId)
  }
}

