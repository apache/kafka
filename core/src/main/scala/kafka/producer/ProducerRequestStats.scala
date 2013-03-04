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
import kafka.common.ClientIdAndBroker

class ProducerRequestMetrics(metricId: ClientIdAndBroker) extends KafkaMetricsGroup {
  val requestTimer = new KafkaTimer(newTimer(metricId + "ProducerRequestRateAndTimeMs", TimeUnit.MILLISECONDS, TimeUnit.SECONDS))
  val requestSizeHist = newHistogram(metricId + "ProducerRequestSize")
}

/**
 * Tracks metrics of requests made by a given producer client to all brokers.
 * @param clientId ClientId of the given producer
 */
class ProducerRequestStats(clientId: String) {
  private val valueFactory = (k: ClientIdAndBroker) => new ProducerRequestMetrics(k)
  private val stats = new Pool[ClientIdAndBroker, ProducerRequestMetrics](Some(valueFactory))
  private val allBrokersStats = new ProducerRequestMetrics(new ClientIdAndBroker(clientId, "AllBrokers"))

  def getProducerRequestAllBrokersStats(): ProducerRequestMetrics = allBrokersStats

  def getProducerRequestStats(brokerInfo: String): ProducerRequestMetrics = {
    stats.getAndMaybePut(new ClientIdAndBroker(clientId, brokerInfo + "-"))
  }
}

/**
 * Stores the request stats information of each producer client in a (clientId -> ProducerRequestStats) map.
 */
object ProducerRequestStatsRegistry {
  private val valueFactory = (k: String) => new ProducerRequestStats(k)
  private val globalStats = new Pool[String, ProducerRequestStats](Some(valueFactory))

  def getProducerRequestStats(clientId: String) = {
    globalStats.getAndMaybePut(clientId)
  }
}

