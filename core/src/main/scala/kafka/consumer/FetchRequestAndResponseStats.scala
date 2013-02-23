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

package kafka.consumer

import kafka.metrics.{KafkaTimer, KafkaMetricsGroup}
import kafka.utils.Pool
import java.util.concurrent.TimeUnit
import kafka.common.ClientIdAndBroker

class FetchRequestAndResponseMetrics(metricId: ClientIdAndBroker) extends KafkaMetricsGroup {
  val requestTimer = new KafkaTimer(newTimer(metricId + "FetchRequestRateAndTimeMs", TimeUnit.MILLISECONDS, TimeUnit.SECONDS))
  val requestSizeHist = newHistogram(metricId + "FetchResponseSize")
}

/**
 * Tracks metrics of the requests made by a given consumer client to all brokers, and the responses obtained from the brokers.
 * @param clientId ClientId of the given consumer
 */
class FetchRequestAndResponseStats(clientId: String) {
  private val valueFactory = (k: ClientIdAndBroker) => new FetchRequestAndResponseMetrics(k)
  private val stats = new Pool[ClientIdAndBroker, FetchRequestAndResponseMetrics](Some(valueFactory))
  private val allBrokersStats = new FetchRequestAndResponseMetrics(new ClientIdAndBroker(clientId, "AllBrokers"))

  def getFetchRequestAndResponseAllBrokersStats(): FetchRequestAndResponseMetrics = allBrokersStats

  def getFetchRequestAndResponseStats(brokerInfo: String): FetchRequestAndResponseMetrics = {
    stats.getAndMaybePut(new ClientIdAndBroker(clientId, brokerInfo + "-"))
  }
}

/**
 * Stores the fetch request and response stats information of each consumer client in a (clientId -> FetchRequestAndResponseStats) map.
 */
object FetchRequestAndResponseStatsRegistry {
  private val valueFactory = (k: String) => new FetchRequestAndResponseStats(k)
  private val globalStats = new Pool[String, FetchRequestAndResponseStats](Some(valueFactory))

  def getFetchRequestAndResponseStats(clientId: String) = {
    globalStats.getAndMaybePut(clientId)
  }
}


