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

import java.util.concurrent.TimeUnit

import kafka.common.{ClientIdAllBrokers, ClientIdBroker, ClientIdAndBroker}
import kafka.metrics.{KafkaMetricsGroup, KafkaTimer}
import kafka.utils.Pool

@deprecated("This class has been deprecated and will be removed in a future release.", "0.11.0.0")
class FetchRequestAndResponseMetrics(metricId: ClientIdBroker) extends KafkaMetricsGroup {
  val tags = metricId match {
    case ClientIdAndBroker(clientId, brokerHost, brokerPort) =>
      Map("clientId" -> clientId, "brokerHost" -> brokerHost,
      "brokerPort" -> brokerPort.toString)
    case ClientIdAllBrokers(clientId) =>
      Map("clientId" -> clientId)
  }

  val requestTimer = new KafkaTimer(newTimer("FetchRequestRateAndTimeMs", TimeUnit.MILLISECONDS, TimeUnit.SECONDS, tags))
  val requestSizeHist = newHistogram("FetchResponseSize", biased = true, tags)
  val throttleTimeStats = newTimer("FetchRequestThrottleRateAndTimeMs", TimeUnit.MILLISECONDS, TimeUnit.SECONDS, tags)
}

/**
 * Tracks metrics of the requests made by a given consumer client to all brokers, and the responses obtained from the brokers.
 * @param clientId ClientId of the given consumer
 */
@deprecated("This class has been deprecated and will be removed in a future release.", "0.11.0.0")
class FetchRequestAndResponseStats(clientId: String) {
  private val valueFactory = (k: ClientIdBroker) => new FetchRequestAndResponseMetrics(k)
  private val stats = new Pool[ClientIdBroker, FetchRequestAndResponseMetrics](Some(valueFactory))
  private val allBrokersStats = new FetchRequestAndResponseMetrics(new ClientIdAllBrokers(clientId))

  def getFetchRequestAndResponseAllBrokersStats(): FetchRequestAndResponseMetrics = allBrokersStats

  def getFetchRequestAndResponseStats(brokerHost: String, brokerPort: Int): FetchRequestAndResponseMetrics = {
    stats.getAndMaybePut(new ClientIdAndBroker(clientId, brokerHost, brokerPort))
  }
}

/**
 * Stores the fetch request and response stats information of each consumer client in a (clientId -> FetchRequestAndResponseStats) map.
 */
@deprecated("This object has been deprecated and will be removed in a future release.", "0.11.0.0")
object FetchRequestAndResponseStatsRegistry {
  private val valueFactory = (k: String) => new FetchRequestAndResponseStats(k)
  private val globalStats = new Pool[String, FetchRequestAndResponseStats](Some(valueFactory))

  def getFetchRequestAndResponseStats(clientId: String) = {
    globalStats.getAndMaybePut(clientId)
  }

  def removeConsumerFetchRequestAndResponseStats(clientId: String) {
    val pattern = (".*" + clientId + ".*").r
    val keys = globalStats.keys
    for (key <- keys) {
      pattern.findFirstIn(key) match {
        case Some(_) => globalStats.remove(key)
        case _ =>
      }
    }
  }
}


