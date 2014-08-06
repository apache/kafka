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

package kafka.server

import kafka.metrics.KafkaMetricsGroup
import kafka.network.RequestChannel
import kafka.api.FetchResponseSend

import java.util.concurrent.TimeUnit

/**
 * The purgatory holding delayed fetch requests
 */
class FetchRequestPurgatory(replicaManager: ReplicaManager, requestChannel: RequestChannel)
  extends RequestPurgatory[DelayedFetch](replicaManager.config.brokerId, replicaManager.config.fetchPurgatoryPurgeIntervalRequests) {
  this.logIdent = "[FetchRequestPurgatory-%d] ".format(replicaManager.config.brokerId)

  private class DelayedFetchRequestMetrics(forFollower: Boolean) extends KafkaMetricsGroup {
    private val metricPrefix = if (forFollower) "Follower" else "Consumer"

    val expiredRequestMeter = newMeter(metricPrefix + "ExpiresPerSecond", "requests", TimeUnit.SECONDS)
  }

  private val aggregateFollowerFetchRequestMetrics = new DelayedFetchRequestMetrics(forFollower = true)
  private val aggregateNonFollowerFetchRequestMetrics = new DelayedFetchRequestMetrics(forFollower = false)

  private def recordDelayedFetchExpired(forFollower: Boolean) {
    val metrics = if (forFollower) aggregateFollowerFetchRequestMetrics
    else aggregateNonFollowerFetchRequestMetrics

    metrics.expiredRequestMeter.mark()
  }

  /**
   * Check if a specified delayed fetch request is satisfied
   */
  def checkSatisfied(delayedFetch: DelayedFetch): Boolean = delayedFetch.isSatisfied(replicaManager)

  /**
   * When a delayed fetch request expires just answer it with whatever data is present
   */
  def expire(delayedFetch: DelayedFetch) {
    debug("Expiring fetch request %s.".format(delayedFetch.fetch))
    val fromFollower = delayedFetch.fetch.isFromFollower
    recordDelayedFetchExpired(fromFollower)
    respond(delayedFetch)
  }

  // TODO: purgatory should not be responsible for sending back the responses
  def respond(delayedFetch: DelayedFetch) {
    val response = delayedFetch.respond(replicaManager)
    requestChannel.sendResponse(new RequestChannel.Response(delayedFetch.request, new FetchResponseSend(response)))
  }
}