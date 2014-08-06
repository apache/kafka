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
import kafka.utils.Pool
import kafka.network.{BoundedByteBufferSend, RequestChannel}

import java.util.concurrent.TimeUnit

/**
 * The purgatory holding delayed producer requests
 */
class ProducerRequestPurgatory(replicaManager: ReplicaManager, offsetManager: OffsetManager, requestChannel: RequestChannel)
  extends RequestPurgatory[DelayedProduce](replicaManager.config.brokerId, replicaManager.config.producerPurgatoryPurgeIntervalRequests) {
  this.logIdent = "[ProducerRequestPurgatory-%d] ".format(replicaManager.config.brokerId)

  private class DelayedProducerRequestMetrics(keyLabel: String = DelayedRequestKey.globalLabel) extends KafkaMetricsGroup {
    val expiredRequestMeter = newMeter(keyLabel + "ExpiresPerSecond", "requests", TimeUnit.SECONDS)
  }

  private val producerRequestMetricsForKey = {
    val valueFactory = (k: DelayedRequestKey) => new DelayedProducerRequestMetrics(k.keyLabel + "-")
    new Pool[DelayedRequestKey, DelayedProducerRequestMetrics](Some(valueFactory))
  }

  private val aggregateProduceRequestMetrics = new DelayedProducerRequestMetrics

  private def recordDelayedProducerKeyExpired(key: DelayedRequestKey) {
    val keyMetrics = producerRequestMetricsForKey.getAndMaybePut(key)
    List(keyMetrics, aggregateProduceRequestMetrics).foreach(_.expiredRequestMeter.mark())
  }

  /**
   * Check if a specified delayed fetch request is satisfied
   */
  def checkSatisfied(delayedProduce: DelayedProduce) = delayedProduce.isSatisfied(replicaManager)

  /**
   * When a delayed produce request expires answer it with possible time out error codes
   */
  def expire(delayedProduce: DelayedProduce) {
    debug("Expiring produce request %s.".format(delayedProduce.produce))
    for ((topicPartition, responseStatus) <- delayedProduce.partitionStatus if responseStatus.acksPending)
      recordDelayedProducerKeyExpired(new TopicPartitionRequestKey(topicPartition))
    respond(delayedProduce)
  }

  // TODO: purgatory should not be responsible for sending back the responses
  def respond(delayedProduce: DelayedProduce) {
    val response = delayedProduce.respond(offsetManager)
    requestChannel.sendResponse(new RequestChannel.Response(delayedProduce.request, new BoundedByteBufferSend(response)))
  }
}
