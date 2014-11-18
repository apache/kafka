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

import kafka.common.TopicAndPartition
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

  private class DelayedProducerRequestMetrics(metricId: Option[TopicAndPartition]) extends KafkaMetricsGroup {
    val tags: scala.collection.Map[String, String] = metricId match {
      case Some(topicAndPartition) => Map("topic" -> topicAndPartition.topic, "partition" -> topicAndPartition.partition.toString)
      case None => Map.empty
    }

    val expiredRequestMeter = newMeter("ExpiresPerSecond", "requests", TimeUnit.SECONDS, tags)
  }

  private val producerRequestMetricsForKey = {
    val valueFactory = (k: TopicAndPartition) => new DelayedProducerRequestMetrics(Some(k))
    new Pool[TopicAndPartition, DelayedProducerRequestMetrics](Some(valueFactory))
  }

  private val aggregateProduceRequestMetrics = new DelayedProducerRequestMetrics(None)

  private def recordDelayedProducerKeyExpired(metricId: TopicAndPartition) {
    val keyMetrics = producerRequestMetricsForKey.getAndMaybePut(metricId)
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
      recordDelayedProducerKeyExpired(topicPartition)
    respond(delayedProduce)
  }

  // TODO: purgatory should not be responsible for sending back the responses
  def respond(delayedProduce: DelayedProduce) {
    val response = delayedProduce.respond(offsetManager)
    requestChannel.sendResponse(new RequestChannel.Response(delayedProduce.request, new BoundedByteBufferSend(response)))
  }
}
