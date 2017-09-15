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

package kafka.server

import kafka.common.TopicAndPartition
import org.apache.kafka.clients.admin.NewPartitions
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.ApiError

import scala.collection.{Map, Seq, mutable}

/**
  * A delayed create partitions operation that can be created by the admin manager and watched
  * in the topic purgatory
  */
class DelayedCreatePartitions(delayMs: Long,
                              done: Map[String, ApiError],
                              waiting: Map[String, NewPartitions],
                              adminManager: AdminManager,
                              listenerName: ListenerName,
                              responseCallback: Map[String, ApiError] => Unit)
  extends DelayedOperation(delayMs) {

  // the results of topics we were waiting for
  val waitedResults = mutable.Map.empty[String, ApiError]

  // the topics we're still waiting for
  val waitingFor = mutable.Map.empty[String, NewPartitions] ++= waiting

  /**
    * Call-back to execute when a delayed operation gets expired and hence forced to complete.
    */
  override def onExpiration(): Unit = {
    (waitingFor.keySet--waitedResults.keySet).foreach{
      case topic => waitedResults += topic -> new ApiError(Errors.REQUEST_TIMED_OUT, Errors.REQUEST_TIMED_OUT.message())
    }
    onComplete()
  }

  /**
    * Process for completing an operation; This function needs to be defined
    * in subclasses and will be called exactly once in forceComplete()
    */
  override def onComplete(): Unit = {
    responseCallback(waitedResults ++ done)
  }

  /**
    * Try to complete the delayed operation by first checking if the operation
    * can be completed by now. If yes execute the completion logic by calling
    * forceComplete() and return true iff forceComplete returns true; otherwise return false
    *
    * This function needs to be defined in subclasses
    */
  override def tryComplete(): Boolean = {
    for (topicMeta <- adminManager.metadataCache.getTopicMetadata(waitingFor.keySet, listenerName)) {
      if (isTraceEnabled)
        trace(s"DelayedCreatePartitions.tryComplete(): topicMeta: $topicMeta")
      val requiredCount = waitingFor(topicMeta.topic).totalCount()
      val currentCount = topicMeta.partitionMetadata.size()
      if (isTraceEnabled)
        trace(s"DelayedCreatePartitions.tryComplete(): topic: ${topicMeta.topic}, currentCount: $currentCount, requiredCount: $requiredCount")
      if (currentCount == requiredCount) {
        waitedResults += topicMeta.topic -> ApiError.NONE
        waitingFor -= topicMeta.topic
      }
    }
    if (isTraceEnabled)
      trace(s"DelayedCreatePartitions.tryComplete(): waitedResults: $waitedResults, waitingFor: $waitingFor")
    if (waitingFor.isEmpty) {
      return forceComplete()
    } else {
      false
    }

  }
}
