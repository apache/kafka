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

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.ApiError

import scala.collection.{Map, mutable}

/** A delayed elect leader operation that can be created by the replica manager and watched
  * in the elect leader purgatory
  */
class DelayedElectLeader(
  delayMs: Long,
  expectedLeaders: Map[TopicPartition, Int],
  results: Map[TopicPartition, ApiError],
  replicaManager: ReplicaManager,
  responseCallback: Map[TopicPartition, ApiError] => Unit
) extends DelayedOperation(delayMs) {

  private val waitingPartitions = mutable.Map() ++= expectedLeaders
  private val fullResults = mutable.Map() ++= results


  /**
    * Call-back to execute when a delayed operation gets expired and hence forced to complete.
    */
  override def onExpiration(): Unit = {}

  /**
    * Process for completing an operation; This function needs to be defined
    * in subclasses and will be called exactly once in forceComplete()
    */
  override def onComplete(): Unit = {
    // This could be called to force complete, so I need the full list of partitions, so I can time them all out.
    updateWaiting()
    val timedOut = waitingPartitions.map {
      case (tp, _) => tp -> new ApiError(Errors.REQUEST_TIMED_OUT, null)
    }
    responseCallback(timedOut ++ fullResults)
  }

  /**
    * Try to complete the delayed operation by first checking if the operation
    * can be completed by now. If yes execute the completion logic by calling
    * forceComplete() and return true iff forceComplete returns true; otherwise return false
    *
    * This function needs to be defined in subclasses
    */
  override def tryComplete(): Boolean = {
    updateWaiting()
    debug(s"tryComplete() waitingPartitions: $waitingPartitions")
    waitingPartitions.isEmpty && forceComplete()
  }

  private def updateWaiting(): Unit = {
    val metadataCache = replicaManager.metadataCache
    val completedPartitions = waitingPartitions.collect {
      case (tp, leader) if metadataCache.getPartitionInfo(tp.topic, tp.partition).exists(_.leader == leader) => tp
    }
    completedPartitions.foreach  { tp =>
      waitingPartitions -= tp
      fullResults += tp -> ApiError.NONE
    }
  }

}
