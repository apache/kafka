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

/** A delayed elect preferred leader operation that can be created by the replica manager and watched
  * in the elect preferred leader purgatory
  */
class DelayedElectPreferredLeader(delayMs: Long,
                                  expectedLeaders: Map[TopicPartition, Int],
                                  results: Map[TopicPartition, ApiError],
                                  replicaManager: ReplicaManager,
                                  responseCallback: Map[TopicPartition, ApiError] => Unit)
    extends DelayedOperation(delayMs) {

  var waitingPartitions = expectedLeaders
  val fullResults = results.to[mutable.Set]


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
    val timedout = waitingPartitions.map{
      case (tp, leader) => tp -> new ApiError(Errors.REQUEST_TIMED_OUT, null)
    }.toMap
    responseCallback(timedout ++ fullResults)
  }

  private def timeoutWaiting = {
    waitingPartitions.map(partition => partition -> new ApiError(Errors.REQUEST_TIMED_OUT, null)).toMap
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

  private def updateWaiting() = {
    waitingPartitions.foreach{case (tp, leader) =>
      val ps = replicaManager.metadataCache.getPartitionInfo(tp.topic, tp.partition)
      ps match {
        case Some(ps) =>
          if (leader == ps.basePartitionState.leader) {
            waitingPartitions -= tp
            fullResults += tp -> ApiError.NONE
          }
        case None =>
      }
    }
  }

}
