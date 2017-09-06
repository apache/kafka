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

import java.util.concurrent.atomic.AtomicReference

import kafka.common.TopicAndPartition
import kafka.controller.ControllerContext
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.ApiError

import scala.collection.{Map, Set, mutable}

/**
 * A delayed elect preferred leader operation that can be created by the replica manager and watched
 * in the elect preferred leader purgatory
 */
class DelayedElectPreferredLeader(delayMs: Long,
                                  waiting: Set[TopicAndPartition],
                                  results: Map[TopicAndPartition, ApiError],
                                  replicaManager: ReplicaManager,
                                  controllerContext: ControllerContext,
                                  responseCallback: Map[TopicAndPartition, ApiError] => Unit)
    extends DelayedOperation(delayMs) {

  val expectedLeaders: Map[TopicAndPartition, Int]= waiting.map(tp => tp -> controllerContext.partitionReplicaAssignment(tp).head).toMap
  var waitingPartitions: Set[TopicAndPartition] = new mutable.HashSet[TopicAndPartition]() ++= waiting
  val fullResults = new mutable.HashSet() ++= results

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
    val timedout = waitingPartitions.map(partition => partition -> new ApiError(Errors.REQUEST_TIMED_OUT, null)).toMap
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
    debug("tryComplete() waitingPartitions: %s".format(waitingPartitions))
    if (waitingPartitions.isEmpty)
      forceComplete()
    else
      false
  }

  private def updateWaiting() = {
    waitingPartitions.foreach(tp => {
        val ps = replicaManager.metadataCache.getPartitionInfo(tp.topic, tp.partition)
        ps match {
          case Some(ps) =>
            if (expectedLeaders(tp) == ps.basePartitionState.leader) {
              waitingPartitions -= tp
              fullResults += tp -> ApiError.NONE
            }
          case None =>
        }
      }
    )
  }
}
