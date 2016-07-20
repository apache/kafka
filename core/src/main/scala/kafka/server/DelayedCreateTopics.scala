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

import kafka.api.LeaderAndIsr
import org.apache.kafka.common.protocol.Errors

import scala.collection._

/**
  * The create metadata maintained by the delayed create operation
  *
  * TODO: local state doesn't count, need to know state of all relevant brokers
  *
  */
case class CreateTopicMetadata(topic: String, replicaAssignments: Map[Int, Seq[Int]], error: Errors)

/**
  * A delayed create topics operation that can be created by the admin manager and watched
  * in the topic purgatory
  */
class DelayedCreateTopics(delayMs: Long,
                          createMetadata: Seq[CreateTopicMetadata],
                          adminManager: AdminManager,
                          responseCallback: Map[String, Errors] => Unit)
  extends DelayedOperation(delayMs) {

  /**
    * The operation can be completed if all of the topics that do not have an error exist and every partition has a leader in the controller.
    * See KafkaController.onNewTopicCreation
    */
  override def tryComplete() : Boolean = {
    trace(s"Trying to complete operation for $createMetadata")

    val leaderlessPartitionCount = createMetadata.filter(_.error == Errors.NONE)
      .foldLeft(0) { case (topicCounter, metadata) =>
        topicCounter + missingLeaderCount(metadata.topic, metadata.replicaAssignments.keySet)
      }

    if (leaderlessPartitionCount == 0) {
      trace("All partitions have a leader, completing the delayed operation")
      forceComplete()
    } else {
      trace(s"$leaderlessPartitionCount partitions do not have a leader, not completing the delayed operation")
      false
    }
  }

  /**
    * Check for partitions that are still missing a leader, update their error code and call the responseCallback
    */
  override def onComplete() {
    trace(s"Completing operation for $createMetadata")
    val results = createMetadata.map { metadata =>
      // ignore topics that already have errors
      if (metadata.error == Errors.NONE && missingLeaderCount(metadata.topic, metadata.replicaAssignments.keySet) > 0)
        (metadata.topic, Errors.REQUEST_TIMED_OUT)
      else
        (metadata.topic, metadata.error)
    }.toMap
    responseCallback(results)
  }

  override def onExpiration(): Unit = { }

  private def missingLeaderCount(topic: String, partitions: Set[Int]): Int = {
    partitions.foldLeft(0) { case (counter, partition) =>
      if (isMissingLeader(topic, partition)) counter + 1 else counter
    }
  }

  private def isMissingLeader(topic: String, partition: Int): Boolean = {
    val partitionInfo = adminManager.metadataCache.getPartitionInfo(topic, partition)
    partitionInfo.isEmpty || partitionInfo.get.leaderIsrAndControllerEpoch.leaderAndIsr.leader == LeaderAndIsr.NoLeader
  }
}
