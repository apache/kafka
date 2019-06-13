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
package kafka.controller

import kafka.zk.KafkaZkClient
import org.apache.kafka.common.TopicPartition

import scala.collection.mutable

class StateMachineUtil(controllerContext: ControllerContext, zkClient: KafkaZkClient) {

  def maybeUpdateLeaderAndIsr(partitions: Seq[TopicPartition],
                              stateChangeError: (TopicPartition, String) => Exception): Map[TopicPartition, Exception] = {
    val partitionStates = try {
      zkClient.fetchPartitionStates(partitions)
    } catch {
      case e: Exception =>
        return partitions.map(tp => tp -> e).toMap
    }

    val errors = mutable.Map.empty[TopicPartition, Exception]
    partitionStates.foreach {
      case (tp, Some(leaderIsrAndControllerEpoch)) =>
        if (leaderIsrAndControllerEpoch.controllerEpoch > controllerContext.epoch) {
          val failureReason = s"Leader and ISR path of $tp written by another controller. This probably " +
            s"means the current controller with epoch ${controllerContext.epoch} went through a soft failure and " +
            s"another controller was elected with epoch ${leaderIsrAndControllerEpoch.controllerEpoch}."
          errors.put(tp, stateChangeError(tp, failureReason))
        } else {
          controllerContext.partitionLeadershipInfo.put(tp, leaderIsrAndControllerEpoch)
        }

      case (tp, None) =>
        val failureReason = if (controllerContext.isTopicQueuedUpForDeletion(tp.topic)) {
          s"The topic ${tp.topic} is pending deletion."
        } else {
          s"The leader and ISR path in zookeeper is empty"
        }
        errors.put(tp, stateChangeError(tp, failureReason))
    }

    errors.toMap
  }

}
