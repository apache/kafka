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

package kafka.utils

import kafka.api.LeaderAndIsr
import kafka.controller.LeaderIsrAndControllerEpoch
import kafka.zk._
import org.apache.kafka.common.TopicPartition

object ReplicationUtils extends Logging {

  def updateLeaderAndIsr(zkClient: KafkaZkClient, partition: TopicPartition, newLeaderAndIsr: LeaderAndIsr,
                         controllerEpoch: Int): (Boolean, Int) = {
    debug(s"Updated ISR for $partition to ${newLeaderAndIsr.isr.mkString(",")}")
    val path = TopicPartitionStateZNode.path(partition)
    val newLeaderData = TopicPartitionStateZNode.encode(LeaderIsrAndControllerEpoch(newLeaderAndIsr, controllerEpoch))
    // use the epoch of the controller that made the leadership decision, instead of the current controller epoch
    val updatePersistentPath: (Boolean, Int) = zkClient.conditionalUpdatePath(path, newLeaderData,
      newLeaderAndIsr.zkVersion, Some(checkLeaderAndIsrZkData))
    updatePersistentPath
  }

  private def checkLeaderAndIsrZkData(zkClient: KafkaZkClient, path: String, expectedLeaderAndIsrInfo: Array[Byte]): (Boolean, Int) = {
    try {
      val (writtenLeaderOpt, writtenStat) = zkClient.getDataAndStat(path)
      val expectedLeaderOpt = TopicPartitionStateZNode.decode(expectedLeaderAndIsrInfo, writtenStat)
      val succeeded = writtenLeaderOpt.exists { writtenData =>
        val writtenLeaderOpt = TopicPartitionStateZNode.decode(writtenData, writtenStat)
        (expectedLeaderOpt, writtenLeaderOpt) match {
          case (Some(expectedLeader), Some(writtenLeader)) if expectedLeader == writtenLeader => true
          case _ => false
        }
      }
      if (succeeded) (true, writtenStat.getVersion)
      else (false, -1)
    } catch {
      case _: Exception => (false, -1)
    }
  }

}
