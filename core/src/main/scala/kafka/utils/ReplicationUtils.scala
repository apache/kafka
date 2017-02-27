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
import kafka.controller.{IsrChangeNotificationListener, LeaderIsrAndControllerEpoch}
import kafka.utils.ZkUtils._
import org.apache.kafka.common.TopicPartition
import org.apache.zookeeper.data.Stat

object ReplicationUtils extends Logging {

  private val IsrChangeNotificationPrefix = "isr_change_"

//  TODO: Rewrite to return Option[LeaderAndIsr]
  def updateLeaderAndIsr(zkUtils: ZkUtils,
                         topic: String,
                         partitionId: Int,
                         newLeaderAndIsr: LeaderAndIsr,
                         controllerEpoch: Int,
                         zkVersion: Int): (Boolean, Int) = {
    debug("Updated ISR for partition [%s,%d] to %s".format(topic, partitionId, newLeaderAndIsr.isr.mkString(",")))
    val path = getTopicPartitionLeaderAndIsrPath(topic, partitionId)
    val newLeaderData = zkUtils.leaderAndIsrZkData(newLeaderAndIsr, controllerEpoch)
    zkUtils.conditionalUpdatePersistentPath(path, newLeaderData, zkVersion, Some(checkLeaderAndIsrZkData))
  }

  def propagateIsrChanges(zkUtils: ZkUtils, isrChangeSet: Set[TopicPartition]): Unit = {
    val isrChangeNotificationPath: String = zkUtils.createSequentialPersistentPath(
      ZkUtils.IsrChangeNotificationPath + "/" + IsrChangeNotificationPrefix,
      generateIsrChangeJson(isrChangeSet)
    )
    debug(s"Added $isrChangeNotificationPath for $isrChangeSet")
  }

//  TODO: Return might be rewritten to Option[Int], where None = (false, -1), Some(i) = (true, i = writtenStat.getVersion)
  def checkLeaderAndIsrZkData(zkUtils: ZkUtils, path: String, expectedLeaderAndIsrInfo: String): (Boolean, Int) = {
    val (writtenLeaderOpt, writtenStat) = zkUtils.readDataMaybeNull(path)
    val expectedLeaderOpt = parseLeaderAndIsr(expectedLeaderAndIsrInfo, path, writtenStat)

    val ret = for {
      writtenData <- writtenLeaderOpt
      writtenLeader <- parseLeaderAndIsr(writtenData, path, writtenStat)
      expectedLeader <- expectedLeaderOpt if expectedLeader == writtenLeader
    } yield (true, writtenStat.getVersion)

    ret.getOrElse((false, -1))
  }

  def getLeaderIsrAndEpochForPartition(zkUtils: ZkUtils, topic: String, partition: Int): Option[LeaderIsrAndControllerEpoch] = {
    val leaderAndIsrPath = getTopicPartitionLeaderAndIsrPath(topic, partition)
    val (leaderAndIsrOpt, stat) = zkUtils.readDataMaybeNull(leaderAndIsrPath)
    leaderAndIsrOpt.flatMap(
      leaderAndIsrStr => parseLeaderAndIsr(leaderAndIsrStr, leaderAndIsrPath, stat)
    )
  }

  private def parseLeaderAndIsr(leaderAndIsrStr: String,
                                path: String,
                                stat: Stat): Option[LeaderIsrAndControllerEpoch] = {
    Json.parseFull(leaderAndIsrStr).map { m =>
      val leaderIsrAndEpochInfo = m.asInstanceOf[Map[String, Any]]
      val leader = leaderIsrAndEpochInfo.get("leader").get.asInstanceOf[Int]
      val epoch = leaderIsrAndEpochInfo.get("leader_epoch").get.asInstanceOf[Int]
      val isr = leaderIsrAndEpochInfo.get("isr").get.asInstanceOf[List[Int]]
      val controllerEpoch = leaderIsrAndEpochInfo.get("controller_epoch").get.asInstanceOf[Int]
      val zkPathVersion = stat.getVersion
      debug("Leader %d, Epoch %d, Isr %s, Zk path version %d for leaderAndIsrPath %s".format(leader, epoch,
        isr.toString(), zkPathVersion, path))
      LeaderIsrAndControllerEpoch(LeaderAndIsr(leader, epoch, isr, zkPathVersion), controllerEpoch)
    }
  }

  private def generateIsrChangeJson(isrChanges: Set[TopicPartition]): String = {
    val partitions = isrChanges.map(tp => Map("topic" -> tp.topic, "partition" -> tp.partition)).toArray
    Json.encode(Map("version" -> IsrChangeNotificationListener.version, "partitions" -> partitions))
  }

}
