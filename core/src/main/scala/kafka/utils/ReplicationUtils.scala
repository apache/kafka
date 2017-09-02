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

import scala.collection._

object ReplicationUtils extends Logging {

  private val IsrChangeNotificationPrefix = "isr_change_"

  def updateLeaderAndIsr(zkUtils: ZkUtils, topic: String, partitionId: Int, newLeaderAndIsr: LeaderAndIsr, controllerEpoch: Int,
    zkVersion: Int): (Boolean,Int) = {
    debug(s"Updated ISR for $topic-$partitionId to ${newLeaderAndIsr.isr.mkString(",")}")
    val path = getTopicPartitionLeaderAndIsrPath(topic, partitionId)
    val newLeaderData = zkUtils.leaderAndIsrZkData(newLeaderAndIsr, controllerEpoch)
    // use the epoch of the controller that made the leadership decision, instead of the current controller epoch
    val updatePersistentPath: (Boolean, Int) = zkUtils.conditionalUpdatePersistentPath(path, newLeaderData, zkVersion, Some(checkLeaderAndIsrZkData))
    updatePersistentPath
  }

  def propagateIsrChanges(zkUtils: ZkUtils, isrChangeSet: Set[TopicPartition]): Unit = {
    val isrChangeNotificationPath: String = zkUtils.createSequentialPersistentPath(
      ZkUtils.IsrChangeNotificationPath + "/" + IsrChangeNotificationPrefix,
      generateIsrChangeJson(isrChangeSet))
    debug(s"Added $isrChangeNotificationPath for $isrChangeSet")
  }

  private def checkLeaderAndIsrZkData(zkUtils: ZkUtils, path: String, expectedLeaderAndIsrInfo: String): (Boolean, Int) = {
    try {
      val writtenLeaderAndIsrInfo = zkUtils.readDataMaybeNull(path)
      val writtenLeaderOpt = writtenLeaderAndIsrInfo._1
      val writtenStat = writtenLeaderAndIsrInfo._2
      val expectedLeader = parseLeaderAndIsr(expectedLeaderAndIsrInfo, path, writtenStat)
      writtenLeaderOpt.foreach { writtenData =>
        val writtenLeader = parseLeaderAndIsr(writtenData, path, writtenStat)
        (expectedLeader,writtenLeader) match {
          case (Some(expectedLeader),Some(writtenLeader)) =>
            if(expectedLeader == writtenLeader)
              return (true, writtenStat.getVersion())
          case _ =>
        }
      }
    } catch {
      case _: Exception =>
    }
    (false, -1)
  }

  def getLeaderIsrAndEpochForPartition(zkUtils: ZkUtils, topic: String, partition: Int): Option[LeaderIsrAndControllerEpoch] = {
    val leaderAndIsrPath = getTopicPartitionLeaderAndIsrPath(topic, partition)
    val (leaderAndIsrOpt, stat) = zkUtils.readDataMaybeNull(leaderAndIsrPath)
    debug(s"Read leaderISR $leaderAndIsrOpt for $topic-$partition")
    leaderAndIsrOpt.flatMap(leaderAndIsrStr => parseLeaderAndIsr(leaderAndIsrStr, leaderAndIsrPath, stat))
  }

  private def parseLeaderAndIsr(leaderAndIsrStr: String, path: String, stat: Stat): Option[LeaderIsrAndControllerEpoch] = {
    Json.parseFull(leaderAndIsrStr).flatMap { js =>
      val leaderIsrAndEpochInfo = js.asJsonObject
      val leader = leaderIsrAndEpochInfo("leader").to[Int]
      val epoch = leaderIsrAndEpochInfo("leader_epoch").to[Int]
      val isr = leaderIsrAndEpochInfo("isr").to[List[Int]]
      val controllerEpoch = leaderIsrAndEpochInfo("controller_epoch").to[Int]
      val zkPathVersion = stat.getVersion
      trace(s"Leader $leader, Epoch $epoch, Isr $isr, Zk path version $zkPathVersion for leaderAndIsrPath $path")
      Some(LeaderIsrAndControllerEpoch(LeaderAndIsr(leader, epoch, isr, zkPathVersion), controllerEpoch))}
  }

  private def generateIsrChangeJson(isrChanges: Set[TopicPartition]): String = {
    val partitions = isrChanges.map(tp => Map("topic" -> tp.topic, "partition" -> tp.partition)).toArray
    Json.encode(Map("version" -> IsrChangeNotificationListener.version, "partitions" -> partitions))
  }

}
