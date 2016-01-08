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
import kafka.common.TopicAndPartition
import kafka.controller.{IsrChangeNotificationListener, LeaderIsrAndControllerEpoch}
import kafka.utils.ZkUtils._
import org.I0Itec.zkclient.ZkClient
import org.apache.zookeeper.data.Stat

import scala.collection._
import scala.collection.mutable.ArrayBuffer
object ReplicationUtils extends Logging {

  private val IsrChangeNotificationPrefix = "isr_change_"

  def updateLeaderAndIsr(zkUtils: ZkUtils, topic: String, partitionId: Int, newLeaderAndIsr: LeaderAndIsr, controllerEpoch: Int,
    zkVersion: Int): (Boolean,Int) = {
    debug("Updated ISR for partition [%s,%d] to %s".format(topic, partitionId, newLeaderAndIsr.isr.mkString(",")))
    val path = getTopicPartitionLeaderAndIsrPath(topic, partitionId)
    val newLeaderData = zkUtils.leaderAndIsrZkData(newLeaderAndIsr, controllerEpoch)
    // use the epoch of the controller that made the leadership decision, instead of the current controller epoch
    val updatePersistentPath: (Boolean, Int) = zkUtils.conditionalUpdatePersistentPath(path, newLeaderData, zkVersion, Some(checkLeaderAndIsrZkData))
    updatePersistentPath
  }

  /**
    * Async version of updateLeaderAndIsr
    */
  def updateLeaderAndIsrAsync(zkUtils: ZkUtils, topic: String, partitionId: Int, newLeaderAndIsr: LeaderAndIsr, controllerEpoch: Int,
                              zkVersion: Int, topCallback: (Boolean,Int,scala.Any) => Unit, topCtx: scala.Any) = {
    debug("Updated async ISR for partition [%s,%d] to %s".format(topic, partitionId, newLeaderAndIsr.isr.mkString(",")))
    val internalCtx = (topCallback, topCtx)
    def updateCallback(success: Boolean, newVersion: Int, internalCtx: scala.Any): Unit = {
      val (topCallback, topCtx) = internalCtx.asInstanceOf[((Boolean,Int,scala.Any) => Unit, scala.Any)]
      topCallback(success, newVersion, topCtx)
    }
    val path = getTopicPartitionLeaderAndIsrPath(topic, partitionId)
    val newLeaderData = zkUtils.leaderAndIsrZkData(newLeaderAndIsr, controllerEpoch)
    // use the epoch of the controller that made the leadership decision, instead of the current controller epoch
    zkUtils.conditionalUpdatePersistentPathAsync(path, newLeaderData, zkVersion, Some(checkLeaderAndIsrZkDataAsync), updateCallback, internalCtx)
  }


  def propagateIsrChanges(zkUtils: ZkUtils, isrChangeSet: Set[TopicAndPartition]): Unit = {
    val isrChangeNotificationPath: String = zkUtils.createSequentialPersistentPath(
      ZkUtils.IsrChangeNotificationPath + "/" + IsrChangeNotificationPrefix,
      generateIsrChangeJson(isrChangeSet))
    debug("Added " + isrChangeNotificationPath + " for " + isrChangeSet)
  }

  def checkLeaderAndIsrZkData(zkUtils: ZkUtils, path: String, expectedLeaderAndIsrInfo: String): (Boolean,Int) = {
    try {
      val writtenLeaderAndIsrInfo = zkUtils.readDataMaybeNull(path)
      val writtenLeaderOpt = writtenLeaderAndIsrInfo._1
      val writtenStat = writtenLeaderAndIsrInfo._2
      val expectedLeader = parseLeaderAndIsr(expectedLeaderAndIsrInfo, path, writtenStat)
      writtenLeaderOpt match {
        case Some(writtenData) =>
          val writtenLeader = parseLeaderAndIsr(writtenData, path, writtenStat)
          (expectedLeader,writtenLeader) match {
            case (Some(expectedLeader),Some(writtenLeader)) =>
              if(expectedLeader == writtenLeader)
                return (true,writtenStat.getVersion())
            case _ =>
          }
        case None =>
      }
    } catch {
      case e1: Exception =>
    }
    (false,-1)
  }

  def checkLeaderAndIsrZkDataAsync(zkUtils: ZkUtils, path: String, expectedLeaderAndIsrInfo: String,
      topCallback: (Boolean,Int,scala.Any) => Unit, topCtx: scala.Any) = {
    val internalCtx = (topCallback, topCtx)
    def internalCallback(writtenLeaderOpt: Option[String], stat: Stat, internalCtx: scala.Any): Unit = {
      val (topCallback, topCtx) = internalCtx.asInstanceOf[((Boolean, Int, scala.Any) => Unit, scala.Any)]
      val expectedLeader = parseLeaderAndIsr(expectedLeaderAndIsrInfo, path, stat)
      writtenLeaderOpt match {
        case Some(writtenData) =>
          val writtenLeader = parseLeaderAndIsr(writtenData, path, stat)
          (expectedLeader, writtenLeader) match {
            case (Some(expectedLeader), Some(writtenLeader)) =>
              if (expectedLeader == writtenLeader)
                return topCallback(true, stat.getVersion(), topCtx)
            case _ =>
          }
        case None =>
      }
      return topCallback(false, -1, topCtx)
    }
    zkUtils.readDataMaybeNullAsync(path, internalCallback, internalCtx)
  }

  def getLeaderIsrAndEpochForPartition(zkUtils: ZkUtils, topic: String, partition: Int):Option[LeaderIsrAndControllerEpoch] = {
    val leaderAndIsrPath = getTopicPartitionLeaderAndIsrPath(topic, partition)
    val (leaderAndIsrOpt, stat) = zkUtils.readDataMaybeNull(leaderAndIsrPath)
    leaderAndIsrOpt.flatMap(leaderAndIsrStr => parseLeaderAndIsr(leaderAndIsrStr, leaderAndIsrPath, stat))
  }

  private def parseLeaderAndIsr(leaderAndIsrStr: String, path: String, stat: Stat)
      : Option[LeaderIsrAndControllerEpoch] = {
    Json.parseFull(leaderAndIsrStr).flatMap {m =>
      val leaderIsrAndEpochInfo = m.asInstanceOf[Map[String, Any]]
      val leader = leaderIsrAndEpochInfo.get("leader").get.asInstanceOf[Int]
      val epoch = leaderIsrAndEpochInfo.get("leader_epoch").get.asInstanceOf[Int]
      val isr = leaderIsrAndEpochInfo.get("isr").get.asInstanceOf[List[Int]]
      val controllerEpoch = leaderIsrAndEpochInfo.get("controller_epoch").get.asInstanceOf[Int]
      val zkPathVersion = stat.getVersion
      debug("Leader %d, Epoch %d, Isr %s, Zk path version %d for leaderAndIsrPath %s".format(leader, epoch,
        isr.toString(), zkPathVersion, path))
      Some(LeaderIsrAndControllerEpoch(LeaderAndIsr(leader, epoch, isr, zkPathVersion), controllerEpoch))}
  }

  private def generateIsrChangeJson(isrChanges: Set[TopicAndPartition]): String = {
    val partitions = isrChanges.map(tp => Map("topic" -> tp.topic, "partition" -> tp.partition)).toArray
    Json.encode(Map("version" -> IsrChangeNotificationListener.version, "partitions" -> partitions))
  }

}
