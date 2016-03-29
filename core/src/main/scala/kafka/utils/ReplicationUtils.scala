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

import java.util.concurrent.CountDownLatch

import kafka.api.LeaderAndIsr
import kafka.common.TopicAndPartition
import kafka.controller.{ZkLeaderAndIsrReadResult, ZkLeaderAndIsrUpdateResult, ZkLeaderAndIsrUpdateBatch, IsrChangeNotificationListener, LeaderIsrAndControllerEpoch}
import kafka.utils.ZkUtils._
import org.apache.zookeeper.data.Stat

import scala.collection._

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
   * Unlike the synchronous leader and isr update call which takes an optional checker function, the async leader and
   * isr update call does not verify if the leader and isr data is the same or not. The update will fail even if the
   * data are the same but the zk version mismatch. We do this because we cannot read data in the zookeeper event
   * thread.
   */
  def asyncUpdateLeaderAndIsr(zkUtils: ZkUtils,
                              leaderAndIsrUpdateBatch: ZkLeaderAndIsrUpdateBatch,
                              controllerEpoch: Int,
                              retryOnException: Option[Exception => Boolean] = None) = {
    val unprocessedUpdates = new CountDownLatch(leaderAndIsrUpdateBatch.size)
    leaderAndIsrUpdateBatch.leaderAndIsrUpdates.foreach { case (tp, update) => {
      val path = getTopicPartitionLeaderAndIsrPath(tp.topic, tp.partition)
      val newLeaderAndIsr = update.newLeaderAndIsr
      val newLeaderData = zkUtils.leaderAndIsrZkData(newLeaderAndIsr, controllerEpoch)
      zkUtils.asyncConditionalUpdatePersistentPath(path, newLeaderData, update.expectZkVersion,
        (updateSucceeded, updatedZkVersion) => {
          // Remove the successfully updated partitions
          // We do not handle failure and retry to make Zookeeper EventThread light weighted. The caller is
          // responsible to check if the update batch is finished or retry is needed.
          try {
            trace(s"Received LeaderAndIsr update callback of update $newLeaderAndIsr for $tp. " +
              s"UpdateSucceeded = $updateSucceeded")
            if (updateSucceeded) {
              leaderAndIsrUpdateBatch.completeLeaderAndIsrUpdate(tp)
              update.onSuccessCallbacks.foreach { case onSuccess =>
                onSuccess(tp, new ZkLeaderAndIsrUpdateResult(newLeaderAndIsr, updatedZkVersion))
              }
            }
          } finally {
            unprocessedUpdates.countDown()
          }
        }, retryOnException)
    }}
    unprocessedUpdates.await()
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

  def getLeaderIsrAndEpochForPartition(zkUtils: ZkUtils, topic: String, partition: Int):Option[LeaderIsrAndControllerEpoch] = {
    val leaderAndIsrPath = getTopicPartitionLeaderAndIsrPath(topic, partition)
    val (leaderAndIsrOpt, stat) = zkUtils.readDataMaybeNull(leaderAndIsrPath)
    leaderAndIsrOpt.flatMap(leaderAndIsrStr => parseLeaderAndIsr(leaderAndIsrStr, leaderAndIsrPath, stat))
  }

  def asyncGetLeaderIsrAndEpochForPartitions(zkUtils: ZkUtils,
                                             partitions: Set[TopicAndPartition]): Map[TopicAndPartition, ZkLeaderAndIsrReadResult] = {
    val unprocessedReads = new CountDownLatch(partitions.size)
    val zkLeaderAndIsrReadResults = new mutable.HashMap[TopicAndPartition, ZkLeaderAndIsrReadResult]
    partitions.foreach { case tap =>
      val leaderAndIsrPath = getTopicPartitionLeaderAndIsrPath(tap.topic, tap.partition)
      zkUtils.asyncReadDataMaybeNull(leaderAndIsrPath, new ZkReadCallback {
        override def handle(dataOpt: Option[String], stat: Stat, exceptionOpt: Option[Exception]): Unit = {
          try {
            zkLeaderAndIsrReadResults += tap -> {
                if (exceptionOpt.isEmpty) {
                  val leaderIsrControllerEpochOpt =
                    dataOpt.flatMap(leaderAndIsrStr => parseLeaderAndIsr(leaderAndIsrStr, leaderAndIsrPath, stat))
                  new ZkLeaderAndIsrReadResult(leaderIsrControllerEpochOpt, None)
                }
                else
                  new ZkLeaderAndIsrReadResult(None, exceptionOpt)
              }
          } finally {
            unprocessedReads.countDown()
          }
        }
      })
    }
    unprocessedReads.await()
    zkLeaderAndIsrReadResults
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
