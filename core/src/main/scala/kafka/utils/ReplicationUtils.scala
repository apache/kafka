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
import kafka.cluster.Replica
import kafka.api.LeaderAndIsr
import kafka.controller.LeaderIsrAndControllerEpoch
import org.apache.zookeeper.data.Stat
import org.I0Itec.zkclient.ZkClient

import scala.Some
import scala.collection._

object ReplicationUtils extends Logging {

  def updateIsr(zkClient: ZkClient, topic: String, partitionId: Int, brokerId: Int, leaderEpoch: Int,
    controllerEpoch: Int, zkVersion: Int, newIsr: Set[Replica]): (Boolean,Int) = {
    debug("Updated ISR for partition [%s,%d] to %s".format(topic, partitionId, newIsr.mkString(",")))
    val newLeaderAndIsr = new LeaderAndIsr(brokerId, leaderEpoch, newIsr.map(r => r.brokerId).toList, zkVersion)
    val path = ZkUtils.getTopicPartitionLeaderAndIsrPath(topic, partitionId)
    val newLeaderData = ZkUtils.leaderAndIsrZkData(newLeaderAndIsr, controllerEpoch)
    // use the epoch of the controller that made the leadership decision, instead of the current controller epoch
    ZkUtils.conditionalUpdatePersistentPath(zkClient, path, newLeaderData, zkVersion,
      Some(checkLeaderAndIsrZkData))
  }

  def checkLeaderAndIsrZkData(zkClient: ZkClient, path: String,newLeaderData: String, zkVersion: Int): (Boolean,Int) = {
    try {
      val newLeaderStat: Stat = new Stat()
      newLeaderStat.setVersion(zkVersion)
      val newLeader = parseLeaderAndIsr(newLeaderData, path, newLeaderStat)
      val writtenLeaderAndIsrInfo = ZkUtils.readDataMaybeNull(zkClient,path)
      val writtenLeaderOpt = writtenLeaderAndIsrInfo._1
      val writtenStat = writtenLeaderAndIsrInfo._2
      writtenLeaderOpt match {
        case Some(writtenData) =>
          val writtenLeader = parseLeaderAndIsr(writtenData, path, writtenStat)
            (newLeader,writtenLeader) match {
            case (Some(newLeader),Some(writtenLeader)) =>
              if(newLeader.equals(writtenLeader))
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

  def parseLeaderAndIsr(leaderAndIsrStr: String, path: String, stat: Stat)
      : Option[LeaderIsrAndControllerEpoch] = {
    Json.parseFull(leaderAndIsrStr) match {
      case Some(m) =>
        val leaderIsrAndEpochInfo = m.asInstanceOf[Map[String, Any]]
        val leader = leaderIsrAndEpochInfo.get("leader").get.asInstanceOf[Int]
        val epoch = leaderIsrAndEpochInfo.get("leader_epoch").get.asInstanceOf[Int]
        val isr = leaderIsrAndEpochInfo.get("isr").get.asInstanceOf[List[Int]]
        val controllerEpoch = leaderIsrAndEpochInfo.get("controller_epoch").get.asInstanceOf[Int]
        val zkPathVersion = stat.getVersion
        debug("Leader %d, Epoch %d, Isr %s, Zk path version %d for leaderAndIsrPath %s".format(leader, epoch,
          isr.toString(), zkPathVersion, path))
        Some(LeaderIsrAndControllerEpoch(LeaderAndIsr(leader, epoch, isr, zkPathVersion), controllerEpoch))
      case None => None
    }
  }

}
