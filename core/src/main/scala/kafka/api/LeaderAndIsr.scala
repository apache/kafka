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


package kafka.api

import java.nio._

import kafka.controller.LeaderIsrAndControllerEpoch
import kafka.utils._

import scala.collection.Set

object LeaderAndIsr {
  val initialLeaderEpoch: Int = 0
  val initialZKVersion: Int = 0
  val NoLeader = -1
  val LeaderDuringDelete = -2
}

case class LeaderAndIsr(var leader: Int, var leaderEpoch: Int, var isr: List[Int], var zkVersion: Int) {
  def this(leader: Int, isr: List[Int]) = this(leader, LeaderAndIsr.initialLeaderEpoch, isr, LeaderAndIsr.initialZKVersion)

  override def toString: String = {
    Json.encode(Map("leader" -> leader, "leader_epoch" -> leaderEpoch, "isr" -> isr))
  }
}

object PartitionStateInfo {
  def readFrom(buffer: ByteBuffer): PartitionStateInfo = {
    val controllerEpoch = buffer.getInt
    val leader = buffer.getInt
    val leaderEpoch = buffer.getInt
    val isrSize = buffer.getInt
    val isr = for(i <- 0 until isrSize) yield buffer.getInt
    val zkVersion = buffer.getInt
    val replicationFactor = buffer.getInt
    val replicas = for(i <- 0 until replicationFactor) yield buffer.getInt
    PartitionStateInfo(LeaderIsrAndControllerEpoch(LeaderAndIsr(leader, leaderEpoch, isr.toList, zkVersion), controllerEpoch),
                       replicas.toSet)
  }
}

case class PartitionStateInfo(leaderIsrAndControllerEpoch: LeaderIsrAndControllerEpoch,
                              allReplicas: Set[Int]) {
  def replicationFactor = allReplicas.size

  def writeTo(buffer: ByteBuffer) {
    buffer.putInt(leaderIsrAndControllerEpoch.controllerEpoch)
    buffer.putInt(leaderIsrAndControllerEpoch.leaderAndIsr.leader)
    buffer.putInt(leaderIsrAndControllerEpoch.leaderAndIsr.leaderEpoch)
    buffer.putInt(leaderIsrAndControllerEpoch.leaderAndIsr.isr.size)
    leaderIsrAndControllerEpoch.leaderAndIsr.isr.foreach(buffer.putInt(_))
    buffer.putInt(leaderIsrAndControllerEpoch.leaderAndIsr.zkVersion)
    buffer.putInt(replicationFactor)
    allReplicas.foreach(buffer.putInt(_))
  }

  def sizeInBytes(): Int = {
    val size =
      4 /* epoch of the controller that elected the leader */ +
      4 /* leader broker id */ +
      4 /* leader epoch */ +
      4 /* number of replicas in isr */ +
      4 * leaderIsrAndControllerEpoch.leaderAndIsr.isr.size /* replicas in isr */ +
      4 /* zk version */ +
      4 /* replication factor */ +
      allReplicas.size * 4
    size
  }

  override def toString: String = {
    val partitionStateInfo = new StringBuilder
    partitionStateInfo.append("(LeaderAndIsrInfo:" + leaderIsrAndControllerEpoch.toString)
    partitionStateInfo.append(",ReplicationFactor:" + replicationFactor + ")")
    partitionStateInfo.append(",AllReplicas:" + allReplicas.mkString(",") + ")")
    partitionStateInfo.toString()
  }
}
