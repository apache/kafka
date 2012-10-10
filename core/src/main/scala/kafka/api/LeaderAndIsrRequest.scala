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
import kafka.utils._
import collection.mutable.Map
import collection.mutable.HashMap


object LeaderAndIsr {
  val initialLeaderEpoch: Int = 0
  val initialZKVersion: Int = 0
}

case class LeaderAndIsr(var leader: Int, var leaderEpoch: Int, var isr: List[Int], var zkVersion: Int){
  def this(leader: Int, ISR: List[Int]) = this(leader, LeaderAndIsr.initialLeaderEpoch, ISR, LeaderAndIsr.initialZKVersion)

  override def toString(): String = {
    val jsonDataMap = new HashMap[String, String]
    jsonDataMap.put("leader", leader.toString)
    jsonDataMap.put("leaderEpoch", leaderEpoch.toString)
    jsonDataMap.put("ISR", isr.mkString(","))
    Utils.stringMapToJsonString(jsonDataMap)
  }
}


object PartitionInfo {
  def readFrom(buffer: ByteBuffer): PartitionInfo = {
    val leader = buffer.getInt
    val leaderGenId = buffer.getInt
    val ISRString = Utils.readShortString(buffer, "UTF-8")
    val ISR = ISRString.split(",").map(_.toInt).toList
    val zkVersion = buffer.getInt
    val replicationFactor = buffer.getInt
    PartitionInfo(LeaderAndIsr(leader, leaderGenId, ISR, zkVersion), replicationFactor)
  }
}

case class PartitionInfo(val leaderAndIsr: LeaderAndIsr, val replicationFactor: Int) {
  def writeTo(buffer: ByteBuffer) {
    buffer.putInt(leaderAndIsr.leader)
    buffer.putInt(leaderAndIsr.leaderEpoch)
    Utils.writeShortString(buffer, leaderAndIsr.isr.mkString(","), "UTF-8")
    buffer.putInt(leaderAndIsr.zkVersion)
    buffer.putInt(replicationFactor)
  }

  def sizeInBytes(): Int = {
    val size = 4 + 4 + (2 + leaderAndIsr.isr.mkString(",").length) + 4 + 4
    size
  }
}


object LeaderAndIsrRequest {
  val CurrentVersion = 1.shortValue()
  val DefaultClientId = ""
  val IsInit: Boolean = true
  val NotInit: Boolean = false
  val DefaultAckTimeout: Int = 1000

  def readFrom(buffer: ByteBuffer): LeaderAndIsrRequest = {
    val versionId = buffer.getShort
    val clientId = Utils.readShortString(buffer)
    val ackTimeoutMs = buffer.getInt
    val partitionInfosCount = buffer.getInt
    val partitionInfos = new HashMap[(String, Int), PartitionInfo]

    for(i <- 0 until partitionInfosCount){
      val topic = Utils.readShortString(buffer, "UTF-8")
      val partition = buffer.getInt
      val partitionInfo = PartitionInfo.readFrom(buffer)

      partitionInfos.put((topic, partition), partitionInfo)
    }
    new LeaderAndIsrRequest(versionId, clientId, ackTimeoutMs, partitionInfos)
  }
}


case class LeaderAndIsrRequest (versionId: Short,
                                clientId: String,
                                ackTimeoutMs: Int,
                                partitionInfos: Map[(String, Int), PartitionInfo])
        extends RequestOrResponse(Some(RequestKeys.LeaderAndIsrKey)) {

  def this(partitionInfos: Map[(String, Int), PartitionInfo]) = {
    this(LeaderAndIsrRequest.CurrentVersion, LeaderAndIsrRequest.DefaultClientId, LeaderAndIsrRequest.DefaultAckTimeout, partitionInfos)
  }

  def writeTo(buffer: ByteBuffer) {
    buffer.putShort(versionId)
    Utils.writeShortString(buffer, clientId)
    buffer.putInt(ackTimeoutMs)
    buffer.putInt(partitionInfos.size)
    for((key, value) <- partitionInfos){
      Utils.writeShortString(buffer, key._1, "UTF-8")
      buffer.putInt(key._2)
      value.writeTo(buffer)
    }
  }

  def sizeInBytes(): Int = {
    var size = 1 + 2 + (2 + clientId.length) + 4 + 4
    for((key, value) <- partitionInfos)
      size += (2 + key._1.length) + 4 + value.sizeInBytes
    size
  }
}