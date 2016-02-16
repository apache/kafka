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

import kafka.api.ApiUtils._
import kafka.cluster.BrokerEndPoint
import kafka.common.ErrorMapping
import kafka.controller.LeaderIsrAndControllerEpoch
import kafka.network.{RequestOrResponseSend, RequestChannel}
import kafka.network.RequestChannel.Response
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

  override def toString(): String = {
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

  override def toString(): String = {
    val partitionStateInfo = new StringBuilder
    partitionStateInfo.append("(LeaderAndIsrInfo:" + leaderIsrAndControllerEpoch.toString)
    partitionStateInfo.append(",ReplicationFactor:" + replicationFactor + ")")
    partitionStateInfo.append(",AllReplicas:" + allReplicas.mkString(",") + ")")
    partitionStateInfo.toString()
  }
}

object LeaderAndIsrRequest {
  val CurrentVersion = 0.shortValue
  val IsInit: Boolean = true
  val NotInit: Boolean = false
  val DefaultAckTimeout: Int = 1000

  def readFrom(buffer: ByteBuffer): LeaderAndIsrRequest = {
    val versionId = buffer.getShort
    val correlationId = buffer.getInt
    val clientId = Option(readShortString(buffer)).getOrElse("")
    val controllerId = buffer.getInt
    val controllerEpoch = buffer.getInt
    val partitionStateInfosCount = buffer.getInt
    val partitionStateInfos = new collection.mutable.HashMap[(String, Int), PartitionStateInfo]

    for(i <- 0 until partitionStateInfosCount){
      val topic = readShortString(buffer)
      val partition = buffer.getInt
      val partitionStateInfo = PartitionStateInfo.readFrom(buffer)

      partitionStateInfos.put((topic, partition), partitionStateInfo)
    }

    val leadersCount = buffer.getInt
    var leaders = Set[BrokerEndPoint]()
    for (i <- 0 until leadersCount)
      leaders += BrokerEndPoint.readFrom(buffer)

    new LeaderAndIsrRequest(versionId, correlationId, clientId, controllerId, controllerEpoch, partitionStateInfos.toMap, leaders)
  }
}

case class LeaderAndIsrRequest (versionId: Short,
                                correlationId: Int,
                                clientId: String,
                                controllerId: Int,
                                controllerEpoch: Int,
                                partitionStateInfos: Map[(String, Int), PartitionStateInfo],
                                leaders: Set[BrokerEndPoint])
    extends RequestOrResponse(Some(RequestKeys.LeaderAndIsrKey)) {

  def this(partitionStateInfos: Map[(String, Int), PartitionStateInfo], leaders: Set[BrokerEndPoint], controllerId: Int,
           controllerEpoch: Int, correlationId: Int, clientId: String) = {
    this(LeaderAndIsrRequest.CurrentVersion, correlationId, clientId,
         controllerId, controllerEpoch, partitionStateInfos, leaders)
  }

  def writeTo(buffer: ByteBuffer) {
    buffer.putShort(versionId)
    buffer.putInt(correlationId)
    writeShortString(buffer, clientId)
    buffer.putInt(controllerId)
    buffer.putInt(controllerEpoch)
    buffer.putInt(partitionStateInfos.size)
    for((key, value) <- partitionStateInfos){
      writeShortString(buffer, key._1)
      buffer.putInt(key._2)
      value.writeTo(buffer)
    }
    buffer.putInt(leaders.size)
    leaders.foreach(_.writeTo(buffer))
  }

  def sizeInBytes(): Int = {
    var size =
      2 /* version id */ +
      4 /* correlation id */ +
      (2 + clientId.length) /* client id */ +
      4 /* controller id */ +
      4 /* controller epoch */ +
      4 /* number of partitions */
    for((key, value) <- partitionStateInfos)
      size += (2 + key._1.length) /* topic */ + 4 /* partition */ + value.sizeInBytes /* partition state info */
    size += 4 /* number of leader brokers */
    for(broker <- leaders)
      size += broker.sizeInBytes /* broker info */
    size
  }

  override def toString(): String = {
    describe(true)
  }

  override  def handleError(e: Throwable, requestChannel: RequestChannel, request: RequestChannel.Request): Unit = {
    val responseMap = partitionStateInfos.map {
      case (topicAndPartition, partitionAndState) => (topicAndPartition, ErrorMapping.codeFor(e.getClass.asInstanceOf[Class[Throwable]]))
    }
    val errorResponse = LeaderAndIsrResponse(correlationId, responseMap)
    requestChannel.sendResponse(new Response(request, new RequestOrResponseSend(request.connectionId, errorResponse)))
  }

  override def describe(details: Boolean): String = {
    val leaderAndIsrRequest = new StringBuilder
    leaderAndIsrRequest.append("Name:" + this.getClass.getSimpleName)
    leaderAndIsrRequest.append(";Version:" + versionId)
    leaderAndIsrRequest.append(";Controller:" + controllerId)
    leaderAndIsrRequest.append(";ControllerEpoch:" + controllerEpoch)
    leaderAndIsrRequest.append(";CorrelationId:" + correlationId)
    leaderAndIsrRequest.append(";ClientId:" + clientId)
    leaderAndIsrRequest.append(";Leaders:" + leaders.mkString(","))
    if(details)
      leaderAndIsrRequest.append(";PartitionState:" + partitionStateInfos.mkString(","))
    leaderAndIsrRequest.toString()
  }
}
