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

import java.nio.ByteBuffer

import kafka.api.ApiUtils._
import kafka.cluster.{Broker, BrokerEndPoint}
import kafka.common.{ErrorMapping, KafkaException, TopicAndPartition}
import kafka.network.{RequestOrResponseSend, RequestChannel}
import kafka.network.RequestChannel.Response
import org.apache.kafka.common.protocol.SecurityProtocol

import scala.collection.Set

object UpdateMetadataRequest {
  val CurrentVersion = 1.shortValue
  val IsInit: Boolean = true
  val NotInit: Boolean = false
  val DefaultAckTimeout: Int = 1000

  def readFrom(buffer: ByteBuffer): UpdateMetadataRequest = {
    val versionId = buffer.getShort
    val correlationId = buffer.getInt
    val clientId = Option(readShortString(buffer)).getOrElse("")
    val controllerId = buffer.getInt
    val controllerEpoch = buffer.getInt
    val partitionStateInfosCount = buffer.getInt
    val partitionStateInfos = new collection.mutable.HashMap[TopicAndPartition, PartitionStateInfo]

    for(i <- 0 until partitionStateInfosCount){
      val topic = readShortString(buffer)
      val partition = buffer.getInt
      val partitionStateInfo = PartitionStateInfo.readFrom(buffer)

      partitionStateInfos.put(TopicAndPartition(topic, partition), partitionStateInfo)
    }

    val numAliveBrokers = buffer.getInt

    val aliveBrokers = versionId match {
      case 0 => for(i <- 0 until numAliveBrokers) yield new Broker(BrokerEndPoint.readFrom(buffer),SecurityProtocol.PLAINTEXT)
      case 1 => for(i <- 0 until numAliveBrokers) yield Broker.readFrom(buffer)
      case v => throw new KafkaException( "Version " + v.toString + " is invalid for UpdateMetadataRequest. Valid versions are 0 or 1.")
    }

    new UpdateMetadataRequest(versionId, correlationId, clientId, controllerId, controllerEpoch,
      partitionStateInfos.toMap, aliveBrokers.toSet)
  }
}

case class UpdateMetadataRequest (versionId: Short,
                                  correlationId: Int,
                                  clientId: String,
                                  controllerId: Int,
                                  controllerEpoch: Int,
                                  partitionStateInfos: Map[TopicAndPartition, PartitionStateInfo],
                                  aliveBrokers: Set[Broker])
  extends RequestOrResponse(Some(RequestKeys.UpdateMetadataKey)) {

  def this(controllerId: Int, controllerEpoch: Int, correlationId: Int, clientId: String,
           partitionStateInfos: Map[TopicAndPartition, PartitionStateInfo], aliveBrokers: Set[Broker]) = {
    this(UpdateMetadataRequest.CurrentVersion, correlationId, clientId,
      controllerId, controllerEpoch, partitionStateInfos, aliveBrokers)
  }

  def writeTo(buffer: ByteBuffer) {
    buffer.putShort(versionId)
    buffer.putInt(correlationId)
    writeShortString(buffer, clientId)
    buffer.putInt(controllerId)
    buffer.putInt(controllerEpoch)
    buffer.putInt(partitionStateInfos.size)
    for((key, value) <- partitionStateInfos){
      writeShortString(buffer, key.topic)
      buffer.putInt(key.partition)
      value.writeTo(buffer)
    }
    buffer.putInt(aliveBrokers.size)

    versionId match {
      case 0 => aliveBrokers.foreach(_.getBrokerEndPoint(SecurityProtocol.PLAINTEXT).writeTo(buffer))
      case 1 => aliveBrokers.foreach(_.writeTo(buffer))
      case v => throw new KafkaException( "Version " + v.toString + " is invalid for UpdateMetadataRequest. Valid versions are 0 or 1.")
    }
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
      size += (2 + key.topic.length) /* topic */ + 4 /* partition */ + value.sizeInBytes /* partition state info */
    size += 4 /* number of alive brokers in the cluster */

    versionId match  {
      case 0 => for(broker <- aliveBrokers)
        size += broker.getBrokerEndPoint(SecurityProtocol.PLAINTEXT).sizeInBytes /* broker info */
      case 1 => for(broker  <- aliveBrokers)
        size += broker.sizeInBytes
      case v => throw new KafkaException( "Version " + v.toString + " is invalid for UpdateMetadataRequest. Valid versions are 0 or 1.")
    }

    size
  }

  override def toString(): String = {
    describe(true)
  }

  override def handleError(e: Throwable, requestChannel: RequestChannel, request: RequestChannel.Request): Unit = {
    val errorResponse = new UpdateMetadataResponse(correlationId, ErrorMapping.codeFor(e.getClass.asInstanceOf[Class[Throwable]]))
    requestChannel.sendResponse(new Response(request, new RequestOrResponseSend(request.connectionId, errorResponse)))
  }

  override def describe(details: Boolean): String = {
    val updateMetadataRequest = new StringBuilder
    updateMetadataRequest.append("Name:" + this.getClass.getSimpleName)
    updateMetadataRequest.append(";Version:" + versionId)
    updateMetadataRequest.append(";Controller:" + controllerId)
    updateMetadataRequest.append(";ControllerEpoch:" + controllerEpoch)
    updateMetadataRequest.append(";CorrelationId:" + correlationId)
    updateMetadataRequest.append(";ClientId:" + clientId)
    updateMetadataRequest.append(";AliveBrokers:" + aliveBrokers.mkString(","))
    if(details)
      updateMetadataRequest.append(";PartitionState:" + partitionStateInfos.mkString(","))
    updateMetadataRequest.toString()
  }
}
