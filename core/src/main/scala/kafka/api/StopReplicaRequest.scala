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
import kafka.network.{RequestOrResponseSend, RequestChannel, InvalidRequestException}
import kafka.common.{TopicAndPartition, ErrorMapping}
import kafka.network.RequestChannel.Response
import kafka.utils.Logging
import collection.Set


object StopReplicaRequest extends Logging {
  val CurrentVersion = 0.shortValue
  val DefaultClientId = ""
  val DefaultAckTimeout = 100

  def readFrom(buffer: ByteBuffer): StopReplicaRequest = {
    val versionId = buffer.getShort
    val correlationId = buffer.getInt
    val clientId = Option(readShortString(buffer)).getOrElse("")
    val controllerId = buffer.getInt
    val controllerEpoch = buffer.getInt
    val deletePartitions = buffer.get match {
      case 1 => true
      case 0 => false
      case x =>
        throw new InvalidRequestException("Invalid byte %d in delete partitions field. (Assuming false.)".format(x))
    }
    val topicPartitionPairCount = buffer.getInt
    val topicPartitionPairSet = new collection.mutable.HashSet[TopicAndPartition]()
    (1 to topicPartitionPairCount) foreach { _ =>
      topicPartitionPairSet.add(TopicAndPartition(readShortString(buffer), buffer.getInt))
    }
    StopReplicaRequest(versionId, correlationId, clientId, controllerId, controllerEpoch,
                       deletePartitions, topicPartitionPairSet.toSet)
  }
}

case class StopReplicaRequest(versionId: Short,
                              correlationId: Int,
                              clientId: String,
                              controllerId: Int,
                              controllerEpoch: Int,
                              deletePartitions: Boolean,
                              partitions: Set[TopicAndPartition])
        extends RequestOrResponse(Some(RequestKeys.StopReplicaKey)) {

  def this(deletePartitions: Boolean, partitions: Set[TopicAndPartition], controllerId: Int, controllerEpoch: Int, correlationId: Int) = {
    this(StopReplicaRequest.CurrentVersion, correlationId, StopReplicaRequest.DefaultClientId,
         controllerId, controllerEpoch, deletePartitions, partitions)
  }

  def writeTo(buffer: ByteBuffer) {
    buffer.putShort(versionId)
    buffer.putInt(correlationId)
    writeShortString(buffer, clientId)
    buffer.putInt(controllerId)
    buffer.putInt(controllerEpoch)
    buffer.put(if (deletePartitions) 1.toByte else 0.toByte)
    buffer.putInt(partitions.size)
    for (topicAndPartition <- partitions) {
      writeShortString(buffer, topicAndPartition.topic)
      buffer.putInt(topicAndPartition.partition)
    }
  }

  def sizeInBytes(): Int = {
    var size =
      2 + /* versionId */
      4 + /* correlation id */
      ApiUtils.shortStringLength(clientId) +
      4 + /* controller id*/
      4 + /* controller epoch */
      1 + /* deletePartitions */
      4 /* partition count */
    for (topicAndPartition <- partitions){
      size += (ApiUtils.shortStringLength(topicAndPartition.topic)) +
              4 /* partition id */
    }
    size
  }

  override def toString(): String = {
    describe(true)
  }

  override  def handleError(e: Throwable, requestChannel: RequestChannel, request: RequestChannel.Request): Unit = {
    val responseMap = partitions.map {
      case topicAndPartition => (topicAndPartition, ErrorMapping.codeFor(e.getClass.asInstanceOf[Class[Throwable]]))
    }.toMap
    val errorResponse = StopReplicaResponse(correlationId, responseMap)
    requestChannel.sendResponse(new Response(request, new RequestOrResponseSend(request.connectionId, errorResponse)))
  }

  override def describe(details: Boolean): String = {
    val stopReplicaRequest = new StringBuilder
    stopReplicaRequest.append("Name: " + this.getClass.getSimpleName)
    stopReplicaRequest.append("; Version: " + versionId)
    stopReplicaRequest.append("; CorrelationId: " + correlationId)
    stopReplicaRequest.append("; ClientId: " + clientId)
    stopReplicaRequest.append("; DeletePartitions: " + deletePartitions)
    stopReplicaRequest.append("; ControllerId: " + controllerId)
    stopReplicaRequest.append("; ControllerEpoch: " + controllerEpoch)
    if(details)
      stopReplicaRequest.append("; Partitions: " + partitions.mkString(","))
    stopReplicaRequest.toString()
  }
}
