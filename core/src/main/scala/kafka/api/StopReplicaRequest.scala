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
import kafka.network.{BoundedByteBufferSend, RequestChannel, InvalidRequestException}
import kafka.common.ErrorMapping
import kafka.network.RequestChannel.Response
import kafka.utils.Logging


object StopReplicaRequest extends Logging {
  val CurrentVersion = 0.shortValue
  val DefaultClientId = ""
  val DefaultAckTimeout = 100

  def readFrom(buffer: ByteBuffer): StopReplicaRequest = {
    val versionId = buffer.getShort
    val correlationId = buffer.getInt
    val clientId = readShortString(buffer)
    val ackTimeoutMs = buffer.getInt
    val controllerId = buffer.getInt
    val controllerEpoch = buffer.getInt
    val deletePartitions = buffer.get match {
      case 1 => true
      case 0 => false
      case x =>
        throw new InvalidRequestException("Invalid byte %d in delete partitions field. (Assuming false.)".format(x))
    }
    val topicPartitionPairCount = buffer.getInt
    val topicPartitionPairSet = new collection.mutable.HashSet[(String, Int)]()
    (1 to topicPartitionPairCount) foreach { _ =>
      topicPartitionPairSet.add(readShortString(buffer), buffer.getInt)
    }
    StopReplicaRequest(versionId, correlationId, clientId, ackTimeoutMs, controllerId, controllerEpoch,
                       deletePartitions, topicPartitionPairSet.toSet)
  }
}

case class StopReplicaRequest(versionId: Short,
                              override val correlationId: Int,
                              clientId: String,
                              ackTimeoutMs: Int,
                              controllerId: Int,
                              controllerEpoch: Int,
                              deletePartitions: Boolean,
                              partitions: Set[(String, Int)])
        extends RequestOrResponse(Some(RequestKeys.StopReplicaKey), correlationId) {

  def this(deletePartitions: Boolean, partitions: Set[(String, Int)], controllerId: Int, controllerEpoch: Int, correlationId: Int) = {
    this(StopReplicaRequest.CurrentVersion, correlationId, StopReplicaRequest.DefaultClientId, StopReplicaRequest.DefaultAckTimeout,
         controllerId, controllerEpoch, deletePartitions, partitions)
  }

  def writeTo(buffer: ByteBuffer) {
    buffer.putShort(versionId)
    buffer.putInt(correlationId)
    writeShortString(buffer, clientId)
    buffer.putInt(ackTimeoutMs)
    buffer.putInt(controllerId)
    buffer.putInt(controllerEpoch)
    buffer.put(if (deletePartitions) 1.toByte else 0.toByte)
    buffer.putInt(partitions.size)
    for ((topic, partitionId) <- partitions){
      writeShortString(buffer, topic)
      buffer.putInt(partitionId)
    }
  }

  def sizeInBytes(): Int = {
    var size =
      2 + /* versionId */
      4 + /* correlation id */
      ApiUtils.shortStringLength(clientId) +
      4 + /* ackTimeoutMs */
      4 + /* controller id*/
      4 + /* controller epoch */
      1 + /* deletePartitions */
      4 /* partition count */
    for ((topic, partitionId) <- partitions){
      size += (ApiUtils.shortStringLength(topic)) +
              4 /* partition id */
    }
    size
  }

  override def toString(): String = {
    val stopReplicaRequest = new StringBuilder
    stopReplicaRequest.append("Name: " + this.getClass.getSimpleName)
    stopReplicaRequest.append("; Version: " + versionId)
    stopReplicaRequest.append("; CorrelationId: " + correlationId)
    stopReplicaRequest.append("; ClientId: " + clientId)
    stopReplicaRequest.append("; AckTimeoutMs: " + ackTimeoutMs + " ms")
    stopReplicaRequest.append("; DeletePartitions: " + deletePartitions)
    stopReplicaRequest.append("; ControllerId: " + controllerId)
    stopReplicaRequest.append("; ControllerEpoch: " + controllerEpoch)
    stopReplicaRequest.append("; Partitions: " + partitions.mkString(","))
    stopReplicaRequest.toString()
  }

  override  def handleError(e: Throwable, requestChannel: RequestChannel, request: RequestChannel.Request): Unit = {
    val responseMap = partitions.map {
      case topicAndPartition => (topicAndPartition, ErrorMapping.codeFor(e.getClass.asInstanceOf[Class[Throwable]]))
    }.toMap
    val errorResponse = StopReplicaResponse(correlationId, responseMap)
    requestChannel.sendResponse(new Response(request, new BoundedByteBufferSend(errorResponse)))
  }
}
