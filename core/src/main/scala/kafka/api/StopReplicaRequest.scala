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
import kafka.utils.Logging
import kafka.network.InvalidRequestException


object StopReplicaRequest extends Logging {
  val CurrentVersion = 0.shortValue
  val DefaultClientId = ""
  val DefaultAckTimeout = 100

  def readFrom(buffer: ByteBuffer): StopReplicaRequest = {
    val versionId = buffer.getShort
    val correlationId = buffer.getInt
    val clientId = readShortString(buffer)
    val ackTimeoutMs = buffer.getInt
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
    StopReplicaRequest(versionId, correlationId, clientId, ackTimeoutMs, deletePartitions, topicPartitionPairSet.toSet, controllerEpoch)
  }
}

case class StopReplicaRequest(versionId: Short,
                              correlationId: Int,
                              clientId: String,
                              ackTimeoutMs: Int,
                              deletePartitions: Boolean,
                              partitions: Set[(String, Int)],
                              controllerEpoch: Int)
        extends RequestOrResponse(Some(RequestKeys.StopReplicaKey)) {

  def this(deletePartitions: Boolean, partitions: Set[(String, Int)], controllerEpoch: Int) = {
    this(StopReplicaRequest.CurrentVersion, 0, StopReplicaRequest.DefaultClientId, StopReplicaRequest.DefaultAckTimeout,
         deletePartitions, partitions, controllerEpoch)
  }

  def writeTo(buffer: ByteBuffer) {
    buffer.putShort(versionId)
    buffer.putInt(correlationId)
    writeShortString(buffer, clientId)
    buffer.putInt(ackTimeoutMs)
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
      4 + /* controller epoch */
      1 + /* deletePartitions */
      4 /* partition count */
    for ((topic, partitionId) <- partitions){
      size += (ApiUtils.shortStringLength(topic)) +
              4 /* partition id */
    }
    size
  }
}
