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

object StopReplicaRequest {
  val CurrentVersion = 1.shortValue()
  val DefaultClientId = ""
  val DefaultAckTimeout = 100

  def readFrom(buffer: ByteBuffer): StopReplicaRequest = {
    val versionId = buffer.getShort
    val clientId = readShortString(buffer)
    val ackTimeoutMs = buffer.getInt
    val topicPartitionPairCount = buffer.getInt
    val topicPartitionPairSet = new collection.mutable.HashSet[(String, Int)]()
    for (i <- 0 until topicPartitionPairCount)
      topicPartitionPairSet.add(readShortString(buffer), buffer.getInt)
    new StopReplicaRequest(versionId, clientId, ackTimeoutMs, topicPartitionPairSet.toSet)
  }
}

case class StopReplicaRequest(versionId: Short,
                              clientId: String,
                              ackTimeoutMs: Int,
                              partitions: Set[(String, Int)])
        extends RequestOrResponse(Some(RequestKeys.StopReplicaKey)) {
  def this(partitions: Set[(String, Int)]) = {
    this(StopReplicaRequest.CurrentVersion, StopReplicaRequest.DefaultClientId, StopReplicaRequest.DefaultAckTimeout,
        partitions)
  }

  def writeTo(buffer: ByteBuffer) {
    buffer.putShort(versionId)
    writeShortString(buffer, clientId)
    buffer.putInt(ackTimeoutMs)
    buffer.putInt(partitions.size)
    for ((topic, partitionId) <- partitions){
      writeShortString(buffer, topic)
      buffer.putInt(partitionId)
    }
  }

  def sizeInBytes(): Int = {
    var size = 2 + (2 + clientId.length()) + 4 + 4
    for ((topic, partitionId) <- partitions){
      size += (2 + topic.length()) + 4
    }
    size
  }
}
