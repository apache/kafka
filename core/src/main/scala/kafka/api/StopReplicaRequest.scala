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
import collection.mutable.HashSet
import collection.mutable.Set

object StopReplicaRequest {
  val CurrentVersion = 1.shortValue()
  val DefaultClientId = ""

  def readFrom(buffer: ByteBuffer): StopReplicaRequest = {
    val versionId = buffer.getShort
    val clientId = Utils.readShortString(buffer)
    val ackTimeout = buffer.getInt
    val topicPartitionPairCount = buffer.getInt
    val topicPartitionPairSet = new HashSet[(String, Int)]()
    for (i <- 0 until topicPartitionPairCount){
      topicPartitionPairSet.add((Utils.readShortString(buffer, "UTF-8"), buffer.getInt))
    }
    new StopReplicaRequest(versionId, clientId, ackTimeout, topicPartitionPairSet)
  }
}

case class StopReplicaRequest(versionId: Short,
                              clientId: String,
                              ackTimeout: Int,
                              stopReplicaSet: Set[(String, Int)]
                                     ) extends RequestOrResponse(Some(RequestKeys.StopReplicaRequest)) {
  def this(ackTimeout: Int, stopReplicaSet: Set[(String, Int)]) = {
    this(StopReplicaRequest.CurrentVersion, StopReplicaRequest.DefaultClientId, ackTimeout, stopReplicaSet)
  }

  def writeTo(buffer: ByteBuffer) {
    buffer.putShort(versionId)
    Utils.writeShortString(buffer, clientId)
    buffer.putInt(ackTimeout)
    buffer.putInt(stopReplicaSet.size)
    for ((topic, partitionId) <- stopReplicaSet){
      Utils.writeShortString(buffer, topic, "UTF-8")
      buffer.putInt(partitionId)
    }
  }

  def sizeInBytes(): Int = {
    var size = 2 + (2 + clientId.length()) + 4 + 4
    for ((topic, partitionId) <- stopReplicaSet){
      size += (2 + topic.length()) + 4
    }
    size
  }
}