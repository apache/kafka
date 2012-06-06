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


object LeaderAndISR {
  def readFrom(buffer: ByteBuffer): LeaderAndISR = {
    val leader = buffer.getInt
    val leaderGenId = buffer.getInt
    val ISRString = Utils.readShortString(buffer, "UTF-8")
    val ISR = ISRString.split(",").map(_.toInt).toList
    val zkVersion = buffer.getLong
    new LeaderAndISR(leader, leaderGenId, ISR, zkVersion)
  }
}

case class LeaderAndISR(leader: Int, leaderEpoc: Int, ISR: List[Int], zkVersion: Long){
  def writeTo(buffer: ByteBuffer) {
    buffer.putInt(leader)
    buffer.putInt(leaderEpoc)
    Utils.writeShortString(buffer, ISR.mkString(","), "UTF-8")
    buffer.putLong(zkVersion)
  }

  def sizeInBytes(): Int = {
    val size = 4 + 4 + (2 + ISR.mkString(",").length) + 8
    size
  }
}


object LeaderAndISRRequest {
  val CurrentVersion = 1.shortValue()
  val DefaultClientId = ""

  def readFrom(buffer: ByteBuffer): LeaderAndISRRequest = {
    val versionId = buffer.getShort
    val clientId = Utils.readShortString(buffer)
    val isInit = buffer.get()
    val ackTimeout = buffer.getInt
    val leaderAndISRRequestCount = buffer.getInt
    val leaderAndISRInfos = new HashMap[(String, Int), LeaderAndISR]

    for(i <- 0 until leaderAndISRRequestCount){
      val topic = Utils.readShortString(buffer, "UTF-8")
      val partition = buffer.getInt
      val leaderAndISRRequest = LeaderAndISR.readFrom(buffer)

      leaderAndISRInfos.put((topic, partition), leaderAndISRRequest)
    }
    new LeaderAndISRRequest(versionId, clientId, isInit, ackTimeout, leaderAndISRInfos)
  }
}


case class LeaderAndISRRequest (versionId: Short,
                                clientId: String,
                                isInit: Byte,
                                ackTimeout: Int,
                                leaderAndISRInfos:
                                Map[(String, Int), LeaderAndISR])
        extends RequestOrResponse(Some(RequestKeys.LeaderAndISRRequest)) {
  def this(isInit: Byte, ackTimeout: Int, leaderAndISRInfos: Map[(String, Int), LeaderAndISR]) = {
    this(LeaderAndISRRequest.CurrentVersion, LeaderAndISRRequest.DefaultClientId, isInit, ackTimeout, leaderAndISRInfos)
  }

  def writeTo(buffer: ByteBuffer) {
    buffer.putShort(versionId)
    Utils.writeShortString(buffer, clientId)
    buffer.put(isInit)
    buffer.putInt(ackTimeout)
    buffer.putInt(leaderAndISRInfos.size)
    for((key, value) <- leaderAndISRInfos){
      Utils.writeShortString(buffer, key._1, "UTF-8")
      buffer.putInt(key._2)
      value.writeTo(buffer)
    }
  }

  def sizeInBytes(): Int = {
    var size = 1 + 2 + (2 + clientId.length) + 4 + 4
    for((key, value) <- leaderAndISRInfos)
      size += (2 + key._1.length) + 4 + value.sizeInBytes
    size
  }
}