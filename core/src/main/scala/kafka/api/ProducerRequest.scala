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
import kafka.message._
import kafka.utils._


object ProducerRequest {
  val CurrentVersion: Short = 0
  
  def readFrom(buffer: ByteBuffer): ProducerRequest = {
    val versionId: Short = buffer.getShort
    val correlationId: Int = buffer.getInt
    val clientId: String = Utils.readShortString(buffer, "UTF-8")
    val requiredAcks: Short = buffer.getShort
    val ackTimeoutMs: Int = buffer.getInt
    //build the topic structure
    val topicCount = buffer.getInt
    val data = new Array[TopicData](topicCount)
    for(i <- 0 until topicCount) {
      val topic = Utils.readShortString(buffer, "UTF-8")
      		
      val partitionCount = buffer.getInt
      //build the partition structure within this topic
      val partitionData = new Array[PartitionData](partitionCount)
      for (j <- 0 until partitionCount) {
        val partition = buffer.getInt
        val messageSetSize = buffer.getInt
        val messageSetBuffer = new Array[Byte](messageSetSize)
        buffer.get(messageSetBuffer,0,messageSetSize)
        partitionData(j) = new PartitionData(partition,new ByteBufferMessageSet(ByteBuffer.wrap(messageSetBuffer)))
      }
      data(i) = new TopicData(topic,partitionData)
    }
    new ProducerRequest(versionId, correlationId, clientId, requiredAcks, ackTimeoutMs, data)
  }
}

case class ProducerRequest( versionId: Short,
                            correlationId: Int,
                            clientId: String,
                            requiredAcks: Short,
                            ackTimeoutMs: Int,
                            data: Array[TopicData] ) extends RequestOrResponse(Some(RequestKeys.ProduceKey)) {

  def this(correlationId: Int, clientId: String, requiredAcks: Short, ackTimeoutMs: Int, data: Array[TopicData]) =
    this(ProducerRequest.CurrentVersion, correlationId, clientId, requiredAcks, ackTimeoutMs, data)

  def writeTo(buffer: ByteBuffer) {
    buffer.putShort(versionId)
    buffer.putInt(correlationId)
    Utils.writeShortString(buffer, clientId, "UTF-8")
    buffer.putShort(requiredAcks)
    buffer.putInt(ackTimeoutMs)
    //save the topic structure
    buffer.putInt(data.size) //the number of topics
    for(topicData <- data) {
      Utils.writeShortString(buffer, topicData.topic, "UTF-8") //write the topic
      buffer.putInt(topicData.partitionDataArray.size) //the number of partitions
      for(partitionData <- topicData.partitionDataArray) {
        buffer.putInt(partitionData.partition)
        buffer.putInt(partitionData.messages.asInstanceOf[ByteBufferMessageSet].buffer.limit)
        buffer.put(partitionData.messages.asInstanceOf[ByteBufferMessageSet].buffer)
        partitionData.messages.asInstanceOf[ByteBufferMessageSet].buffer.rewind
      }
    }
  }

  def sizeInBytes(): Int = {
    var size = 0 
    //size, request_type_id, version_id, correlation_id, client_id, required_acks, ack_timeout, data.size
    size = 2 + 4 + 2 + clientId.length + 2 + 4 + 4
    for(topicData <- data) {
	    size += 2 + topicData.topic.length + 4
      for(partitionData <- topicData.partitionDataArray) {
        size += 4 + 4 + partitionData.messages.sizeInBytes.asInstanceOf[Int]
      }
    }
    size
  }

  // need to override case-class equals due to broken java-array equals()
  override def equals(other: Any): Boolean = {
   other match {
      case that: ProducerRequest =>
        ( correlationId == that.correlationId &&
          clientId == that.clientId &&
          requiredAcks == that.requiredAcks &&
          ackTimeoutMs == that.ackTimeoutMs &&
          data.toSeq == that.data.toSeq )
      case _ => false
    }
  }

  def topicPartitionCount = data.foldLeft(0)(_ + _.partitionDataArray.length)

}

