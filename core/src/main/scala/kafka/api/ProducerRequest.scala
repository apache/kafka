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
import kafka.common._
import kafka.message._
import kafka.network.{RequestOrResponseSend, RequestChannel}
import kafka.network.RequestChannel.Response

object ProducerRequest {
  val CurrentVersion = 1.shortValue

  def readFrom(buffer: ByteBuffer): ProducerRequest = {
    val versionId: Short = buffer.getShort
    val correlationId: Int = buffer.getInt
    val clientId: String = Option(readShortString(buffer)).getOrElse("")
    val requiredAcks: Short = buffer.getShort
    val ackTimeoutMs: Int = buffer.getInt
    //build the topic structure
    val topicCount = buffer.getInt
    val partitionDataPairs = (1 to topicCount).flatMap(_ => {
      // process topic
      val topic = readShortString(buffer)
      val partitionCount = buffer.getInt
      (1 to partitionCount).map(_ => {
        val partition = buffer.getInt
        val messageSetSize = buffer.getInt
        val messageSetBuffer = new Array[Byte](messageSetSize)
        buffer.get(messageSetBuffer,0,messageSetSize)
        (TopicAndPartition(topic, partition), new ByteBufferMessageSet(ByteBuffer.wrap(messageSetBuffer)))
      })
    })

    ProducerRequest(versionId, correlationId, clientId, requiredAcks, ackTimeoutMs, collection.mutable.Map(partitionDataPairs:_*))
  }
}

case class ProducerRequest(versionId: Short = ProducerRequest.CurrentVersion,
                           correlationId: Int,
                           clientId: String,
                           requiredAcks: Short,
                           ackTimeoutMs: Int,
                           data: collection.mutable.Map[TopicAndPartition, ByteBufferMessageSet])
    extends RequestOrResponse(Some(RequestKeys.ProduceKey)) {

  /**
   * Partitions the data into a map of maps (one for each topic).
   */
  private lazy val dataGroupedByTopic = data.groupBy(_._1.topic)
  val topicPartitionMessageSizeMap = data.map(r => r._1 -> r._2.sizeInBytes).toMap

  def this(correlationId: Int,
           clientId: String,
           requiredAcks: Short,
           ackTimeoutMs: Int,
           data: collection.mutable.Map[TopicAndPartition, ByteBufferMessageSet]) =
    this(ProducerRequest.CurrentVersion, correlationId, clientId, requiredAcks, ackTimeoutMs, data)

  def writeTo(buffer: ByteBuffer) {
    buffer.putShort(versionId)
    buffer.putInt(correlationId)
    writeShortString(buffer, clientId)
    buffer.putShort(requiredAcks)
    buffer.putInt(ackTimeoutMs)

    //save the topic structure
    buffer.putInt(dataGroupedByTopic.size) //the number of topics
    dataGroupedByTopic.foreach {
      case (topic, topicAndPartitionData) =>
        writeShortString(buffer, topic) //write the topic
        buffer.putInt(topicAndPartitionData.size) //the number of partitions
        topicAndPartitionData.foreach(partitionAndData => {
          val partition = partitionAndData._1.partition
          val partitionMessageData = partitionAndData._2
          val bytes = partitionMessageData.buffer
          buffer.putInt(partition)
          buffer.putInt(bytes.limit)
          buffer.put(bytes)
          bytes.rewind
        })
    }
  }

  def sizeInBytes: Int = {
    2 + /* versionId */
    4 + /* correlationId */
    shortStringLength(clientId) + /* client id */
    2 + /* requiredAcks */
    4 + /* ackTimeoutMs */
    4 + /* number of topics */
    dataGroupedByTopic.foldLeft(0)((foldedTopics, currTopic) => {
      foldedTopics +
      shortStringLength(currTopic._1) +
      4 + /* the number of partitions */
      {
        currTopic._2.foldLeft(0)((foldedPartitions, currPartition) => {
          foldedPartitions +
          4 + /* partition id */
          4 + /* byte-length of serialized messages */
          currPartition._2.sizeInBytes
        })
      }
    })
  }

  def numPartitions = data.size

  override def toString(): String = {
    describe(true)
  }

  override  def handleError(e: Throwable, requestChannel: RequestChannel, request: RequestChannel.Request): Unit = {
    if(request.requestObj.asInstanceOf[ProducerRequest].requiredAcks == 0) {
        requestChannel.closeConnection(request.processor, request)
    }
    else {
      val producerResponseStatus = data.map {
        case (topicAndPartition, data) =>
          (topicAndPartition, ProducerResponseStatus(ErrorMapping.codeFor(e.getClass.asInstanceOf[Class[Throwable]]), -1l))
      }
      val errorResponse = ProducerResponse(correlationId, producerResponseStatus)
      requestChannel.sendResponse(new Response(request, new RequestOrResponseSend(request.connectionId, errorResponse)))
    }
  }

  override def describe(details: Boolean): String = {
    val producerRequest = new StringBuilder
    producerRequest.append("Name: " + this.getClass.getSimpleName)
    producerRequest.append("; Version: " + versionId)
    producerRequest.append("; CorrelationId: " + correlationId)
    producerRequest.append("; ClientId: " + clientId)
    producerRequest.append("; RequiredAcks: " + requiredAcks)
    producerRequest.append("; AckTimeoutMs: " + ackTimeoutMs + " ms")
    if(details)
      producerRequest.append("; TopicAndPartition: " + topicPartitionMessageSizeMap.mkString(","))
    producerRequest.toString()
  }

  def emptyData(){
    data.clear()
  }
}

