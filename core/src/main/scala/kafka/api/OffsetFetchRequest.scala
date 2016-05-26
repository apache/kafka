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
import kafka.common.{TopicAndPartition, _}
import kafka.network.{RequestOrResponseSend, RequestChannel}
import kafka.network.RequestChannel.Response
import kafka.utils.Logging
import org.apache.kafka.common.protocol.{ApiKeys, Errors}

object OffsetFetchRequest extends Logging {
  val CurrentVersion: Short = 1
  val DefaultClientId = ""

  def readFrom(buffer: ByteBuffer): OffsetFetchRequest = {
    // Read values from the envelope
    val versionId = buffer.getShort
    val correlationId = buffer.getInt
    val clientId = readShortString(buffer)

    // Read the OffsetFetchRequest
    val consumerGroupId = readShortString(buffer)
    val topicCount = buffer.getInt
    val pairs = (1 to topicCount).flatMap(_ => {
      val topic = readShortString(buffer)
      val partitionCount = buffer.getInt
      (1 to partitionCount).map(_ => {
        val partitionId = buffer.getInt
        TopicAndPartition(topic, partitionId)
      })
    })
    OffsetFetchRequest(consumerGroupId, pairs, versionId, correlationId, clientId)
  }
}

case class OffsetFetchRequest(groupId: String,
                              requestInfo: Seq[TopicAndPartition],
                              versionId: Short = OffsetFetchRequest.CurrentVersion,
                              correlationId: Int = 0,
                              clientId: String = OffsetFetchRequest.DefaultClientId)
    extends RequestOrResponse(Some(ApiKeys.OFFSET_FETCH.id)) {

  lazy val requestInfoGroupedByTopic = requestInfo.groupBy(_.topic)
  
  def writeTo(buffer: ByteBuffer) {
    // Write envelope
    buffer.putShort(versionId)
    buffer.putInt(correlationId)
    writeShortString(buffer, clientId)

    // Write OffsetFetchRequest
    writeShortString(buffer, groupId)             // consumer group
    buffer.putInt(requestInfoGroupedByTopic.size) // number of topics
    requestInfoGroupedByTopic.foreach( t1 => { // (topic, Seq[TopicAndPartition])
      writeShortString(buffer, t1._1) // topic
      buffer.putInt(t1._2.size)       // number of partitions for this topic
      t1._2.foreach( t2 => {
        buffer.putInt(t2.partition)
      })
    })
  }

  override def sizeInBytes =
    2 + /* versionId */
    4 + /* correlationId */
    shortStringLength(clientId) +
    shortStringLength(groupId) + 
    4 + /* topic count */
    requestInfoGroupedByTopic.foldLeft(0)((count, t) => {
      count + shortStringLength(t._1) + /* topic */
      4 + /* number of partitions */
      t._2.size * 4 /* partition */
    })

  override  def handleError(e: Throwable, requestChannel: RequestChannel, request: RequestChannel.Request): Unit = {
    val responseMap = requestInfo.map {
      case (topicAndPartition) => (topicAndPartition, OffsetMetadataAndError(
        Errors.forException(e).code
      ))
    }.toMap
    val errorResponse = OffsetFetchResponse(requestInfo=responseMap, correlationId=correlationId)
    requestChannel.sendResponse(new Response(request, new RequestOrResponseSend(request.connectionId, errorResponse)))
  }

  override def describe(details: Boolean): String = {
    val offsetFetchRequest = new StringBuilder
    offsetFetchRequest.append("Name: " + this.getClass.getSimpleName)
    offsetFetchRequest.append("; Version: " + versionId)
    offsetFetchRequest.append("; CorrelationId: " + correlationId)
    offsetFetchRequest.append("; ClientId: " + clientId)
    offsetFetchRequest.append("; GroupId: " + groupId)
    if(details)
      offsetFetchRequest.append("; RequestInfo: " + requestInfo.mkString(","))
    offsetFetchRequest.toString()
  }

  override def toString: String = {
    describe(details = true)
  }
}
