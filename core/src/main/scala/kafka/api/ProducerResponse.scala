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
import kafka.message.Message
import org.apache.kafka.common.protocol.Errors

import scala.collection.Map
import kafka.common.TopicAndPartition
import kafka.api.ApiUtils._

object ProducerResponse {
  // readFrom assumes that the response is written using V2 format
  def readFrom(buffer: ByteBuffer): ProducerResponse = {
    val correlationId = buffer.getInt
    val topicCount = buffer.getInt
    val statusPairs = (1 to topicCount).flatMap(_ => {
      val topic = readShortString(buffer)
      val partitionCount = buffer.getInt
      (1 to partitionCount).map(_ => {
        val partition = buffer.getInt
        val error = buffer.getShort
        val offset = buffer.getLong
        val timestamp = buffer.getLong
        (TopicAndPartition(topic, partition), ProducerResponseStatus(error, offset, timestamp))
      })
    })

    val throttleTime = buffer.getInt
    ProducerResponse(correlationId, Map(statusPairs:_*), ProducerRequest.CurrentVersion, throttleTime)
  }
}

case class ProducerResponseStatus(var error: Short, offset: Long, timestamp: Long = Message.NoTimestamp)

case class ProducerResponse(correlationId: Int,
                            status: Map[TopicAndPartition, ProducerResponseStatus],
                            requestVersion: Int = 0,
                            throttleTime: Int = 0)
    extends RequestOrResponse() {

  /**
   * Partitions the status map into a map of maps (one for each topic).
   */
  private lazy val statusGroupedByTopic = status.groupBy(_._1.topic)

  def hasError = status.values.exists(_.error != Errors.NONE.code)

  val sizeInBytes = {
    val throttleTimeSize = if (requestVersion > 0) 4 else 0
    val groupedStatus = statusGroupedByTopic
    4 + /* correlation id */
    4 + /* topic count */
    groupedStatus.foldLeft (0) ((foldedTopics, currTopic) => {
      foldedTopics +
      shortStringLength(currTopic._1) +
      4 + /* partition count for this topic */
      currTopic._2.size * {
        4 + /* partition id */
        2 + /* error code */
        8 + /* offset */
        8 /* timestamp */
      }
    }) +
    throttleTimeSize
  }

  def writeTo(buffer: ByteBuffer) {
    val groupedStatus = statusGroupedByTopic
    buffer.putInt(correlationId)
    buffer.putInt(groupedStatus.size) // topic count

    groupedStatus.foreach(topicStatus => {
      val (topic, errorsAndOffsets) = topicStatus
      writeShortString(buffer, topic)
      buffer.putInt(errorsAndOffsets.size) // partition count
      errorsAndOffsets.foreach {
        case((TopicAndPartition(_, partition), ProducerResponseStatus(error, nextOffset, timestamp))) =>
          buffer.putInt(partition)
          buffer.putShort(error)
          buffer.putLong(nextOffset)
          buffer.putLong(timestamp)
      }
    })
    // Throttle time is only supported on V1 style requests
    if (requestVersion > 0)
      buffer.putInt(throttleTime)
  }

  override def describe(details: Boolean):String = { toString }
}

