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

import kafka.common.TopicAndPartition
import kafka.message.{ByteBufferMessageSet, MessageSet}
import kafka.api.ApiUtils._
import org.apache.kafka.common.protocol.Errors

import scala.collection._

object FetchResponsePartitionData {
  def readFrom(buffer: ByteBuffer): FetchResponsePartitionData = {
    val error = Errors.forCode(buffer.getShort)
    val hw = buffer.getLong
    val messageSetSize = buffer.getInt
    val messageSetBuffer = buffer.slice()
    messageSetBuffer.limit(messageSetSize)
    buffer.position(buffer.position() + messageSetSize)
    new FetchResponsePartitionData(error, hw, new ByteBufferMessageSet(messageSetBuffer))
  }

  val headerSize =
    2 + /* error code */
    8 + /* high watermark */
    4 /* messageSetSize */
}

case class FetchResponsePartitionData(error: Errors = Errors.NONE, hw: Long = -1L, messages: MessageSet) {
  val sizeInBytes = FetchResponsePartitionData.headerSize + messages.sizeInBytes
}

object TopicData {
  def readFrom(buffer: ByteBuffer): TopicData = {
    val topic = readShortString(buffer)
    val partitionCount = buffer.getInt
    val topicPartitionDataPairs = (1 to partitionCount).map(_ => {
      val partitionId = buffer.getInt
      val partitionData = FetchResponsePartitionData.readFrom(buffer)
      (partitionId, partitionData)
    })
    TopicData(topic, Seq(topicPartitionDataPairs:_*))
  }

  def headerSize(topic: String) =
    shortStringLength(topic) +
    4 /* partition count */
}

case class TopicData(topic: String, partitionData: Seq[(Int, FetchResponsePartitionData)]) {
  val sizeInBytes =
    TopicData.headerSize(topic) + partitionData.foldLeft(0)((folded, data) => {
      folded + data._2.sizeInBytes + 4
    }                                  /*_ + _.sizeInBytes + 4*/)

  val headerSize = TopicData.headerSize(topic)
}

@deprecated("This object has been deprecated and will be removed in a future release.", "1.0.0")
object FetchResponse {

  // The request version is used to determine which fields we can expect in the response
  def readFrom(buffer: ByteBuffer, requestVersion: Int): FetchResponse = {
    val correlationId = buffer.getInt
    val throttleTime = if (requestVersion > 0) buffer.getInt else 0
    val topicCount = buffer.getInt
    val pairs = (1 to topicCount).flatMap(_ => {
      val topicData = TopicData.readFrom(buffer)
      topicData.partitionData.map { case (partitionId, partitionData) =>
        (TopicAndPartition(topicData.topic, partitionId), partitionData)
      }
    })
    FetchResponse(correlationId, Vector(pairs:_*), requestVersion, throttleTime)
  }

  type FetchResponseEntry = (Int, FetchResponsePartitionData)

  def batchByTopic(data: Seq[(TopicAndPartition, FetchResponsePartitionData)]): Seq[(String, Seq[FetchResponseEntry])] =
    FetchRequest.batchByTopic(data)

  // Returns the size of the response header
  def headerSize(requestVersion: Int): Int = {
    val throttleTimeSize = if (requestVersion > 0) 4 else 0
    4 + /* correlationId */
    4 + /* topic count */
    throttleTimeSize
  }

  // Returns the size of entire fetch response in bytes (including the header size)
  def responseSize(dataGroupedByTopic: Seq[(String, Seq[FetchResponseEntry])],
                   requestVersion: Int): Int = {
    headerSize(requestVersion) +
    dataGroupedByTopic.foldLeft(0) { case (folded, (topic, partitionDataMap)) =>
      val topicData = TopicData(topic, partitionDataMap.map {
        case (partitionId, partitionData) => (partitionId, partitionData)
      })
      folded + topicData.sizeInBytes
    }
  }
}

@deprecated("This object has been deprecated and will be removed in a future release.", "1.0.0")
case class FetchResponse(correlationId: Int,
                         data: Seq[(TopicAndPartition, FetchResponsePartitionData)],
                         requestVersion: Int = 0,
                         throttleTimeMs: Int = 0)
  extends RequestOrResponse() {

  /**
   * Partitions the data into a map of maps (one for each topic).
   */
  private lazy val dataByTopicAndPartition = data.toMap
  lazy val dataGroupedByTopic = FetchResponse.batchByTopic(data)
  val headerSizeInBytes = FetchResponse.headerSize(requestVersion)
  lazy val sizeInBytes = FetchResponse.responseSize(dataGroupedByTopic, requestVersion)

  /*
   * Writes the header of the FetchResponse to the input buffer
   */
  def writeHeaderTo(buffer: ByteBuffer) = {
    buffer.putInt(sizeInBytes)
    buffer.putInt(correlationId)
    // Include the throttleTime only if the client can read it
    if (requestVersion > 0)
      buffer.putInt(throttleTimeMs)

    buffer.putInt(dataGroupedByTopic.size) // topic count
  }
  /*
   * FetchResponse uses [sendfile](http://man7.org/linux/man-pages/man2/sendfile.2.html)
   * api for data transfer through the FetchResponseSend, so `writeTo` aren't actually being used.
   * It is implemented as an empty function to conform to `RequestOrResponse.writeTo`
   * abstract method signature.
   */
  def writeTo(buffer: ByteBuffer): Unit = throw new UnsupportedOperationException

  override def describe(details: Boolean): String = toString

  private def partitionDataFor(topic: String, partition: Int): FetchResponsePartitionData = {
    val topicAndPartition = TopicAndPartition(topic, partition)
    dataByTopicAndPartition.get(topicAndPartition) match {
      case Some(partitionData) => partitionData
      case _ =>
        throw new IllegalArgumentException(
          "No partition %s in fetch response %s".format(topicAndPartition, this.toString))
    }
  }

  def messageSet(topic: String, partition: Int): ByteBufferMessageSet =
    partitionDataFor(topic, partition).messages.asInstanceOf[ByteBufferMessageSet]

  def highWatermark(topic: String, partition: Int) = partitionDataFor(topic, partition).hw

  def hasError = dataByTopicAndPartition.values.exists(_.error != Errors.NONE)

  def error(topic: String, partition: Int) = partitionDataFor(topic, partition).error
}
