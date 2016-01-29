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
import java.nio.channels.GatheringByteChannel

import kafka.common.TopicAndPartition
import kafka.message.{MessageSet, ByteBufferMessageSet}
import kafka.api.ApiUtils._
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.network.{Send, MultiSend}
import org.apache.kafka.common.protocol.Errors

import scala.collection._

object FetchResponsePartitionData {
  def readFrom(buffer: ByteBuffer): FetchResponsePartitionData = {
    val error = buffer.getShort
    val hw = buffer.getLong
    val messageSetSize = buffer.getInt
    val messageSetBuffer = buffer.slice()
    messageSetBuffer.limit(messageSetSize)
    buffer.position(buffer.position + messageSetSize)
    new FetchResponsePartitionData(error, hw, new ByteBufferMessageSet(messageSetBuffer))
  }

  val headerSize =
    2 + /* error code */
    8 + /* high watermark */
    4 /* messageSetSize */
}

case class FetchResponsePartitionData(error: Short = Errors.NONE.code, hw: Long = -1L, messages: MessageSet) {
  val sizeInBytes = FetchResponsePartitionData.headerSize + messages.sizeInBytes
}

// SENDS

class PartitionDataSend(val partitionId: Int,
                        val partitionData: FetchResponsePartitionData) extends Send {
  private val emptyBuffer = ByteBuffer.allocate(0)
  private val messageSize = partitionData.messages.sizeInBytes
  private var messagesSentSize = 0
  private var pending = false
  private val buffer = ByteBuffer.allocate( 4 /** partitionId **/ + FetchResponsePartitionData.headerSize)
  buffer.putInt(partitionId)
  buffer.putShort(partitionData.error)
  buffer.putLong(partitionData.hw)
  buffer.putInt(partitionData.messages.sizeInBytes)
  buffer.rewind()

  override def completed = !buffer.hasRemaining && messagesSentSize >= messageSize && !pending

  override def destination: String = ""

  override def writeTo(channel: GatheringByteChannel): Long = {
    var written = 0L
    if (buffer.hasRemaining)
      written += channel.write(buffer)
    if (!buffer.hasRemaining) {
      if (messagesSentSize < messageSize) {
        val bytesSent = partitionData.messages.writeTo(channel, messagesSentSize, messageSize - messagesSentSize)
        messagesSentSize += bytesSent
        written += bytesSent
      }
      if (messagesSentSize >= messageSize && hasPendingWrites(channel))
        channel.write(emptyBuffer)
    }

    pending = hasPendingWrites(channel)

    written
  }

  override def size = buffer.capacity() + messageSize
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
    TopicData(topic, Map(topicPartitionDataPairs:_*))
  }

  def headerSize(topic: String) =
    shortStringLength(topic) +
    4 /* partition count */
}

case class TopicData(topic: String, partitionData: Map[Int, FetchResponsePartitionData]) {
  val sizeInBytes =
    TopicData.headerSize(topic) + partitionData.values.foldLeft(0)(_ + _.sizeInBytes + 4)

  val headerSize = TopicData.headerSize(topic)
}

class TopicDataSend(val dest: String, val topicData: TopicData) extends Send {

  private val emptyBuffer = ByteBuffer.allocate(0)

  private var sent = 0L

  private var pending = false

  override def completed: Boolean = sent >= size && !pending

  override def destination: String = dest

  override def size = topicData.headerSize + sends.size()

  private val buffer = ByteBuffer.allocate(topicData.headerSize)
  writeShortString(buffer, topicData.topic)
  buffer.putInt(topicData.partitionData.size)
  buffer.rewind()

  private val sends = new MultiSend(dest,
                            JavaConversions.seqAsJavaList(topicData.partitionData.toList.map(d => new PartitionDataSend(d._1, d._2))))

  override def writeTo(channel: GatheringByteChannel): Long = {
    if (completed)
      throw new KafkaException("This operation cannot be completed on a complete request.")

    var written = 0L
    if (buffer.hasRemaining)
      written += channel.write(buffer)
    if (!buffer.hasRemaining) {
      if (!sends.completed)
        written += sends.writeTo(channel)
      if (sends.completed && hasPendingWrites(channel))
        written += channel.write(emptyBuffer)
    }

    pending = hasPendingWrites(channel)

    sent += written
    written
  }
}


object FetchResponse {

  // The request version is used to determine which fields we can expect in the response
  def readFrom(buffer: ByteBuffer, requestVersion: Int): FetchResponse = {
    val correlationId = buffer.getInt
    val throttleTime = if (requestVersion > 0) buffer.getInt else 0
    val topicCount = buffer.getInt
    val pairs = (1 to topicCount).flatMap(_ => {
      val topicData = TopicData.readFrom(buffer)
      topicData.partitionData.map {
        case (partitionId, partitionData) =>
          (TopicAndPartition(topicData.topic, partitionId), partitionData)
      }
    })
    FetchResponse(correlationId, Map(pairs:_*), requestVersion, throttleTime)
  }

  // Returns the size of the response header
  def headerSize(requestVersion: Int): Int = {
    val throttleTimeSize = if (requestVersion > 0) 4 else 0
    4 + /* correlationId */
    4 + /* topic count */
    throttleTimeSize
  }

  // Returns the size of entire fetch response in bytes (including the header size)
  def responseSize(dataGroupedByTopic: Map[String, Map[TopicAndPartition, FetchResponsePartitionData]],
                   requestVersion: Int): Int = {
    headerSize(requestVersion) +
    dataGroupedByTopic.foldLeft(0) { case (folded, (topic, partitionDataMap)) =>
      val topicData = TopicData(topic, partitionDataMap.map {
        case (topicAndPartition, partitionData) => (topicAndPartition.partition, partitionData)
      })
      folded + topicData.sizeInBytes
    }
  }
}

case class FetchResponse(correlationId: Int,
                         data: Map[TopicAndPartition, FetchResponsePartitionData],
                         requestVersion: Int = 0,
                         throttleTimeMs: Int = 0)
  extends RequestOrResponse() {

  /**
   * Partitions the data into a map of maps (one for each topic).
   */
  lazy val dataGroupedByTopic = data.groupBy{ case (topicAndPartition, fetchData) => topicAndPartition.topic }
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
    data.get(topicAndPartition) match {
      case Some(partitionData) => partitionData
      case _ =>
        throw new IllegalArgumentException(
          "No partition %s in fetch response %s".format(topicAndPartition, this.toString))
    }
  }

  def messageSet(topic: String, partition: Int): ByteBufferMessageSet =
    partitionDataFor(topic, partition).messages.asInstanceOf[ByteBufferMessageSet]

  def highWatermark(topic: String, partition: Int) = partitionDataFor(topic, partition).hw

  def hasError = data.values.exists(_.error != Errors.NONE.code)

  def errorCode(topic: String, partition: Int) = partitionDataFor(topic, partition).error
}


class FetchResponseSend(val dest: String, val fetchResponse: FetchResponse) extends Send {

  private val emptyBuffer = ByteBuffer.allocate(0)

  private val payloadSize = fetchResponse.sizeInBytes

  private var sent = 0L

  private var pending = false

  override def size = 4 /* for size byte */ + payloadSize

  override def completed = sent >= size && !pending

  override def destination = dest

  // The throttleTimeSize will be 0 if the request was made from a client sending a V0 style request
  private val buffer = ByteBuffer.allocate(4 /* for size */ + fetchResponse.headerSizeInBytes)
  fetchResponse.writeHeaderTo(buffer)
  buffer.rewind()

  private val sends = new MultiSend(dest, JavaConversions.seqAsJavaList(fetchResponse.dataGroupedByTopic.toList.map {
    case(topic, data) => new TopicDataSend(dest, TopicData(topic,
                                                     data.map{case(topicAndPartition, message) => (topicAndPartition.partition, message)}))
    }))

  override def writeTo(channel: GatheringByteChannel): Long = {
    if (completed)
      throw new KafkaException("This operation cannot be completed on a complete request.")

    var written = 0L

    if (buffer.hasRemaining)
      written += channel.write(buffer)
    if (!buffer.hasRemaining) {
      if (!sends.completed)
        written += sends.writeTo(channel)
      if (sends.completed && hasPendingWrites(channel))
        written += channel.write(emptyBuffer)
    }

    sent += written
    pending = hasPendingWrites(channel)

    written
  }
}
