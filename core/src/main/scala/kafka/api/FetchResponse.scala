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

import kafka.common.{TopicAndPartition, ErrorMapping}
import kafka.message.{MessageSet, ByteBufferMessageSet}
import kafka.network.{MultiSend, Send}
import kafka.api.ApiUtils._

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

case class FetchResponsePartitionData(error: Short = ErrorMapping.NoError, hw: Long = -1L, messages: MessageSet) {
  val sizeInBytes = FetchResponsePartitionData.headerSize + messages.sizeInBytes
}

// SENDS

class PartitionDataSend(val partitionId: Int,
                        val partitionData: FetchResponsePartitionData) extends Send {
  private val messageSize = partitionData.messages.sizeInBytes
  private var messagesSentSize = 0

  private val buffer = ByteBuffer.allocate( 4 /** partitionId **/ + FetchResponsePartitionData.headerSize)
  buffer.putInt(partitionId)
  buffer.putShort(partitionData.error)
  buffer.putLong(partitionData.hw)
  buffer.putInt(partitionData.messages.sizeInBytes)
  buffer.rewind()

  override def complete = !buffer.hasRemaining && messagesSentSize >= messageSize

  override def writeTo(channel: GatheringByteChannel): Int = {
    var written = 0
    if(buffer.hasRemaining)
      written += channel.write(buffer)
    if(!buffer.hasRemaining && messagesSentSize < messageSize) {
      val bytesSent = partitionData.messages.writeTo(channel, messagesSentSize, messageSize - messagesSentSize)
      messagesSentSize += bytesSent
      written += bytesSent
    }
    written
  }
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

class TopicDataSend(val topicData: TopicData) extends Send {
  private val size = topicData.sizeInBytes

  private var sent = 0

  override def complete = sent >= size

  private val buffer = ByteBuffer.allocate(topicData.headerSize)
  writeShortString(buffer, topicData.topic)
  buffer.putInt(topicData.partitionData.size)
  buffer.rewind()

  val sends = new MultiSend(topicData.partitionData.toList
                                    .map(d => new PartitionDataSend(d._1, d._2))) {
    val expectedBytesToWrite = topicData.sizeInBytes - topicData.headerSize
  }

  def writeTo(channel: GatheringByteChannel): Int = {
    expectIncomplete()
    var written = 0
    if(buffer.hasRemaining)
      written += channel.write(buffer)
    if(!buffer.hasRemaining && !sends.complete) {
      written += sends.writeTo(channel)
    }
    sent += written
    written
  }
}


object FetchResponse {

  val headerSize =
    4 + /* correlationId */
    4 /* topic count */

  def readFrom(buffer: ByteBuffer): FetchResponse = {
    val correlationId = buffer.getInt
    val topicCount = buffer.getInt
    val pairs = (1 to topicCount).flatMap(_ => {
      val topicData = TopicData.readFrom(buffer)
      topicData.partitionData.map {
        case (partitionId, partitionData) =>
          (TopicAndPartition(topicData.topic, partitionId), partitionData)
      }
    })
    FetchResponse(correlationId, Map(pairs:_*))
  }
}


case class FetchResponse(correlationId: Int,
                         data: Map[TopicAndPartition, FetchResponsePartitionData])
    extends RequestOrResponse() {

  /**
   * Partitions the data into a map of maps (one for each topic).
   */
  lazy val dataGroupedByTopic = data.groupBy(_._1.topic)

  val sizeInBytes =
    FetchResponse.headerSize +
    dataGroupedByTopic.foldLeft(0) ((folded, curr) => {
      val topicData = TopicData(curr._1, curr._2.map {
        case (topicAndPartition, partitionData) => (topicAndPartition.partition, partitionData)
      })
      folded + topicData.sizeInBytes
    })

  /*
   * FetchResponse uses [sendfile](http://man7.org/linux/man-pages/man2/sendfile.2.html)
   * api for data transfer, so `writeTo` aren't actually being used.
   * It is implemented as an empty function to comform to `RequestOrResponse.writeTo`
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

  def hasError = data.values.exists(_.error != ErrorMapping.NoError)

  def errorCode(topic: String, partition: Int) = partitionDataFor(topic, partition).error
}


class FetchResponseSend(val fetchResponse: FetchResponse) extends Send {
  private val size = fetchResponse.sizeInBytes

  private var sent = 0

  private val sendSize = 4 /* for size */ + size

  override def complete = sent >= sendSize

  private val buffer = ByteBuffer.allocate(4 /* for size */ + FetchResponse.headerSize)
  buffer.putInt(size)
  buffer.putInt(fetchResponse.correlationId)
  buffer.putInt(fetchResponse.dataGroupedByTopic.size) // topic count
  buffer.rewind()

  val sends = new MultiSend(fetchResponse.dataGroupedByTopic.toList.map {
    case(topic, data) => new TopicDataSend(TopicData(topic,
                                                     data.map{case(topicAndPartition, message) => (topicAndPartition.partition, message)}))
  }) {
    val expectedBytesToWrite = fetchResponse.sizeInBytes - FetchResponse.headerSize
  }

  def writeTo(channel: GatheringByteChannel):Int = {
    expectIncomplete()
    var written = 0
    if(buffer.hasRemaining)
      written += channel.write(buffer)
    if(!buffer.hasRemaining && !sends.complete) {
      written += sends.writeTo(channel)
    }
    sent += written
    written
  }
}

