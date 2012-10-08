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
import kafka.utils.Utils

object FetchResponsePartitionData {
  def readFrom(buffer: ByteBuffer): FetchResponsePartitionData = {
    val partition = buffer.getInt
    val error = buffer.getShort
    val initialOffset = buffer.getLong
    val hw = buffer.getLong
    val messageSetSize = buffer.getInt
    val messageSetBuffer = buffer.slice()
    messageSetBuffer.limit(messageSetSize)
    buffer.position(buffer.position + messageSetSize)
    new FetchResponsePartitionData(partition, error, initialOffset, hw, new ByteBufferMessageSet(messageSetBuffer))
  }

  val headerSize =
    4 + /* partition */
    2 + /* error code */
    8 + /* initialOffset */
    8 + /* high watermark */
    4 /* messageSetSize */
}

case class FetchResponsePartitionData(partition: Int, error: Short = ErrorMapping.NoError, initialOffset:Long = 0L, hw: Long = -1L, messages: MessageSet) {

  val sizeInBytes = FetchResponsePartitionData.headerSize + messages.sizeInBytes.intValue()

  def this(partition: Int, messages: MessageSet) = this(partition, ErrorMapping.NoError, 0L, -1L, messages)
  
}

// SENDS

class PartitionDataSend(val partitionData: FetchResponsePartitionData) extends Send {
  private val messageSize = partitionData.messages.sizeInBytes
  private var messagesSentSize = 0L

  private val buffer = ByteBuffer.allocate(FetchResponsePartitionData.headerSize)
  buffer.putInt(partitionData.partition)
  buffer.putShort(partitionData.error)
  buffer.putLong(partitionData.initialOffset)
  buffer.putLong(partitionData.hw)
  buffer.putInt(partitionData.messages.sizeInBytes.intValue())
  buffer.rewind()

  override def complete = !buffer.hasRemaining && messagesSentSize >= messageSize

  override def writeTo(channel: GatheringByteChannel): Int = {
    var written = 0
    if(buffer.hasRemaining)
      written += channel.write(buffer)
    if(!buffer.hasRemaining && messagesSentSize < messageSize) {
      val bytesSent = partitionData.messages.writeTo(channel, messagesSentSize, messageSize - messagesSentSize).toInt
      messagesSentSize += bytesSent
      written += bytesSent
    }
    written
  }
}

object TopicData {
  def readFrom(buffer: ByteBuffer): TopicData = {
    val topic = Utils.readShortString(buffer, RequestOrResponse.DefaultCharset)
    val partitionCount = buffer.getInt
    val topicPartitionDataPairs = (1 to partitionCount).map(_ => {
      val partitionData = FetchResponsePartitionData.readFrom(buffer)
      (TopicAndPartition(topic, partitionData.partition), partitionData)
    })
    TopicData(topic, Map(topicPartitionDataPairs:_*))
  }

  def headerSize(topic: String) =
    Utils.shortStringLength(topic, RequestOrResponse.DefaultCharset) +
    4 /* partition count */
}

case class TopicData(topic: String, partitionData: Map[TopicAndPartition, FetchResponsePartitionData]) {
  val sizeInBytes =
    TopicData.headerSize(topic) + partitionData.values.foldLeft(0)(_ + _.sizeInBytes)

  val headerSize = TopicData.headerSize(topic)
}

class TopicDataSend(val topicData: TopicData) extends Send {
  private val size = topicData.sizeInBytes

  private var sent = 0

  override def complete = sent >= size

  private val buffer = ByteBuffer.allocate(topicData.headerSize)
  Utils.writeShortString(buffer, topicData.topic, RequestOrResponse.DefaultCharset)
  buffer.putInt(topicData.partitionData.size)
  buffer.rewind()

  val sends = new MultiSend(topicData.partitionData.toList.map(d => new PartitionDataSend(d._2))) {
    val expectedBytesToWrite = topicData.sizeInBytes - topicData.headerSize
  }

  def writeTo(channel: GatheringByteChannel): Int = {
    expectIncomplete()
    var written = 0
    if(buffer.hasRemaining)
      written += channel.write(buffer)
    if(!buffer.hasRemaining && !sends.complete) {
      written += sends.writeCompletely(channel)
    }
    sent += written
    written
  }
}


object FetchResponse {

  val headerSize =
    2 + /* versionId */
    4 + /* correlationId */
    4 /* topic count */

  def readFrom(buffer: ByteBuffer): FetchResponse = {
    val versionId = buffer.getShort
    val correlationId = buffer.getInt
    val topicCount = buffer.getInt
    val pairs = (1 to topicCount).flatMap(_ => {
      val topicData = TopicData.readFrom(buffer)
      topicData.partitionData.values.map(
        partitionData => (TopicAndPartition(topicData.topic, partitionData.partition), partitionData)
      )
    })
    FetchResponse(versionId, correlationId, Map(pairs:_*))
  }
}


case class FetchResponse(versionId: Short,
                         correlationId: Int,
                         data: Map[TopicAndPartition, FetchResponsePartitionData])  {

  /**
   * Partitions the data into a map of maps (one for each topic).
   */
  lazy val dataGroupedByTopic = data.groupBy(_._1.topic)

  val sizeInBytes =
    FetchResponse.headerSize +
    dataGroupedByTopic.foldLeft(0) ((folded, curr) => {
      val topicData = TopicData(curr._1, curr._2)
      folded +
      topicData.sizeInBytes
    })

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
  buffer.putShort(fetchResponse.versionId)
  buffer.putInt(fetchResponse.correlationId)
  buffer.putInt(fetchResponse.dataGroupedByTopic.size) // topic count
  buffer.rewind()

  val sends = new MultiSend(fetchResponse.dataGroupedByTopic.toList.map {
    case(topic, data) => new TopicDataSend(TopicData(topic, data))
  }) {
    val expectedBytesToWrite = fetchResponse.sizeInBytes - FetchResponse.headerSize
  }

  def writeTo(channel: GatheringByteChannel):Int = {
    expectIncomplete()
    var written = 0
    if(buffer.hasRemaining)
      written += channel.write(buffer)
    if(!buffer.hasRemaining && !sends.complete) {
      written += sends.writeCompletely(channel)
    }
    sent += written
    written
  }
}

