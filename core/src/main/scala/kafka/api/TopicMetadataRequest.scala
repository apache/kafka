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
import kafka.utils.Utils._
import kafka.network.{Send, Request}
import java.nio.channels.GatheringByteChannel
import kafka.common.ErrorMapping
import collection.mutable.ListBuffer

sealed trait DetailedMetadataRequest { def requestId: Short }
case object SegmentMetadata extends DetailedMetadataRequest { val requestId = 1.asInstanceOf[Short] }
case object NoSegmentMetadata extends DetailedMetadataRequest { val requestId = 0.asInstanceOf[Short] }

object TopicMetadataRequest {

  /**
   * TopicMetadataRequest has the following format -
   *
   * number of topics (4 bytes) list of topics (2 bytes + topic.length per topic) detailedMetadata (2 bytes) timestamp (8 bytes) count (4 bytes)
   *
   * The detailedMetadata field is a placeholder for requesting various details about partition and log metadata
   * By default, the value for this field is 0, which means it will just return leader, replica and ISR metadata for
   * all partitions of the list of topics mentioned in the request.
   */
  def getDetailedMetadataRequest(requestId: Short): DetailedMetadataRequest = {
    requestId match {
      case SegmentMetadata.requestId => SegmentMetadata
      case NoSegmentMetadata.requestId => NoSegmentMetadata
      case _ => throw new IllegalArgumentException("Unknown detailed metadata request id " + requestId)
    }
  }

  def readFrom(buffer: ByteBuffer): TopicMetadataRequest = {
    val numTopics = getIntInRange(buffer, "number of topics", (0, Int.MaxValue))
    val topics = new ListBuffer[String]()
    for(i <- 0 until numTopics)
      topics += readShortString(buffer, "UTF-8")
    val topicsList = topics.toList
    val returnDetailedMetadata = getDetailedMetadataRequest(buffer.getShort)
    var timestamp: Option[Long] = None
    var count: Option[Int] = None
    returnDetailedMetadata match {
      case NoSegmentMetadata =>
      case SegmentMetadata =>
        timestamp = Some(buffer.getLong)
        count = Some(buffer.getInt)
      case _ => throw new IllegalArgumentException("Invalid value for the detailed metadata request "
                                                    + returnDetailedMetadata.requestId)
    }
    debug("topic = %s, detailed metadata request = %d"
          .format(topicsList.head, returnDetailedMetadata.requestId))
    new TopicMetadataRequest(topics.toList, returnDetailedMetadata, timestamp, count)
  }

  def serializeTopicMetadata(topicMetadata: Seq[TopicMetadata]): ByteBuffer = {
    val size = topicMetadata.foldLeft(4 /* num topics */)(_ + _.sizeInBytes)
    val buffer = ByteBuffer.allocate(size)
    /* number of topics */
    buffer.putInt(topicMetadata.size)
    /* topic partition_metadata */
    topicMetadata.foreach(m => m.writeTo(buffer))
    buffer.rewind()
    buffer
  }

  def deserializeTopicsMetadataResponse(buffer: ByteBuffer): Seq[TopicMetadata] = {
    /* number of topics */
    val numTopics = getIntInRange(buffer, "number of topics", (0, Int.MaxValue))
    val topicMetadata = new Array[TopicMetadata](numTopics)
    for(i <- 0 until  numTopics)
      topicMetadata(i) = TopicMetadata.readFrom(buffer)
    topicMetadata
  }
}

case class TopicMetadataRequest(val topics: Seq[String],
                                val detailedMetadata: DetailedMetadataRequest = NoSegmentMetadata,
                                val timestamp: Option[Long] = None, val count: Option[Int] = None)
  extends Request(RequestKeys.TopicMetadata){

  def writeTo(buffer: ByteBuffer) {
    buffer.putInt(topics.size)
    topics.foreach(topic => writeShortString(buffer, topic))
    buffer.putShort(detailedMetadata.requestId)
    detailedMetadata match {
      case SegmentMetadata =>
        buffer.putLong(timestamp.get)
        buffer.putInt(count.get)
      case NoSegmentMetadata =>
      case _ => throw new IllegalArgumentException("Invalid value for the detailed metadata request " + detailedMetadata.requestId)
    }
  }

  def sizeInBytes(): Int = {
    var size: Int = 4 /* number of topics */ + topics.foldLeft(0)(_ + shortStringLength(_)) /* topics */ +
                    2 /* detailed metadata */
    detailedMetadata match {
      case SegmentMetadata =>
        size += 8 /* timestamp */ + 4 /* count */
      case NoSegmentMetadata =>
      case _ => throw new IllegalArgumentException("Invalid value for the detailed metadata request " + detailedMetadata.requestId)
    }
    size
  }
}

class TopicMetadataSend(topicsMetadata: Seq[TopicMetadata]) extends Send {
  private var size: Int = topicsMetadata.foldLeft(0)(_ + _.sizeInBytes)
  private val header = ByteBuffer.allocate(6)
  val metadata = TopicMetadataRequest.serializeTopicMetadata(topicsMetadata)
  header.putInt(size + 2)
  header.putShort(ErrorMapping.NoError.asInstanceOf[Short])
  header.rewind()

  var complete: Boolean = false

  def writeTo(channel: GatheringByteChannel): Int = {
    expectIncomplete()
    var written = 0
    if(header.hasRemaining)
      written += channel.write(header)
    if(!header.hasRemaining && metadata.hasRemaining)
      written += channel.write(metadata)

    if(!metadata.hasRemaining)
      complete = true
    written
  }
}