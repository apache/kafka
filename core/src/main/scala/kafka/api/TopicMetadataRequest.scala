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
import collection.mutable.ListBuffer
import kafka.utils._
import kafka.common.KafkaException

sealed trait DetailedMetadataRequest { def requestId: Short }
case object SegmentMetadata extends DetailedMetadataRequest { val requestId = 1.asInstanceOf[Short] }
case object NoSegmentMetadata extends DetailedMetadataRequest { val requestId = 0.asInstanceOf[Short] }

object TopicMetadataRequest {
  val CurrentVersion = 1.shortValue()
  val DefaultClientId = ""

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
      case _ => throw new KafkaException("Unknown detailed metadata request id " + requestId)
    }
  }

  def readFrom(buffer: ByteBuffer): TopicMetadataRequest = {
    val versionId = buffer.getShort
    val clientId = Utils.readShortString(buffer)
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
      case _ => throw new KafkaException("Invalid value for the detailed metadata request "
                                                    + returnDetailedMetadata.requestId)
    }
    debug("topic = %s, detailed metadata request = %d"
          .format(topicsList.head, returnDetailedMetadata.requestId))
    new TopicMetadataRequest(versionId, clientId, topics.toList, returnDetailedMetadata, timestamp, count)
  }
}

case class TopicMetadataRequest(val versionId: Short,
                                val clientId: String,
                                val topics: Seq[String],
                                val detailedMetadata: DetailedMetadataRequest = NoSegmentMetadata,
                                val timestamp: Option[Long] = None, val count: Option[Int] = None)
 extends RequestOrResponse(Some(RequestKeys.MetadataKey)){

def this(topics: Seq[String]) =
  this(TopicMetadataRequest.CurrentVersion, TopicMetadataRequest.DefaultClientId, topics, NoSegmentMetadata, None, None)




  def writeTo(buffer: ByteBuffer) {
    buffer.putShort(versionId)
    Utils.writeShortString(buffer, clientId)
    buffer.putInt(topics.size)
    topics.foreach(topic => writeShortString(buffer, topic))
    buffer.putShort(detailedMetadata.requestId)
    detailedMetadata match {
      case SegmentMetadata =>
        buffer.putLong(timestamp.get)
        buffer.putInt(count.get)
      case NoSegmentMetadata =>
      case _ => throw new KafkaException("Invalid value for the detailed metadata request " + detailedMetadata.requestId)
    }
  }

  def sizeInBytes(): Int = {
    var size: Int = 2 + (2 + clientId.length) + 4 /* number of topics */ + topics.foldLeft(0)(_ + shortStringLength(_)) /* topics */ +
                    2 /* detailed metadata */
    detailedMetadata match {
      case SegmentMetadata =>
        size += 8 /* timestamp */ + 4 /* count */
      case NoSegmentMetadata =>
      case _ => throw new KafkaException("Invalid value for the detailed metadata request " + detailedMetadata.requestId)
    }
    size
  }
}
