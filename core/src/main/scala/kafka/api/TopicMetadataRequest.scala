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
import collection.mutable.ListBuffer
import kafka.utils.Logging

object TopicMetadataRequest extends Logging {
  val CurrentVersion = 1.shortValue()
  val DefaultClientId = ""

  /**
   * TopicMetadataRequest has the following format -
   * number of topics (4 bytes) list of topics (2 bytes + topic.length per topic) detailedMetadata (2 bytes) timestamp (8 bytes) count (4 bytes)
   */

  def readFrom(buffer: ByteBuffer): TopicMetadataRequest = {
    val versionId = buffer.getShort
    val clientId = readShortString(buffer)
    val numTopics = readIntInRange(buffer, "number of topics", (0, Int.MaxValue))
    val topics = new ListBuffer[String]()
    for(i <- 0 until numTopics)
      topics += readShortString(buffer)
    val topicsList = topics.toList
    debug("topic = %s".format(topicsList.head))
    new TopicMetadataRequest(versionId, clientId, topics.toList)
  }
}

case class TopicMetadataRequest(val versionId: Short,
                                val clientId: String,
                                val topics: Seq[String])
 extends RequestOrResponse(Some(RequestKeys.MetadataKey)){

def this(topics: Seq[String]) =
  this(TopicMetadataRequest.CurrentVersion, TopicMetadataRequest.DefaultClientId, topics)

  def writeTo(buffer: ByteBuffer) {
    buffer.putShort(versionId)
    writeShortString(buffer, clientId)
    buffer.putInt(topics.size)
    topics.foreach(topic => writeShortString(buffer, topic))
  }

  def sizeInBytes(): Int = {
    2 + (2 + clientId.length) + 4 /* number of topics */ + topics.foldLeft(0)(_ + shortStringLength(_)) /* topics */
  }
}
