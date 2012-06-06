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
import kafka.common.ErrorMapping


object TopicMetaDataResponse {

  def readFrom(buffer: ByteBuffer): TopicMetaDataResponse = {
    val errorCode = buffer.getShort
    val versionId = buffer.getShort

    val topicCount = buffer.getInt
    val topicsMetadata = new Array[TopicMetadata](topicCount)
    for( i <- 0 until topicCount) {
      topicsMetadata(i) = TopicMetadata.readFrom(buffer)
    }
    new TopicMetaDataResponse(versionId, topicsMetadata.toSeq, errorCode)
  }
}

case class TopicMetaDataResponse(versionId: Short,
                                 topicsMetadata: Seq[TopicMetadata],
                                 errorCode: Short = ErrorMapping.NoError) extends RequestOrResponse
{
  val sizeInBytes = 2 + topicsMetadata.foldLeft(4)(_ + _.sizeInBytes) + 2

  def writeTo(buffer: ByteBuffer) {
    buffer.putShort(versionId)
    /* error code */
    buffer.putShort(errorCode)
    /* topic metadata */
    buffer.putInt(topicsMetadata.length)
    topicsMetadata.foreach(_.writeTo(buffer))
  }
}