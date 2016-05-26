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
import kafka.api.ApiUtils._
import org.apache.kafka.common.protocol.Errors
import collection.Set

object ControlledShutdownResponse {
  def readFrom(buffer: ByteBuffer): ControlledShutdownResponse = {
    val correlationId = buffer.getInt
    val errorCode = buffer.getShort
    val numEntries = buffer.getInt

    var partitionsRemaining = Set[TopicAndPartition]()
    for (i<- 0 until numEntries){
      val topic = readShortString(buffer)
      val partition = buffer.getInt
      partitionsRemaining += new TopicAndPartition(topic, partition)
    }
    new ControlledShutdownResponse(correlationId, errorCode, partitionsRemaining)
  }
}


case class ControlledShutdownResponse(correlationId: Int,
                                      errorCode: Short = Errors.NONE.code,
                                      partitionsRemaining: Set[TopicAndPartition])
  extends RequestOrResponse() {
  def sizeInBytes(): Int ={
    var size =
      4 /* correlation id */ +
        2 /* error code */ +
        4 /* number of responses */
    for (topicAndPartition <- partitionsRemaining) {
      size +=
        2 + topicAndPartition.topic.length /* topic */ +
        4 /* partition */
    }
    size
  }

  def writeTo(buffer: ByteBuffer) {
    buffer.putInt(correlationId)
    buffer.putShort(errorCode)
    buffer.putInt(partitionsRemaining.size)
    for (topicAndPartition:TopicAndPartition <- partitionsRemaining){
      writeShortString(buffer, topicAndPartition.topic)
      buffer.putInt(topicAndPartition.partition)
    }
  }

  override def describe(details: Boolean):String = { toString }

}
