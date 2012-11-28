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

import java.nio._
import kafka.message._
import kafka.network._
import kafka.utils._

object ProducerRequest {
  val RandomPartition = -1
  
  def readFrom(buffer: ByteBuffer): ProducerRequest = {
    val topic = Utils.readShortString(buffer, "UTF-8")
    val partition = buffer.getInt
    val messageSetSize = buffer.getInt
    val messageSetBuffer = buffer.slice()
    messageSetBuffer.limit(messageSetSize)
    buffer.position(buffer.position + messageSetSize)
    new ProducerRequest(topic, partition, new ByteBufferMessageSet(messageSetBuffer))
  }
}

class ProducerRequest(val topic: String,
                      val partition: Int,
                      val messages: ByteBufferMessageSet) extends Request(RequestKeys.Produce) {

  def writeTo(buffer: ByteBuffer) {
    Utils.writeShortString(buffer, topic, "UTF-8")
    buffer.putInt(partition)
    buffer.putInt(messages.serialized.limit)
    buffer.put(messages.serialized)
    messages.serialized.rewind
  }
  
  def sizeInBytes(): Int = 2 + topic.length + 4 + 4 + messages.sizeInBytes.asInstanceOf[Int]

  def getTranslatedPartition(randomSelector: String => Int): Int = {
    if (partition == ProducerRequest.RandomPartition)
      return randomSelector(topic)
    else 
      return partition
  }

  override def toString: String = {
    val builder = new StringBuilder()
    builder.append("ProducerRequest(")
    builder.append(topic + ",")
    builder.append(partition + ",")
    builder.append(messages.sizeInBytes)
    builder.append(")")
    builder.toString
  }

  override def equals(other: Any): Boolean = {
    other match {
      case that: ProducerRequest =>
        (that canEqual this) && topic == that.topic && partition == that.partition &&
                messages.equals(that.messages) 
      case _ => false
    }
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[ProducerRequest]

  override def hashCode: Int = 31 + (17 * partition) + topic.hashCode + messages.hashCode

}
