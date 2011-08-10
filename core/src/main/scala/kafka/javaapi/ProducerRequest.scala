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
package kafka.javaapi

import kafka.network.Request
import kafka.api.RequestKeys
import java.nio.ByteBuffer

class ProducerRequest(val topic: String,
                      val partition: Int,
                      val messages: kafka.javaapi.message.ByteBufferMessageSet) extends Request(RequestKeys.Produce) {
  import Implicits._
  private val underlying = new kafka.api.ProducerRequest(topic, partition, messages)

  def writeTo(buffer: ByteBuffer) { underlying.writeTo(buffer) }

  def sizeInBytes(): Int = underlying.sizeInBytes

  def getTranslatedPartition(randomSelector: String => Int): Int =
    underlying.getTranslatedPartition(randomSelector)

  override def toString: String =
    underlying.toString

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
