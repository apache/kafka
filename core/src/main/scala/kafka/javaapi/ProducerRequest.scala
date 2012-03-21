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
import kafka.api.{RequestKeys, TopicData}
import java.nio.ByteBuffer

class ProducerRequest(val correlationId: Int,
                      val clientId: String,
                      val requiredAcks: Short,
                      val ackTimeout: Int,
                      val data: Array[TopicData]) extends Request(RequestKeys.Produce) {
	
  val underlying = new kafka.api.ProducerRequest(correlationId, clientId, requiredAcks, ackTimeout, data)

  def writeTo(buffer: ByteBuffer) { underlying.writeTo(buffer) }

  def sizeInBytes(): Int = underlying.sizeInBytes

  override def toString: String =
    underlying.toString

  override def equals(other: Any): Boolean = underlying.equals(other)

  def canEqual(other: Any): Boolean = other.isInstanceOf[ProducerRequest]

  override def hashCode: Int = underlying.hashCode

}