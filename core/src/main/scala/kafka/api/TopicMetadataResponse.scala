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

import kafka.cluster.BrokerEndPoint
import java.nio.ByteBuffer

object TopicMetadataResponse {

  def readFrom(buffer: ByteBuffer): TopicMetadataResponse = {
    val correlationId = buffer.getInt
    val brokerCount = buffer.getInt
    val brokers = (0 until brokerCount).map(_ => BrokerEndPoint.readFrom(buffer))
    val brokerMap = brokers.map(b => (b.id, b)).toMap
    val topicCount = buffer.getInt
    val topicsMetadata = (0 until topicCount).map(_ => TopicMetadata.readFrom(buffer, brokerMap))
    new TopicMetadataResponse(brokers, topicsMetadata, correlationId)
  }
}

case class TopicMetadataResponse(brokers: Seq[BrokerEndPoint],
                                 topicsMetadata: Seq[TopicMetadata],
                                 correlationId: Int)
    extends RequestOrResponse() {
  val sizeInBytes: Int = {
    4 + 4 + brokers.map(_.sizeInBytes).sum + 4 + topicsMetadata.map(_.sizeInBytes).sum
  }

  def writeTo(buffer: ByteBuffer) {
    buffer.putInt(correlationId)
    /* brokers */
    buffer.putInt(brokers.size)
    brokers.foreach(_.writeTo(buffer))
    /* topic metadata */
    buffer.putInt(topicsMetadata.length)
    topicsMetadata.foreach(_.writeTo(buffer))
  }

  override def describe(details: Boolean):String = { toString }
}
