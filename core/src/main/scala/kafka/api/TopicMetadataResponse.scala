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

import kafka.cluster.Broker
import java.nio.ByteBuffer

object TopicMetadataResponse {

  def readFrom(buffer: ByteBuffer): TopicMetadataResponse = {
    val correlationId = buffer.getInt
    val brokerCount = buffer.getInt
    val brokers = (0 until brokerCount).map(_ => Broker.readFrom(buffer))
    val brokerMap = brokers.map(b => (b.id, b)).toMap
    val topicCount = buffer.getInt
    val topicsMetadata = (0 until topicCount).map(_ => TopicMetadata.readFrom(buffer, brokerMap))
    new TopicMetadataResponse(topicsMetadata, correlationId)
  }
}

case class TopicMetadataResponse(topicsMetadata: Seq[TopicMetadata],
                                 override val correlationId: Int)
    extends RequestOrResponse(correlationId = correlationId) {
  val sizeInBytes: Int = {
    val brokers = extractBrokers(topicsMetadata).values
    4 + 4 + brokers.map(_.sizeInBytes).sum + 4 + topicsMetadata.map(_.sizeInBytes).sum
  }

  def writeTo(buffer: ByteBuffer) {
    buffer.putInt(correlationId)
    /* brokers */
    val brokers = extractBrokers(topicsMetadata).values
    buffer.putInt(brokers.size)
    brokers.foreach(_.writeTo(buffer))
    /* topic metadata */
    buffer.putInt(topicsMetadata.length)
    topicsMetadata.foreach(_.writeTo(buffer))
  }
    
  def extractBrokers(topicMetadatas: Seq[TopicMetadata]): Map[Int, Broker] = {
    val parts = topicsMetadata.flatMap(_.partitionsMetadata)
    val brokers = (parts.flatMap(_.replicas)) ++ (parts.map(_.leader).collect{case Some(l) => l})
    brokers.map(b => (b.id, b)).toMap
  }
}
