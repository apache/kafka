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

import java.util

import kafka.common.TopicAndPartition
import kafka.api.{PartitionFetchInfo, Request}

import scala.collection.JavaConverters._

object FetchRequest {
  private def seqToLinkedHashMap[K, V](s: Seq[(K, V)]): util.LinkedHashMap[K, V] = {
    val map = new util.LinkedHashMap[K, V]
    s.foreach { case (k, v) => map.put(k, v) }
    map
  }
}

class FetchRequest(correlationId: Int,
                   clientId: String,
                   maxWait: Int,
                   minBytes: Int,
                   requestInfo: util.LinkedHashMap[TopicAndPartition, PartitionFetchInfo]) {

  @deprecated("The order of partitions in `requestInfo` is relevant, so this constructor is deprecated in favour of the " +
    "one that takes a LinkedHashMap", since = "0.10.1.0")
  def this(correlationId: Int, clientId: String, maxWait: Int, minBytes: Int,
    requestInfo: java.util.Map[TopicAndPartition, PartitionFetchInfo]) {
    this(correlationId, clientId, maxWait, minBytes,
      FetchRequest.seqToLinkedHashMap(kafka.api.FetchRequest.shuffle(requestInfo.asScala.toSeq)))
  }

  val underlying = kafka.api.FetchRequest(
    correlationId = correlationId,
    clientId = clientId,
    replicaId = Request.OrdinaryConsumerId,
    maxWait = maxWait,
    minBytes = minBytes,
    requestInfo = requestInfo.asScala.toBuffer
  )

  override def toString = underlying.toString

  override def equals(other: Any) = canEqual(other) && {
    val otherFetchRequest = other.asInstanceOf[kafka.javaapi.FetchRequest]
    this.underlying.equals(otherFetchRequest.underlying)
  }

  def canEqual(other: Any) = other.isInstanceOf[kafka.javaapi.FetchRequest]

  override def hashCode = underlying.hashCode

}

