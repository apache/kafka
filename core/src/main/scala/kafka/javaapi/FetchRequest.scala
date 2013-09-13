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

import java.nio.ByteBuffer
import kafka.common.TopicAndPartition
import kafka.api.{Request, PartitionFetchInfo}
import scala.collection.mutable

class FetchRequest(correlationId: Int,
                   clientId: String,
                   maxWait: Int,
                   minBytes: Int,
                   requestInfo: java.util.Map[TopicAndPartition, PartitionFetchInfo]) {

  val underlying = {
    val scalaMap: Map[TopicAndPartition, PartitionFetchInfo] = {
      import scala.collection.JavaConversions._
      (requestInfo: mutable.Map[TopicAndPartition, PartitionFetchInfo]).toMap
    }
    kafka.api.FetchRequest(
      correlationId = correlationId,
      clientId = clientId,
      replicaId = Request.OrdinaryConsumerId,
      maxWait = maxWait,
      minBytes = minBytes,
      requestInfo = scalaMap
    )
  }

  override def toString = underlying.toString

  override def equals(other: Any) = canEqual(other) && {
    val otherFetchRequest = other.asInstanceOf[kafka.javaapi.FetchRequest]
    this.underlying.equals(otherFetchRequest.underlying)
  }

  def canEqual(other: Any) = other.isInstanceOf[kafka.javaapi.FetchRequest]

  override def hashCode = underlying.hashCode

}

