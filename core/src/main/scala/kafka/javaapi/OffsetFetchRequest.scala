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

import kafka.common.TopicAndPartition
import collection.JavaConverters._

class OffsetFetchRequest(groupId: String,
                         requestInfo: java.util.List[TopicAndPartition],
                         versionId: Short,
                         correlationId: Int,
                         clientId: String) {

  def this(groupId: String,
           requestInfo: java.util.List[TopicAndPartition],
           correlationId: Int,
           clientId: String) {
    // by default bind to version 0 so that it fetches from ZooKeeper
    this(groupId, requestInfo, 0, correlationId, clientId)
  }

  val underlying = {
    kafka.api.OffsetFetchRequest(
      groupId = groupId,
      requestInfo = requestInfo.asScala,
      versionId = versionId,
      correlationId = correlationId,
      clientId = clientId
    )
  }

  override def toString = underlying.toString

  override def equals(obj: Any): Boolean = {
    obj match {
      case null => false
      case other: OffsetFetchRequest => this.underlying.equals(other.underlying)
      case _ => false
    }
  }

  override def hashCode = underlying.hashCode
}


