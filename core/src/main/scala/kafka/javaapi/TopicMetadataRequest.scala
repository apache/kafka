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

import kafka.api._
import java.nio.ByteBuffer
import scala.collection.JavaConversions

class TopicMetadataRequest(val versionId: Short,
                           override val correlationId: Int,
                           val clientId: String,
                           val topics: java.util.List[String])
    extends RequestOrResponse(Some(kafka.api.RequestKeys.MetadataKey), correlationId) {

  val underlying: kafka.api.TopicMetadataRequest =
    new kafka.api.TopicMetadataRequest(versionId, correlationId, clientId, JavaConversions.asBuffer(topics))

  def this(topics: java.util.List[String]) =
    this(kafka.api.TopicMetadataRequest.CurrentVersion, 0, kafka.api.TopicMetadataRequest.DefaultClientId, topics)

  def this(topics: java.util.List[String], correlationId: Int) =
    this(kafka.api.TopicMetadataRequest.CurrentVersion, correlationId, kafka.api.TopicMetadataRequest.DefaultClientId, topics)

  def writeTo(buffer: ByteBuffer) = underlying.writeTo(buffer)

  def sizeInBytes: Int = underlying.sizeInBytes()

}
