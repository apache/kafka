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
import org.apache.kafka.common.protocol.Errors
import scala.collection.JavaConverters._

class OffsetCommitResponse(private val underlying: kafka.api.OffsetCommitResponse) {

  def errors: java.util.Map[TopicAndPartition, Errors] = underlying.commitStatus.asJava

  def hasError = underlying.hasError

  def error(topicAndPartition: TopicAndPartition) = underlying.commitStatus(topicAndPartition)

  def errorCode(topicAndPartition: TopicAndPartition) = error(topicAndPartition).code
}

object OffsetCommitResponse {
  def readFrom(buffer: ByteBuffer) = new OffsetCommitResponse(kafka.api.OffsetCommitResponse.readFrom(buffer))
}
