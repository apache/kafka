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

import kafka.cluster.BrokerEndPoint
import scala.collection.JavaConversions

private[javaapi] object MetadataListImplicits {
  implicit def toJavaTopicMetadataList(topicMetadataSeq: Seq[kafka.api.TopicMetadata]):
  java.util.List[kafka.javaapi.TopicMetadata] = {
    import JavaConversions._
    topicMetadataSeq.map(new kafka.javaapi.TopicMetadata(_))
  }

  implicit def toPartitionMetadataList(partitionMetadataSeq: Seq[kafka.api.PartitionMetadata]):
  java.util.List[kafka.javaapi.PartitionMetadata] = {
    import JavaConversions._
    partitionMetadataSeq.map(new kafka.javaapi.PartitionMetadata(_))
  }
}

class TopicMetadata(private val underlying: kafka.api.TopicMetadata) {
  def topic: String = underlying.topic

  def partitionsMetadata: java.util.List[PartitionMetadata] = {
    import kafka.javaapi.MetadataListImplicits._
    underlying.partitionsMetadata
  }

  def errorCode: Short = underlying.errorCode

  def sizeInBytes: Int = underlying.sizeInBytes

  override def toString = underlying.toString
}


class PartitionMetadata(private val underlying: kafka.api.PartitionMetadata) {
  def partitionId: Int = underlying.partitionId

  def leader: BrokerEndPoint = {
    import kafka.javaapi.Implicits._
    underlying.leader
  }

  def replicas: java.util.List[BrokerEndPoint] = {
    import JavaConversions._
    underlying.replicas
  }

  def isr: java.util.List[BrokerEndPoint] = {
    import JavaConversions._
    underlying.isr
  }

  def errorCode: Short = underlying.errorCode

  def sizeInBytes: Int = underlying.sizeInBytes

  override def toString = underlying.toString
}
