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

package kafka.server.metadata

import java.util
import java.util.Collections

import org.apache.kafka.common.{TopicPartition, Uuid}
import org.slf4j.Logger

case class MetadataImageBuilder(brokerId: Int,
                                log: Logger,
                                prevImage: MetadataImage) {
  private var _partitionsBuilder: MetadataPartitionsBuilder = null
  private var _controllerId = prevImage.controllerId
  private var _brokersBuilder: MetadataBrokersBuilder = null

  def partitionsBuilder(): MetadataPartitionsBuilder = {
    if (_partitionsBuilder == null) {
      _partitionsBuilder = new MetadataPartitionsBuilder(brokerId, prevImage.partitions)
    }
    _partitionsBuilder
  }

  def hasPartitionChanges: Boolean = _partitionsBuilder != null

  def topicIdToName(topicId: Uuid): Option[String] = {
    if (_partitionsBuilder != null) {
      _partitionsBuilder.topicIdToName(topicId)
    } else {
      prevImage.topicIdToName(topicId)
    }
  }

  def topicNameToId(topicName: String): Option[Uuid] = {
    if (_partitionsBuilder != null) {
      _partitionsBuilder.topicNameToId(topicName)
    } else {
      prevImage.topicNameToId(topicName)
    }
  }

  def controllerId(controllerId: Option[Int]): Unit = {
    _controllerId = controllerId
  }

  def brokersBuilder(): MetadataBrokersBuilder = {
    if (_brokersBuilder == null) {
      _brokersBuilder = new MetadataBrokersBuilder(log, prevImage.brokers)
    }
    _brokersBuilder
  }

  def broker(brokerId: Int): Option[MetadataBroker] = {
    if (_brokersBuilder == null) {
      prevImage.brokers.get(brokerId)
    } else {
      _brokersBuilder.get(brokerId)
    }
  }

  def partition(topicName: String, partitionId: Int): Option[MetadataPartition] = {
    if (_partitionsBuilder == null) {
      prevImage.partitions.topicPartition(topicName, partitionId)
    } else {
      _partitionsBuilder.get(topicName, partitionId)
    }
  }

  def hasChanges: Boolean = {
    _partitionsBuilder != null ||
      !_controllerId.equals(prevImage.controllerId) ||
      _brokersBuilder != null
  }

  def build(): MetadataImage = {
    val nextPartitions = if (_partitionsBuilder == null) {
      prevImage.partitions
    } else {
      _partitionsBuilder.build()
    }
    MetadataImage(nextPartitions, _controllerId, brokers())
  }

  def brokers(): MetadataBrokers = {
    if (_brokersBuilder == null) {
      prevImage.brokers
    } else {
      _brokersBuilder.build()
    }
  }
}

case class MetadataImage(partitions: MetadataPartitions,
                         controllerId: Option[Int],
                         brokers: MetadataBrokers) {
  def this() = {
    this(MetadataPartitions(Collections.emptyMap(), Collections.emptyMap()),
      None,
      new MetadataBrokers(Collections.emptyList(), new util.HashMap[Integer, MetadataBroker]()))
  }

  def contains(partition: TopicPartition): Boolean =
    partitions.topicPartition(partition.topic(), partition.partition()).isDefined

  def contains(topic: String): Boolean = partitions.topicPartitions(topic).hasNext

  def aliveBroker(id: Int): Option[MetadataBroker] = brokers.aliveBroker(id)

  def numAliveBrokers(): Int = brokers.aliveBrokers().size

  def controller(): Option[MetadataBroker] = controllerId.flatMap(id => brokers.aliveBroker(id))

  def topicIdToName(uuid: Uuid): Option[String] = {
    partitions.topicIdToName(uuid)
  }

  def topicNameToId(name: String): Option[Uuid] = {
    partitions.topicNameToId(name)
  }
}

