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

import org.apache.kafka.common.message.LeaderAndIsrRequestData
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState
import org.apache.kafka.common.message.UpdateMetadataRequestData.UpdateMetadataPartitionState
import org.apache.kafka.common.metadata.{IsrChangeRecord, PartitionRecord}
import org.apache.kafka.common.{TopicPartition, Uuid}

import scala.jdk.CollectionConverters._


object MetadataPartition {
  def apply(name: String, record: PartitionRecord): MetadataPartition = {
    MetadataPartition(name,
      record.partitionId(),
      record.leader(),
      record.leaderEpoch(),
      record.replicas(),
      record.isr(),
      Collections.emptyList(), // TODO KAFKA-12285 handle offline replicas
      Collections.emptyList(),
      Collections.emptyList())
  }

  def apply(prevPartition: Option[MetadataPartition],
            partition: UpdateMetadataPartitionState): MetadataPartition = {
    new MetadataPartition(partition.topicName(),
      partition.partitionIndex(),
      partition.leader(),
      partition.leaderEpoch(),
      partition.replicas(),
      partition.isr(),
      partition.offlineReplicas(),
      prevPartition.flatMap(p => Some(p.addingReplicas)).getOrElse(Collections.emptyList()),
      prevPartition.flatMap(p => Some(p.removingReplicas)).getOrElse(Collections.emptyList())
    )
  }
}

case class MetadataPartition(topicName: String,
                             partitionIndex: Int,
                             leaderId: Int,
                             leaderEpoch: Int,
                             replicas: util.List[Integer],
                             isr: util.List[Integer],
                             offlineReplicas: util.List[Integer],
                             addingReplicas: util.List[Integer],
                             removingReplicas: util.List[Integer]) {
  def toTopicPartition: TopicPartition = new TopicPartition(topicName, partitionIndex)

  def toLeaderAndIsrPartitionState(isNew: Boolean): LeaderAndIsrRequestData.LeaderAndIsrPartitionState = {
    new LeaderAndIsrPartitionState().setTopicName(topicName).
      setPartitionIndex(partitionIndex).
      setLeader(leaderId).
      setLeaderEpoch(leaderEpoch).
      setReplicas(replicas).
      setIsr(isr).
      setAddingReplicas(addingReplicas).
      setRemovingReplicas(removingReplicas).
      setIsNew(isNew)
    // Note: we don't set ZKVersion here.
  }

  def isReplicaFor(brokerId: Int): Boolean = replicas.contains(Integer.valueOf(brokerId))

  def copyWithIsrChanges(record: IsrChangeRecord): MetadataPartition = {
    MetadataPartition(topicName,
      partitionIndex,
      record.leader(),
      record.leaderEpoch(),
      replicas,
      record.isr(),
      offlineReplicas,
      addingReplicas,
      removingReplicas)
  }
}

class MetadataPartitionsBuilder(val brokerId: Int,
                                val prevPartitions: MetadataPartitions) {
  private var newNameMap = prevPartitions.copyNameMap()
  private var newIdMap = prevPartitions.copyIdMap()
  private val changed = Collections.newSetFromMap[Any](new util.IdentityHashMap())
  private val _localChanged = new util.HashSet[MetadataPartition]
  private val _localRemoved = new util.HashSet[MetadataPartition]

  def topicIdToName(id: Uuid): Option[String] = Option(newIdMap.get(id))

  def removeTopicById(id: Uuid): Iterable[MetadataPartition] = {
    Option(newIdMap.remove(id)) match {
      case None => throw new RuntimeException(s"Unable to locate topic with ID $id")
      case Some(name) => newNameMap.remove(name).values().asScala
    }
  }

  def handleIsrChange(record: IsrChangeRecord): Unit = {
    Option(newIdMap.get(record.topicId())) match {
      case None => throw new RuntimeException(s"Unable to locate topic with ID ${record.topicId()}")
      case Some(name) => Option(newNameMap.get(name)) match {
        case None => throw new RuntimeException(s"Unable to locate topic with name $name")
        case Some(partitionMap) => Option(partitionMap.get(record.partitionId())) match {
          case None => throw new RuntimeException(s"Unable to locate $name-${record.partitionId}")
          case Some(partition) => set(partition.copyWithIsrChanges(record))
        }
      }
    }
  }

  def addUuidMapping(name: String, id: Uuid): Unit = {
    newIdMap.put(id, name)
  }

  def removeUuidMapping(id: Uuid): Unit = {
    newIdMap.remove(id)
  }

  def get(topicName: String, partitionId: Int): Option[MetadataPartition] = {
    Option(newNameMap.get(topicName)).flatMap(m => Option(m.get(partitionId)))
  }

  def set(partition: MetadataPartition): Unit = {
    val prevPartitionMap = newNameMap.get(partition.topicName)
    val newPartitionMap = if (prevPartitionMap == null) {
      val m = new util.HashMap[Int, MetadataPartition](1)
      changed.add(m)
      m
    } else if (changed.contains(prevPartitionMap)) {
      prevPartitionMap
    } else {
      val m = new util.HashMap[Int, MetadataPartition](prevPartitionMap.size() + 1)
      m.putAll(prevPartitionMap)
      changed.add(m)
      m
    }
    val prevPartition = newPartitionMap.put(partition.partitionIndex, partition)
    if (partition.isReplicaFor(brokerId)) {
      _localChanged.add(partition)
    } else if (prevPartition != null && prevPartition.isReplicaFor(brokerId)) {
      _localRemoved.add(prevPartition)
    }
    newNameMap.put(partition.topicName, newPartitionMap)
  }

  def remove(topicName: String, partitionId: Int): Unit = {
    val prevPartitionMap = newNameMap.get(topicName)
    if (prevPartitionMap != null) {
      if (changed.contains(prevPartitionMap)) {
        val prevPartition = prevPartitionMap.remove(partitionId)
        if (prevPartition.isReplicaFor(brokerId)) {
          _localRemoved.add(prevPartition)
        }
      } else {
        Option(prevPartitionMap.get(partitionId)).foreach { prevPartition =>
          if (prevPartition.isReplicaFor(brokerId)) {
            _localRemoved.add(prevPartition)
          }
          val newPartitionMap = new util.HashMap[Int, MetadataPartition](prevPartitionMap.size() - 1)
          prevPartitionMap.forEach { (prevPartitionId, prevPartition) =>
            if (!prevPartitionId.equals(partitionId)) {
              newPartitionMap.put(prevPartitionId, prevPartition)
            }
          }
          changed.add(newPartitionMap)
          newNameMap.put(topicName, newPartitionMap)
        }
      }
    }
  }

  def build(): MetadataPartitions = {
    val result = MetadataPartitions(newNameMap, newIdMap)
    newNameMap = Collections.unmodifiableMap(newNameMap)
    newIdMap = Collections.unmodifiableMap(newIdMap)
    result
  }

  def localChanged(): collection.Set[MetadataPartition] = _localChanged.asScala

  def localRemoved(): collection.Set[MetadataPartition] = _localRemoved.asScala
}

case class MetadataPartitions(private val nameMap: util.Map[String, util.Map[Int, MetadataPartition]],
                              private val idMap: util.Map[Uuid, String]) {
  def topicIdToName(uuid: Uuid): Option[String] = Option(idMap.get(uuid))

  def copyNameMap(): util.Map[String, util.Map[Int, MetadataPartition]] = {
    val copy = new util.HashMap[String, util.Map[Int, MetadataPartition]](nameMap.size())
    copy.putAll(nameMap)
    copy
  }

  def copyIdMap(): util.Map[Uuid, String] = {
    val copy = new util.HashMap[Uuid, String](idMap.size())
    copy.putAll(idMap)
    copy
  }

  def allPartitions(): Iterator[MetadataPartition] = new AllPartitionsIterator(nameMap).asScala

  def allTopicNames(): collection.Set[String] = nameMap.keySet().asScala

  def numTopicPartitions(topicName: String): Option[Int] = {
    val partitionMap = nameMap.get(topicName)
    if (partitionMap == null) {
      None
    } else {
      Some(partitionMap.size())
    }
  }

  def topicPartitions(topicName: String): Iterator[MetadataPartition] = {
    val partitionMap = nameMap.get(topicName)
    if (partitionMap == null) {
      Collections.emptyIterator().asScala
    } else {
      partitionMap.values().iterator().asScala
    }
  }

  def topicPartition(topicName: String, partitionId: Int): Option[MetadataPartition] = {
    Option(nameMap.get(topicName)).flatMap(m => Option(m.get(partitionId)))
  }

  def contains(topicName: String): Boolean = nameMap.containsKey(topicName)
}

class AllPartitionsIterator(nameMap: util.Map[String, util.Map[Int, MetadataPartition]])
    extends util.Iterator[MetadataPartition] {

  val outerIterator: util.Iterator[util.Map[Int, MetadataPartition]] = nameMap.values().iterator()

  var innerIterator: util.Iterator[MetadataPartition] = Collections.emptyIterator()

  var _next: MetadataPartition = _

  override def hasNext: Boolean = {
    if (_next != null) {
      true
    } else {
      while (!innerIterator.hasNext) {
        if (!outerIterator.hasNext) {
          return false
        } else {
          innerIterator = outerIterator.next().values().iterator()
        }
      }
      _next = innerIterator.next()
      true
    }
  }

  override def next(): MetadataPartition = {
    if (!hasNext()) {
      throw new NoSuchElementException()
    }
    val result = _next
    _next = null
    result
  }
}
