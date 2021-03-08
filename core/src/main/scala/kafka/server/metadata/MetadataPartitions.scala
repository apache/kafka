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
import org.apache.kafka.common.metadata.{PartitionChangeRecord, PartitionRecord}
import org.apache.kafka.common.{TopicPartition, Uuid}

import scala.jdk.CollectionConverters._


object MetadataPartition {
  val NO_LEADER_CHANGE = -2

  def apply(name: String, record: PartitionRecord): MetadataPartition = {
    MetadataPartition(name,
      record.partitionId(),
      record.leader(),
      record.leaderEpoch(),
      record.replicas(),
      record.isr(),
      record.partitionEpoch(),
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
      partition.zkVersion(),
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
                             partitionEpoch: Int,
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
      setIsNew(isNew).
      setZkVersion(partitionEpoch)
  }

  def isReplicaFor(brokerId: Int): Boolean = replicas.contains(Integer.valueOf(brokerId))

  def merge(record: PartitionChangeRecord): MetadataPartition = {
    val (newLeader, newLeaderEpoch) = if (record.leader() == MetadataPartition.NO_LEADER_CHANGE) {
      (leaderId, leaderEpoch)
    } else {
      (record.leader(), leaderEpoch + 1)
    }
    val newIsr = if (record.isr() == null) {
      isr
    } else {
      record.isr()
    }
    MetadataPartition(topicName,
      partitionIndex,
      newLeader,
      newLeaderEpoch,
      replicas,
      newIsr,
      partitionEpoch + 1,
      offlineReplicas,
      addingReplicas,
      removingReplicas)
  }
}

class MetadataPartitionsBuilder(val brokerId: Int,
                                val prevPartitions: MetadataPartitions) {
  private var newNameMap = prevPartitions.copyNameMap()
  private var newIdMap = prevPartitions.copyIdMap()
  private var newReverseIdMap = prevPartitions.copyReverseIdMap()
  private val changed = Collections.newSetFromMap[Any](new util.IdentityHashMap())
  private val _localChanged = new util.HashSet[MetadataPartition]
  private val _localRemoved = new util.HashSet[MetadataPartition]

  def topicIdToName(id: Uuid): Option[String] = Option(newIdMap.get(id))

  def topicNameToId(name: String): Option[Uuid] = Option(newReverseIdMap.get(name))

  def removeTopicById(id: Uuid): Iterable[MetadataPartition] = {
    val name = Option(newIdMap.remove(id)).getOrElse {
      throw new RuntimeException(s"Unable to locate topic with ID $id")
    }

    newReverseIdMap.remove(name)

    val prevPartitionMap = newNameMap.remove(name)
    if (prevPartitionMap == null) {
      Seq.empty
    } else {
      changed.remove(prevPartitionMap)

      val removedPartitions = prevPartitionMap.values
      if (prevImageHasTopicId(id)) {
        removedPartitions.forEach { partition =>
          if (partition.isReplicaFor(brokerId)) {
            _localRemoved.add(partition)
          }
        }
      } else {
        removedPartitions.forEach { partition =>
          if (partition.isReplicaFor(brokerId)) {
            _localChanged.remove(partition)
          }
        }
      }
      removedPartitions.asScala
    }
  }

  def handleChange(record: PartitionChangeRecord): Unit = {
    topicIdToName(record.topicId) match {
      case None => throw new RuntimeException(s"Unable to locate topic with ID ${record.topicId()}")
      case Some(name) => Option(newNameMap.get(name)) match {
        case None => throw new RuntimeException(s"Unable to locate topic with name $name")
        case Some(partitionMap) => Option(partitionMap.get(record.partitionId())) match {
          case None => throw new RuntimeException(s"Unable to locate $name-${record.partitionId}")
          case Some(partition) => set(partition.merge(record))
        }
      }
    }
  }

  def addUuidMapping(name: String, id: Uuid): Unit = {
    newIdMap.put(id, name)
    newReverseIdMap.put(name, id)
  }

  def removeUuidMapping(id: Uuid): Unit = {
    val topicName = newIdMap.remove(id)
    if (topicName != null) {
      newReverseIdMap.remove(topicName)
    }
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
    } else if (prevPartition != null) {
      maybeAddToLocalRemoved(prevPartition)
    }
    newNameMap.put(partition.topicName, newPartitionMap)
  }

  private def maybeAddToLocalRemoved(partition: MetadataPartition): Unit = {
    if (partition.isReplicaFor(brokerId)) {
      val currentTopicId = newReverseIdMap.get(partition.topicName)
      val prevImageHasTopic = if (currentTopicId != null) {
        prevImageHasTopicId(currentTopicId)
      } else {
        prevPartitions.allTopicNames().contains(partition.topicName)
      }

      if (prevImageHasTopic) {
        _localRemoved.add(partition)
      }
    }
  }

  private def prevImageHasTopicId(topicId: Uuid): Boolean = {
    prevPartitions.topicIdToName(topicId).isDefined
  }

  def remove(topicName: String, partitionId: Int): Unit = {
    val prevPartitionMap = newNameMap.get(topicName)
    if (prevPartitionMap != null) {
      val removedPartition = if (changed.contains(prevPartitionMap)) {
        Option(prevPartitionMap.remove(partitionId))
      } else {
        Option(prevPartitionMap.get(partitionId)).map { prevPartition =>
          val newPartitionMap = new util.HashMap[Int, MetadataPartition](prevPartitionMap.size() - 1)
          prevPartitionMap.forEach { (prevPartitionId, prevPartition) =>
            if (prevPartitionId != partitionId) {
              newPartitionMap.put(prevPartitionId, prevPartition)
            }
          }
          changed.add(newPartitionMap)
          newNameMap.put(topicName, newPartitionMap)
          prevPartition
        }
      }
      removedPartition.foreach(maybeAddToLocalRemoved)
    }
  }

  def build(): MetadataPartitions = {
    val result = new MetadataPartitions(newNameMap, newIdMap, newReverseIdMap)
    newNameMap = Collections.unmodifiableMap(newNameMap)
    newIdMap = Collections.unmodifiableMap(newIdMap)
    newReverseIdMap = Collections.unmodifiableMap(newReverseIdMap)
    result
  }

  def localChanged(): collection.Set[MetadataPartition] = _localChanged.asScala

  def localRemoved(): collection.Set[MetadataPartition] = _localRemoved.asScala
}

object MetadataPartitions {
  def apply(nameMap: util.Map[String, util.Map[Int, MetadataPartition]],
            idMap: util.Map[Uuid, String]): MetadataPartitions = {
    val reverseMap = idMap.asScala.map(_.swap).toMap.asJava
    new MetadataPartitions(nameMap, idMap, reverseMap)
  }
}

case class MetadataPartitions(private val nameMap: util.Map[String, util.Map[Int, MetadataPartition]],
                              private val idMap: util.Map[Uuid, String],
                              private val reverseIdMap: util.Map[String, Uuid]) {

  def topicIdToName(uuid: Uuid): Option[String] = Option(idMap.get(uuid))

  def topicNameToId(name: String): Option[Uuid] = Option(reverseIdMap.get(name))

  def copyNameMap(): util.Map[String, util.Map[Int, MetadataPartition]] = {
    new util.HashMap(nameMap)
  }

  def copyIdMap(): util.Map[Uuid, String] = {
    new util.HashMap(idMap)
  }

  def copyReverseIdMap(): util.Map[String, Uuid] = {
    new util.HashMap(reverseIdMap)
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
