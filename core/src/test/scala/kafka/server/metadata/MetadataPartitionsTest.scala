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

import java.util.Collections
import java.util.concurrent.TimeUnit

import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.common.metadata.PartitionChangeRecord
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{Test, Timeout}

import scala.collection.mutable
import scala.jdk.CollectionConverters._


@Timeout(value = 120000, unit = TimeUnit.MILLISECONDS)
class MetadataPartitionsTest {

  private val emptyPartitions = MetadataPartitions(Collections.emptyMap(), Collections.emptyMap())

  private def newPartition(topicName: String,
                           partitionIndex: Int,
                           replicas: Option[Seq[Int]] = None,
                           isr: Option[Seq[Int]] = None,
                           numBrokers: Int = 6): MetadataPartition = {
    val effectiveReplicas = asJavaList(replicas.getOrElse {
      val preferredLeaderId = partitionIndex % numBrokers
      List(preferredLeaderId, preferredLeaderId + 1, preferredLeaderId + 2)
    })

    val effectiveIsr = isr match {
      case None => effectiveReplicas
      case Some(s) => s.map(Integer.valueOf).toList.asJava
    }
    new MetadataPartition(topicName,
      partitionIndex,
      effectiveReplicas.asScala.head,
      leaderEpoch = 100,
      effectiveReplicas,
      effectiveIsr,
      partitionEpoch = 200,
      Collections.emptyList(),
      Collections.emptyList(),
      Collections.emptyList())
  }

  @Test
  def testBuildPartitions(): Unit = {
    val builder = new MetadataPartitionsBuilder(0, emptyPartitions)
    assertEquals(None, builder.get("foo", 0))
    builder.set(newPartition("foo", 0))
    assertEquals(Some(newPartition("foo", 0)), builder.get("foo", 0))
    assertEquals(None, builder.get("foo", 1))
    builder.set(newPartition("foo", 1))
    builder.set(newPartition("bar", 0))
    val partitions = builder.build()
    assertEquals(Some(newPartition("foo", 0)), partitions.topicPartition("foo", 0))
    assertEquals(Some(newPartition("foo", 1)), partitions.topicPartition("foo", 1))
    assertEquals(None, partitions.topicPartition("foo", 2))
    assertEquals(Some(newPartition("bar", 0)), partitions.topicPartition("bar", 0))
  }

  @Test
  def testAllPartitionsIterator(): Unit = {
    val builder = new MetadataPartitionsBuilder(0, emptyPartitions)
    val expected = new mutable.HashSet[MetadataPartition]()
    expected += newPartition("foo", 0)
    expected += newPartition("foo", 1)
    expected += newPartition("foo", 2)
    expected += newPartition("bar", 0)
    expected += newPartition("bar", 1)
    expected += newPartition("baz", 0)
    expected.foreach { builder.set }
    val partitions = builder.build()
    val found = new mutable.HashSet[MetadataPartition]()
    partitions.allPartitions().foreach { found += _ }
    assertEquals(expected, found)
  }

  @Test
  def testLocalChangedAndRemoved(): Unit = {
    val builder = new MetadataPartitionsBuilder(0, emptyPartitions)
    builder.set(newPartition("foo", 0))
    assertTrue(newPartition("foo", 0).isReplicaFor(0))
    assertFalse(newPartition("foo", 0).isReplicaFor(4))
    builder.set(newPartition("foo", 1))
    builder.set(newPartition("foo", 2))
    builder.set(newPartition("bar", 0))
    val expectedLocalChanged = new mutable.HashSet[MetadataPartition]()
    expectedLocalChanged += newPartition("foo", 0)
    expectedLocalChanged += newPartition("bar", 0)
    assertEquals(expectedLocalChanged, builder.localChanged())
    assertEquals(Set(), builder.localRemoved())
    val image = builder.build()
    assertEquals(Some(3), image.numTopicPartitions("foo"))
    assertEquals(None, image.numTopicPartitions("quux"))

    val builder2 = new MetadataPartitionsBuilder(1, image)
    builder2.set(newPartition("foo", 0, replicas = Some(Seq(2, 3, 4))))
    builder2.set(newPartition("foo", 1, isr = Some(Seq(0, 1))))
    builder2.set(newPartition("bar", 2))
    builder2.remove("bar", 0)
    builder2.remove("foo", 2)
    val expectedLocalChanged2 = new mutable.HashSet[MetadataPartition]()
    expectedLocalChanged2 += newPartition("foo", 1, isr = Some(Seq(0, 1)))
    assertEquals(expectedLocalChanged2, builder2.localChanged())
    val expectedLocalRemoved2 = new mutable.HashSet[MetadataPartition]()
    expectedLocalRemoved2 += newPartition("bar", 0)
    expectedLocalRemoved2 += newPartition("foo", 0)
    assertEquals(expectedLocalRemoved2, builder2.localRemoved())
  }

  @Test
  def testAllTopicNames(): Unit = {
    val builder = new MetadataPartitionsBuilder(0, emptyPartitions)
    createTopic("foo", numPartitions = 3, builder)
    createTopic("bar", numPartitions = 2, builder)
    createTopic("baz", numPartitions = 3, builder)
    val image = builder.build()
    val expectedTopicNames = new mutable.HashSet[String]()
    expectedTopicNames += "foo"
    expectedTopicNames += "bar"
    expectedTopicNames += "baz"
    assertEquals(expectedTopicNames, image.allTopicNames())
  }

  @Test
  def testUuidMappings(): Unit = {
    val builder = new MetadataPartitionsBuilder(0, emptyPartitions)
    builder.addUuidMapping("foo", Uuid.fromString("qbUrhSpXTau_836U7T5ktg"))
    builder.addUuidMapping("bar", Uuid.fromString("a1I0JF3yRzWFyOuY3F_vHw"))
    builder.removeUuidMapping(Uuid.fromString("gdMy05W7QWG4ZjWir1DjBw"))
    val image = builder.build()
    assertEquals(Some("foo"), image.topicIdToName(Uuid.fromString("qbUrhSpXTau_836U7T5ktg")))
    assertEquals(Some("bar"), image.topicIdToName(Uuid.fromString("a1I0JF3yRzWFyOuY3F_vHw")))
    assertEquals(None, image.topicIdToName(Uuid.fromString("gdMy05W7QWG4ZjWir1DjBw")))
  }

  @Test
  def testMergePartitionChangeRecord(): Unit = {
    val initialMetadata = newPartition(
      topicName = "foo",
      partitionIndex = 0,
      replicas = Some(Seq(1, 2, 3)),
      isr = Some(Seq(1, 2, 3))
    )
    assertEquals(1, initialMetadata.leaderId)

    // If only the ISR changes, then the leader epoch
    // remains the same and the partition epoch is bumped.
    val updatedIsr = initialMetadata.merge(new PartitionChangeRecord()
      .setPartitionId(0)
      .setIsr(asJavaList(Seq(1, 2))))
    assertEquals(asJavaList(Seq(1, 2)), updatedIsr.isr)
    assertEquals(initialMetadata.leaderEpoch, updatedIsr.leaderEpoch)
    assertEquals(initialMetadata.partitionEpoch + 1, updatedIsr.partitionEpoch)
    assertEquals(initialMetadata.leaderId, updatedIsr.leaderId)

    // If the leader changes, then both the leader epoch
    // and the partition epoch should get bumped.
    val updatedLeader = initialMetadata.merge(new PartitionChangeRecord()
      .setPartitionId(0)
      .setLeader(2)
      .setIsr(asJavaList(Seq(2, 3))))
    assertEquals(asJavaList(Seq(2, 3)), updatedLeader.isr)
    assertEquals(initialMetadata.leaderEpoch + 1, updatedLeader.leaderEpoch)
    assertEquals(initialMetadata.partitionEpoch + 1, updatedLeader.partitionEpoch)
    assertEquals(2, updatedLeader.leaderId)
  }

  @Test
  def testTopicCreateAndDelete(): Unit = {
    val topic = "foo"
    val numPartitions = 3
    val topicPartitions = (0 until numPartitions).map(new TopicPartition(topic, _)).toSet
    val builder = new MetadataPartitionsBuilder(0, emptyPartitions)
    val topicId = createTopic(topic, numPartitions, builder)
    val localTopicPartitions = localChanged(builder)

    assertTrue(localTopicPartitions.subsetOf(topicPartitions))
    assertTrue(localTopicPartitions.nonEmpty)
    assertNotEquals(topicPartitions, localTopicPartitions)

    builder.removeTopicById(topicId)
    assertEquals(None, builder.topicIdToName(topicId))
    assertEquals(None, builder.topicNameToId(topic))
    assertEquals(Set.empty, filterPartitions(builder, topicPartitions))
    assertEquals(Set.empty, localRemoved(builder))
    assertEquals(Set.empty, localChanged(builder))

    val metadata = builder.build()
    assertEquals(Set.empty, metadata.allTopicNames())
    assertEquals(None, metadata.topicIdToName(topicId))
    assertEquals(None, metadata.topicNameToId(topic))
    assertEquals(Set.empty, metadata.topicPartitions(topic).toSet)
  }

  @Test
  def testTopicRemoval(): Unit = {
    val brokerId = 0
    val topic = "foo"
    val numPartitions = 3
    val topicPartitions = (0 until numPartitions).map(new TopicPartition(topic, _)).toSet
    val createBuilder = new MetadataPartitionsBuilder(brokerId, emptyPartitions)
    val topicId = createTopic(topic, numPartitions, createBuilder)
    val localTopicPartitions = localChanged(createBuilder)
    val createMetadata = createBuilder.build()

    assertTrue(localTopicPartitions.subsetOf(topicPartitions))
    assertTrue(localTopicPartitions.nonEmpty)
    assertNotEquals(topicPartitions, localTopicPartitions)

    val deleteBuilder = new MetadataPartitionsBuilder(brokerId = 0, createMetadata)
    deleteBuilder.removeTopicById(topicId)
    assertEquals(None, deleteBuilder.topicIdToName(topicId))
    assertEquals(None, deleteBuilder.topicNameToId(topic))
    assertEquals(Set.empty, filterPartitions(deleteBuilder, topicPartitions))
    assertEquals(localTopicPartitions, localRemoved(deleteBuilder))
    assertEquals(Set.empty, localChanged(deleteBuilder))

    val deleteMetadata = deleteBuilder.build()
    assertEquals(Set.empty, deleteMetadata.allTopicNames())
    assertEquals(None, deleteMetadata.topicIdToName(topicId))
    assertEquals(None, deleteMetadata.topicNameToId(topic))
    assertEquals(Set.empty, deleteMetadata.topicPartitions(topic).toSet)
  }

  @Test
  def testTopicDeleteAndRecreate(): Unit = {
    val topic = "foo"
    val numPartitions = 3
    val initialBuilder = new MetadataPartitionsBuilder(0, emptyPartitions)
    val initialTopicId = createTopic(topic, numPartitions, initialBuilder)
    val initialLocalTopicPartitions = initialBuilder.localChanged().map(_.toTopicPartition).toSet
    val initialMetadata = initialBuilder.build()

    val recreateBuilder = new MetadataPartitionsBuilder(brokerId = 0, initialMetadata)
    recreateBuilder.removeTopicById(initialTopicId)
    assertEquals(initialLocalTopicPartitions, localRemoved(recreateBuilder))

    val recreatedNumPartitions = 10
    val recreatedTopicId = createTopic(topic, recreatedNumPartitions, recreateBuilder)
    val recreatedTopicPartitions = (0 until recreatedNumPartitions).map(new TopicPartition(topic, _)).toSet
    val recreatedLocalTopicPartitions = localChanged(recreateBuilder)

    assertTrue(recreatedLocalTopicPartitions.nonEmpty)
    assertNotEquals(recreatedLocalTopicPartitions, recreatedTopicPartitions)
    assertTrue(recreatedLocalTopicPartitions.subsetOf(recreatedTopicPartitions))
    assertFalse(recreatedLocalTopicPartitions.subsetOf(initialLocalTopicPartitions))
    assertEquals(Some(topic), recreateBuilder.topicIdToName(recreatedTopicId))
    assertEquals(Some(recreatedTopicId), recreateBuilder.topicNameToId(topic))
    assertEquals(recreatedTopicPartitions, filterPartitions(recreateBuilder, recreatedTopicPartitions))
    assertEquals(initialLocalTopicPartitions, localRemoved(recreateBuilder))

    val recreatedMetadata = recreateBuilder.build()
    assertEquals(recreatedTopicPartitions, filterPartitions(recreatedMetadata, topic))
    assertEquals(Some(recreatedTopicId), recreatedMetadata.topicNameToId(topic))
    assertEquals(Some(topic), recreatedMetadata.topicIdToName(recreatedTopicId))
  }

  private def localRemoved(
    builder: MetadataPartitionsBuilder
  ): Set[TopicPartition] = {
    builder.localRemoved().toSet[MetadataPartition].map(_.toTopicPartition)
  }

  private def localChanged(
    builder: MetadataPartitionsBuilder
  ): Set[TopicPartition] = {
    builder.localChanged().toSet[MetadataPartition].map(_.toTopicPartition)
  }

  private def filterPartitions(
    metadata: MetadataPartitions,
    topic: String
  ): Set[TopicPartition] = {
    metadata.topicPartitions(topic).map(_.toTopicPartition).toSet
  }

  private def filterPartitions(
    builder: MetadataPartitionsBuilder,
    topicPartitions: Set[TopicPartition]
  ): Set[TopicPartition] = {
    topicPartitions.filter { topicPartition =>
      builder.get(topicPartition.topic, topicPartition.partition).isDefined
    }
  }

  private def createTopic(
    topic: String,
    numPartitions: Int,
    builder: MetadataPartitionsBuilder
  ): Uuid = {
    val topicId = Uuid.randomUuid()
    builder.addUuidMapping(topic, topicId)

    (0 until numPartitions).foreach { partition =>
      builder.set(newPartition(topic, partition))
    }

    topicId
  }

  private def asJavaList(replicas: Iterable[Int]): java.util.List[Integer] = {
    replicas.map(Int.box).toList.asJava
  }

}
