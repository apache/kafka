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
import org.apache.kafka.common.Uuid
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{Test, Timeout}

import java.util.concurrent.TimeUnit
import scala.collection.mutable
import scala.jdk.CollectionConverters._


@Timeout(value = 120000, unit = TimeUnit.MILLISECONDS)
class MetadataPartitionsTest {

  val emptyPartitions = MetadataPartitions(Collections.emptyMap(), Collections.emptyMap())

  private def newPartition(topicName: String,
                           partitionIndex: Int,
                           replicas: Option[Seq[Int]] = None,
                           isr: Option[Seq[Int]] = None): MetadataPartition = {
    val effectiveReplicas = replicas
      .getOrElse(List(partitionIndex, partitionIndex + 1, partitionIndex + 2))
      .map(Int.box)
      .toList.asJava

    val effectiveIsr = isr match {
      case None => effectiveReplicas
      case Some(s) => s.map(Integer.valueOf).toList.asJava
    }
    new MetadataPartition(topicName,
      partitionIndex,
      partitionIndex % 3, 100,
      effectiveReplicas,
      effectiveIsr,
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
    builder.set(newPartition("foo", 0))
    builder.set(newPartition("foo", 1))
    builder.set(newPartition("foo", 2))
    builder.set(newPartition("bar", 0))
    builder.set(newPartition("baz", 0))
    builder.set(newPartition("baz", 1))
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
}
