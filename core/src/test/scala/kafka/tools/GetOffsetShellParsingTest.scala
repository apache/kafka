/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.tools

import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Assertions.{assertFalse, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

class GetOffsetShellParsingTest {

  @Test
  def testTopicPartitionFilterForTopicName(): Unit = {
    val topicPartitionFilter = GetOffsetShell.createTopicPartitionFilterWithPatternList("test")

    assertTrue(topicPartitionFilter.isTopicAllowed("test"))
    assertFalse(topicPartitionFilter.isTopicAllowed("test1"))
    assertFalse(topicPartitionFilter.isTopicAllowed("__consumer_offsets"))

    assertTrue(topicPartitionFilter.isTopicPartitionAllowed(topicPartition("test", 0)))
    assertTrue(topicPartitionFilter.isTopicPartitionAllowed(topicPartition("test", 1)))
    assertFalse(topicPartitionFilter.isTopicPartitionAllowed(topicPartition("test1", 0)))
    assertFalse(topicPartitionFilter.isTopicPartitionAllowed(topicPartition("__consumer_offsets", 0)))
  }

  @Test
  def testTopicPartitionFilterForInternalTopicName(): Unit = {
    val topicPartitionFilter = GetOffsetShell.createTopicPartitionFilterWithPatternList("__consumer_offsets")

    assertTrue(topicPartitionFilter.isTopicAllowed("__consumer_offsets"))
    assertFalse(topicPartitionFilter.isTopicAllowed("test1"))
    assertFalse(topicPartitionFilter.isTopicAllowed("test2"))

    assertTrue(topicPartitionFilter.isTopicPartitionAllowed(topicPartition("__consumer_offsets", 0)))
    assertTrue(topicPartitionFilter.isTopicPartitionAllowed(topicPartition("__consumer_offsets", 1)))
    assertFalse(topicPartitionFilter.isTopicPartitionAllowed(topicPartition("test1", 0)))
    assertFalse(topicPartitionFilter.isTopicPartitionAllowed(topicPartition("test2", 0)))
  }

  @Test
  def testTopicPartitionFilterForTopicNameList(): Unit = {
    val topicPartitionFilter = GetOffsetShell.createTopicPartitionFilterWithPatternList("test,test1,__consumer_offsets")

    assertTrue(topicPartitionFilter.isTopicAllowed("test"))
    assertTrue(topicPartitionFilter.isTopicAllowed("test1"))
    assertTrue(topicPartitionFilter.isTopicAllowed("__consumer_offsets"))
    assertFalse(topicPartitionFilter.isTopicAllowed("test2"))

    assertTrue(topicPartitionFilter.isTopicPartitionAllowed(topicPartition("test", 0)))
    assertTrue(topicPartitionFilter.isTopicPartitionAllowed(topicPartition("test1", 1)))
    assertTrue(topicPartitionFilter.isTopicPartitionAllowed(topicPartition("__consumer_offsets", 0)))
    assertFalse(topicPartitionFilter.isTopicPartitionAllowed(topicPartition("test2", 0)))
  }

  @Test
  def testTopicPartitionFilterForRegex(): Unit = {
    val topicPartitionFilter = GetOffsetShell.createTopicPartitionFilterWithPatternList("test.*")

    assertTrue(topicPartitionFilter.isTopicAllowed("test"))
    assertTrue(topicPartitionFilter.isTopicAllowed("test1"))
    assertTrue(topicPartitionFilter.isTopicAllowed("test2"))
    assertFalse(topicPartitionFilter.isTopicAllowed("__consumer_offsets"))

    assertTrue(topicPartitionFilter.isTopicPartitionAllowed(topicPartition("test", 0)))
    assertTrue(topicPartitionFilter.isTopicPartitionAllowed(topicPartition("test1", 1)))
    assertTrue(topicPartitionFilter.isTopicPartitionAllowed(topicPartition("test2", 0)))
    assertFalse(topicPartitionFilter.isTopicPartitionAllowed(topicPartition("__consumer_offsets", 0)))
  }

  @Test
  def testTopicPartitionFilterForPartitionIndexSpec(): Unit = {
    val topicPartitionFilter = GetOffsetShell.createTopicPartitionFilterWithPatternList(":0")

    assertTrue(topicPartitionFilter.isTopicAllowed("test"))
    assertTrue(topicPartitionFilter.isTopicAllowed("test1"))
    assertTrue(topicPartitionFilter.isTopicAllowed("test2"))
    assertTrue(topicPartitionFilter.isTopicAllowed("__consumer_offsets"))

    assertTrue(topicPartitionFilter.isTopicPartitionAllowed(topicPartition("test", 0)))
    assertTrue(topicPartitionFilter.isTopicPartitionAllowed(topicPartition("test1", 0)))
    assertFalse(topicPartitionFilter.isTopicPartitionAllowed(topicPartition("test2", 1)))
    assertTrue(topicPartitionFilter.isTopicPartitionAllowed(topicPartition("__consumer_offsets", 0)))
    assertFalse(topicPartitionFilter.isTopicPartitionAllowed(topicPartition("__consumer_offsets", 1)))
  }

  @Test
  def testTopicPartitionFilterForPartitionRangeSpec(): Unit = {
    val topicPartitionFilter = GetOffsetShell.createTopicPartitionFilterWithPatternList(":1-3")

    assertTrue(topicPartitionFilter.isTopicAllowed("test"))
    assertTrue(topicPartitionFilter.isTopicAllowed("test1"))
    assertTrue(topicPartitionFilter.isTopicAllowed("__consumer_offsets"))
    assertTrue(topicPartitionFilter.isTopicAllowed("test2"))

    assertTrue(topicPartitionFilter.isTopicPartitionAllowed(topicPartition("test", 1)))
    assertTrue(topicPartitionFilter.isTopicPartitionAllowed(topicPartition("test1", 2)))
    assertTrue(topicPartitionFilter.isTopicPartitionAllowed(topicPartition("__consumer_offsets", 2)))
    assertFalse(topicPartitionFilter.isTopicPartitionAllowed(topicPartition("test2", 0)))
    assertFalse(topicPartitionFilter.isTopicPartitionAllowed(topicPartition("test2", 3)))
    assertFalse(topicPartitionFilter.isTopicPartitionAllowed(topicPartition("__consumer_offsets", 3)))
  }

  @Test
  def testTopicPartitionFilterForPartitionLowerBoundSpec(): Unit = {
    val topicPartitionFilter = GetOffsetShell.createTopicPartitionFilterWithPatternList(":1-")

    assertTrue(topicPartitionFilter.isTopicAllowed("test"))
    assertTrue(topicPartitionFilter.isTopicAllowed("test1"))
    assertTrue(topicPartitionFilter.isTopicAllowed("test2"))
    assertTrue(topicPartitionFilter.isTopicAllowed("__consumer_offsets"))

    assertTrue(topicPartitionFilter.isTopicPartitionAllowed(topicPartition("test", 1)))
    assertTrue(topicPartitionFilter.isTopicPartitionAllowed(topicPartition("test1", 2)))
    assertTrue(topicPartitionFilter.isTopicPartitionAllowed(topicPartition("__consumer_offsets", 2)))
    assertFalse(topicPartitionFilter.isTopicPartitionAllowed(topicPartition("test2", 0)))
    assertFalse(topicPartitionFilter.isTopicPartitionAllowed(topicPartition("__consumer_offsets", 0)))
  }

  @Test
  def testTopicPartitionFilterForPartitionUpperBoundSpec(): Unit = {
    val topicPartitionFilter = GetOffsetShell.createTopicPartitionFilterWithPatternList(":-3")
    assertTrue(topicPartitionFilter.isTopicAllowed("test"))
    assertTrue(topicPartitionFilter.isTopicAllowed("test1"))
    assertTrue(topicPartitionFilter.isTopicAllowed("test2"))
    assertTrue(topicPartitionFilter.isTopicAllowed("test3"))
    assertTrue(topicPartitionFilter.isTopicAllowed("__consumer_offsets"))

    assertTrue(topicPartitionFilter.isTopicPartitionAllowed(topicPartition("test", 0)))
    assertTrue(topicPartitionFilter.isTopicPartitionAllowed(topicPartition("test1", 1)))
    assertTrue(topicPartitionFilter.isTopicPartitionAllowed(topicPartition("test2", 2)))
    assertTrue(topicPartitionFilter.isTopicPartitionAllowed(topicPartition("__consumer_offsets", 2)))
    assertFalse(topicPartitionFilter.isTopicPartitionAllowed(topicPartition("test3", 3)))
    assertFalse(topicPartitionFilter.isTopicPartitionAllowed(topicPartition("__consumer_offsets", 3)))
  }

  @Test
  def testTopicPartitionFilterComplex(): Unit = {
    val topicPartitionFilter = GetOffsetShell.createTopicPartitionFilterWithPatternList("test.*:0,__consumer_offsets:1-2,.*:3")

    assertTrue(topicPartitionFilter.isTopicAllowed("test"))
    assertTrue(topicPartitionFilter.isTopicAllowed("test1"))
    assertTrue(topicPartitionFilter.isTopicAllowed("custom"))
    assertTrue(topicPartitionFilter.isTopicAllowed("__consumer_offsets"))

    assertTrue(topicPartitionFilter.isTopicPartitionAllowed(topicPartition("test", 0)))
    assertTrue(topicPartitionFilter.isTopicPartitionAllowed(topicPartition("test1", 0)))
    assertFalse(topicPartitionFilter.isTopicPartitionAllowed(topicPartition("test", 1)))
    assertFalse(topicPartitionFilter.isTopicPartitionAllowed(topicPartition("test1", 1)))

    assertTrue(topicPartitionFilter.isTopicPartitionAllowed(topicPartition("custom", 3)))
    assertFalse(topicPartitionFilter.isTopicPartitionAllowed(topicPartition("custom", 0)))

    assertTrue(topicPartitionFilter.isTopicPartitionAllowed(topicPartition("__consumer_offsets", 1)))
    assertTrue(topicPartitionFilter.isTopicPartitionAllowed(topicPartition("__consumer_offsets", 3)))
    assertFalse(topicPartitionFilter.isTopicPartitionAllowed(topicPartition("__consumer_offsets", 0)))
    assertFalse(topicPartitionFilter.isTopicPartitionAllowed(topicPartition("__consumer_offsets", 2)))
  }

  @Test
  def testPartitionFilterForSingleIndex(): Unit = {
    val topicPartitionFilter = GetOffsetShell.createTopicPartitionFilterWithPatternList(":1")
    assertTrue(topicPartitionFilter.isTopicPartitionAllowed(topicPartition("test", 1)))
    assertFalse(topicPartitionFilter.isTopicPartitionAllowed(topicPartition("test", 0)))
    assertFalse(topicPartitionFilter.isTopicPartitionAllowed(topicPartition("test", 2)))
  }

  @Test
  def testPartitionFilterForRange(): Unit = {
    val topicPartitionFilter = GetOffsetShell.createTopicPartitionFilterWithPatternList(":1-3")
    assertFalse(topicPartitionFilter.isTopicPartitionAllowed(topicPartition("test", 0)))
    assertTrue(topicPartitionFilter.isTopicPartitionAllowed(topicPartition("test", 1)))
    assertTrue(topicPartitionFilter.isTopicPartitionAllowed(topicPartition("test", 2)))
    assertFalse(topicPartitionFilter.isTopicPartitionAllowed(topicPartition("test", 3)))
    assertFalse(topicPartitionFilter.isTopicPartitionAllowed(topicPartition("test", 4)))
    assertFalse(topicPartitionFilter.isTopicPartitionAllowed(topicPartition("test", 5)))
  }

  @Test
  def testPartitionFilterForLowerBound(): Unit = {
    val topicPartitionFilter = GetOffsetShell.createTopicPartitionFilterWithPatternList(":3-")
    assertFalse(topicPartitionFilter.isTopicPartitionAllowed(topicPartition("test", 0)))
    assertFalse(topicPartitionFilter.isTopicPartitionAllowed(topicPartition("test", 1)))
    assertFalse(topicPartitionFilter.isTopicPartitionAllowed(topicPartition("test", 2)))
    assertTrue(topicPartitionFilter.isTopicPartitionAllowed(topicPartition("test", 3)))
    assertTrue(topicPartitionFilter.isTopicPartitionAllowed(topicPartition("test", 4)))
    assertTrue(topicPartitionFilter.isTopicPartitionAllowed(topicPartition("test", 5)))
  }

  @Test
  def testPartitionFilterForUpperBound(): Unit = {
    val topicPartitionFilter = GetOffsetShell.createTopicPartitionFilterWithPatternList(":-3")
    assertTrue(topicPartitionFilter.isTopicPartitionAllowed(topicPartition("test", 0)))
    assertTrue(topicPartitionFilter.isTopicPartitionAllowed(topicPartition("test", 1)))
    assertTrue(topicPartitionFilter.isTopicPartitionAllowed(topicPartition("test", 2)))
    assertFalse(topicPartitionFilter.isTopicPartitionAllowed(topicPartition("test", 3)))
    assertFalse(topicPartitionFilter.isTopicPartitionAllowed(topicPartition("test", 4)))
    assertFalse(topicPartitionFilter.isTopicPartitionAllowed(topicPartition("test", 5)))
  }

  @Test
  def testPartitionsSetFilter(): Unit = {
    val partitionsSetFilter = GetOffsetShell.createTopicPartitionFilterWithTopicAndPartitionPattern(Some("topic"), "1,3,5")

    assertFalse(partitionsSetFilter.isTopicPartitionAllowed(topicPartition("topic", 0)))
    assertFalse(partitionsSetFilter.isTopicPartitionAllowed(topicPartition("topic", 2)))
    assertFalse(partitionsSetFilter.isTopicPartitionAllowed(topicPartition("topic", 4)))

    assertFalse(partitionsSetFilter.isTopicPartitionAllowed(topicPartition("topic1", 1)))
    assertFalse(partitionsSetFilter.isTopicAllowed("topic1"))

    assertTrue(partitionsSetFilter.isTopicPartitionAllowed(topicPartition("topic", 1)))
    assertTrue(partitionsSetFilter.isTopicPartitionAllowed(topicPartition("topic", 3)))
    assertTrue(partitionsSetFilter.isTopicPartitionAllowed(topicPartition("topic", 5)))
    assertTrue(partitionsSetFilter.isTopicAllowed("topic"))
  }

  @Test
  def testPartitionFilterForInvalidSingleIndex(): Unit = {
    assertThrows(classOf[IllegalArgumentException],
      () => GetOffsetShell.createTopicPartitionFilterWithPatternList(":a"))
  }

  @Test
  def testPartitionFilterForInvalidRange(): Unit = {
    assertThrows(classOf[IllegalArgumentException],
      () => GetOffsetShell.createTopicPartitionFilterWithPatternList(":a-b"))
  }

  @Test
  def testPartitionFilterForInvalidLowerBound(): Unit = {
    assertThrows(classOf[IllegalArgumentException],
      () => GetOffsetShell.createTopicPartitionFilterWithPatternList(":a-"))
  }

  @Test
  def testPartitionFilterForInvalidUpperBound(): Unit = {
    assertThrows(classOf[IllegalArgumentException],
      () => GetOffsetShell.createTopicPartitionFilterWithPatternList(":-b"))
  }

  @Test
  def testInvalidTimeValue(): Unit = {
    assertThrows(classOf[IllegalArgumentException],
      () => GetOffsetShell.fetchOffsets(Array("--bootstrap-server", "localhost:9092", "--time", "invalid")))
  }

  private def topicPartition(topic: String, partition: Int): TopicPartition = {
    new TopicPartition(topic, partition)
  }
}
