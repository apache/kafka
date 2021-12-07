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

package kafka.tools

import kafka.utils.IncludeList
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Assertions.{assertFalse, assertTrue}
import org.junit.jupiter.api.Test

class TopicPartitionFilterTest {

  @Test
  def testPartitionsSetFilter(): Unit = {
    val partitionsSetFilter = PartitionsSetFilter(Set(1, 3, 5))

    assertFalse(partitionsSetFilter.isPartitionAllowed(0))
    assertFalse(partitionsSetFilter.isPartitionAllowed(2))
    assertFalse(partitionsSetFilter.isPartitionAllowed(4))

    assertTrue(partitionsSetFilter.isPartitionAllowed(1))
    assertTrue(partitionsSetFilter.isPartitionAllowed(3))
    assertTrue(partitionsSetFilter.isPartitionAllowed(5))
  }

  @Test
  def testUniquePartitionFilter(): Unit = {
    val uniquePartitionFilter = UniquePartitionFilter(1)

    assertFalse(uniquePartitionFilter.isPartitionAllowed(0))
    assertFalse(uniquePartitionFilter.isPartitionAllowed(2))

    assertTrue(uniquePartitionFilter.isPartitionAllowed(1))
  }

  @Test
  def testPartitionRangeFilter(): Unit = {
    val partitionRangeFilter = PartitionRangeFilter(1, 3)

    assertFalse(partitionRangeFilter.isPartitionAllowed(0))
    assertFalse(partitionRangeFilter.isPartitionAllowed(3))

    assertTrue(partitionRangeFilter.isPartitionAllowed(1))
    assertTrue(partitionRangeFilter.isPartitionAllowed(2))
  }

  @Test
  def testTopicFilterAndPartitionFilter(): Unit = {
    val partitionRangeFilter = PartitionRangeFilter(1, 3)
    val topicFilter = IncludeList("topic")
    val topicPartitionFilter = TopicFilterAndPartitionFilter(topicFilter, partitionRangeFilter)

    assertTrue(topicPartitionFilter.isPartitionAllowed(topicPartition("topic", 1)))
    assertTrue(topicPartitionFilter.isPartitionAllowed(topicPartition("topic", 2)))

    assertFalse(topicPartitionFilter.isPartitionAllowed(topicPartition("topic", 0)))
    assertFalse(topicPartitionFilter.isPartitionAllowed(topicPartition("topic", 3)))
    assertFalse(topicPartitionFilter.isPartitionAllowed(topicPartition("topic1", 2)))
  }

  @Test
  def testCompositeTopicPartitionFilter(): Unit = {
    val filter0 = TopicFilterAndPartitionFilter(IncludeList("topic0"), PartitionRangeFilter(0, 1))
    val filter1 = TopicFilterAndPartitionFilter(IncludeList("topic1"), PartitionRangeFilter(1, 2))
    val filter2 = TopicFilterAndPartitionFilter(IncludeList("topic2"), PartitionRangeFilter(2, 3))

    val compositeTopicPartitionFilter = CompositeTopicPartitionFilter(Array(filter0, filter1, filter2))

    assertTrue(compositeTopicPartitionFilter.isPartitionAllowed(topicPartition("topic0", 0)))
    assertTrue(compositeTopicPartitionFilter.isPartitionAllowed(topicPartition("topic1", 1)))
    assertTrue(compositeTopicPartitionFilter.isPartitionAllowed(topicPartition("topic2", 2)))

    assertFalse(compositeTopicPartitionFilter.isPartitionAllowed(topicPartition("topic0", 1)))
    assertFalse(compositeTopicPartitionFilter.isPartitionAllowed(topicPartition("topic1", 2)))
    assertFalse(compositeTopicPartitionFilter.isPartitionAllowed(topicPartition("topic2", 3)))
  }

  private def topicPartition(topic: String, partition: Int): TopicPartition = {
    new TopicPartition(topic, partition)
  }
}
