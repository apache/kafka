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

import org.apache.kafka.common.PartitionInfo
import org.junit.jupiter.api.Assertions.{assertFalse, assertNotEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

class GetOffsetShellParsingTest {

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testTopicPartitionFilterForTopicName(excludeInternal: Boolean): Unit = {
    val (topicFilter, partitionFilter) = GetOffsetShell.createTopicPartitionFilterWithPatternList("test", excludeInternal)

    assertTrue(topicFilter.apply("test"))
    assertFalse(topicFilter.apply("test1"))
    assertFalse(topicFilter.apply("__consumer_offsets"))

    assertTrue(partitionFilter.apply(partitionInfo("test", 0)))
    assertTrue(partitionFilter.apply(partitionInfo("test", 1)))
    assertFalse(partitionFilter.apply(partitionInfo("test1", 0)))
    assertFalse(partitionFilter.apply(partitionInfo("__consumer_offsets", 0)))
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testTopicPartitionFilterForInternalTopicName(excludeInternal: Boolean): Unit = {
    val (topicFilter, partitionFilter) = GetOffsetShell.createTopicPartitionFilterWithPatternList("__consumer_offsets", excludeInternal)

    assertNotEquals(excludeInternal, topicFilter.apply("__consumer_offsets"))
    assertFalse(topicFilter.apply("test1"))
    assertFalse(topicFilter.apply("test2"))

    assertNotEquals(excludeInternal, partitionFilter.apply(partitionInfo("__consumer_offsets", 0)))
    assertNotEquals(excludeInternal, partitionFilter.apply(partitionInfo("__consumer_offsets", 1)))
    assertFalse(partitionFilter.apply(partitionInfo("test1", 0)))
    assertFalse(partitionFilter.apply(partitionInfo("test2", 0)))
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testTopicPartitionFilterForTopicNameList(excludeInternal: Boolean): Unit = {
    val (topicFilter, partitionFilter) = GetOffsetShell.createTopicPartitionFilterWithPatternList("test,test1,__consumer_offsets", excludeInternal)

    assertTrue(topicFilter.apply("test"))
    assertTrue(topicFilter.apply("test1"))
    assertFalse(topicFilter.apply("test2"))
    assertNotEquals(excludeInternal, topicFilter.apply("__consumer_offsets"))

    assertTrue(partitionFilter.apply(partitionInfo("test", 0)))
    assertTrue(partitionFilter.apply(partitionInfo("test1", 1)))
    assertFalse(partitionFilter.apply(partitionInfo("test2", 0)))
    assertNotEquals(excludeInternal, partitionFilter.apply(partitionInfo("__consumer_offsets", 0)))
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testTopicPartitionFilterForRegex(excludeInternal: Boolean): Unit = {
    val (topicFilter, partitionFilter) = GetOffsetShell.createTopicPartitionFilterWithPatternList("test.*", excludeInternal)

    assertTrue(topicFilter.apply("test"))
    assertTrue(topicFilter.apply("test1"))
    assertTrue(topicFilter.apply("test2"))
    assertFalse(topicFilter.apply("__consumer_offsets"))

    assertTrue(partitionFilter.apply(partitionInfo("test", 0)))
    assertTrue(partitionFilter.apply(partitionInfo("test1", 1)))
    assertTrue(partitionFilter.apply(partitionInfo("test2", 0)))
    assertFalse(partitionFilter.apply(partitionInfo("__consumer_offsets", 0)))
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testTopicPartitionFilterForPartitionIndexSpec(excludeInternal: Boolean): Unit = {
    val (topicFilter, partitionFilter) = GetOffsetShell.createTopicPartitionFilterWithPatternList(":0", excludeInternal)

    assertTrue(topicFilter.apply("test"))
    assertTrue(topicFilter.apply("test1"))
    assertTrue(topicFilter.apply("test2"))
    assertNotEquals(excludeInternal, topicFilter.apply("__consumer_offsets"))

    assertTrue(partitionFilter.apply(partitionInfo("test", 0)))
    assertTrue(partitionFilter.apply(partitionInfo("test1", 0)))
    assertFalse(partitionFilter.apply(partitionInfo("test2", 1)))
    assertNotEquals(excludeInternal, partitionFilter.apply(partitionInfo("__consumer_offsets", 0)))
    assertFalse(partitionFilter.apply(partitionInfo("__consumer_offsets", 1)))
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testTopicPartitionFilterForPartitionRangeSpec(excludeInternal: Boolean): Unit = {
    val (topicFilter, partitionFilter) = GetOffsetShell.createTopicPartitionFilterWithPatternList(":1-3", excludeInternal)

    assertTrue(topicFilter.apply("test"))
    assertTrue(topicFilter.apply("test1"))
    assertTrue(topicFilter.apply("test2"))
    assertNotEquals(excludeInternal, topicFilter.apply("__consumer_offsets"))

    assertTrue(partitionFilter.apply(partitionInfo("test", 1)))
    assertTrue(partitionFilter.apply(partitionInfo("test1", 2)))
    assertFalse(partitionFilter.apply(partitionInfo("test2", 0)))
    assertFalse(partitionFilter.apply(partitionInfo("test2", 3)))
    assertNotEquals(excludeInternal, partitionFilter.apply(partitionInfo("__consumer_offsets", 2)))
    assertFalse(partitionFilter.apply(partitionInfo("__consumer_offsets", 3)))
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testTopicPartitionFilterForPartitionLowerBoundSpec(excludeInternal: Boolean): Unit = {
    val (topicFilter, partitionFilter) = GetOffsetShell.createTopicPartitionFilterWithPatternList(":1-", excludeInternal)

    assertTrue(topicFilter.apply("test"))
    assertTrue(topicFilter.apply("test1"))
    assertTrue(topicFilter.apply("test2"))
    assertNotEquals(excludeInternal, "__consumer_offsets")

    assertTrue(partitionFilter.apply(partitionInfo("test", 1)))
    assertTrue(partitionFilter.apply(partitionInfo("test1", 2)))
    assertFalse(partitionFilter.apply(partitionInfo("test2", 0)))
    assertNotEquals(excludeInternal, partitionFilter.apply(partitionInfo("__consumer_offsets", 2)))
    assertFalse(partitionFilter.apply(partitionInfo("__consumer_offsets", 0)))
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testTopicPartitionFilterForPartitionUpperBoundSpec(excludeInternal: Boolean): Unit = {
    val (topicFilter, partitionFilter) = GetOffsetShell.createTopicPartitionFilterWithPatternList(":-3", excludeInternal)
    assertTrue(topicFilter.apply("test"))
    assertTrue(topicFilter.apply("test1"))
    assertTrue(topicFilter.apply("test2"))
    assertTrue(topicFilter.apply("test3"))
    assertNotEquals(excludeInternal, topicFilter.apply("__consumer_offsets"))

    assertTrue(partitionFilter.apply(partitionInfo("test", 0)))
    assertTrue(partitionFilter.apply(partitionInfo("test1", 1)))
    assertTrue(partitionFilter.apply(partitionInfo("test2", 2)))
    assertFalse(partitionFilter.apply(partitionInfo("test3", 3)))

    assertNotEquals(excludeInternal, partitionFilter.apply(partitionInfo("__consumer_offsets", 2)))
    assertFalse(partitionFilter.apply(partitionInfo("__consumer_offsets", 3)))
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testTopicPartitionFilterComplex(excludeInternal: Boolean): Unit = {
    val (topicFilter, partitionFilter) = GetOffsetShell.createTopicPartitionFilterWithPatternList("test.*:0,__consumer_offsets:1-2,.*:3", excludeInternal)

    assertTrue(topicFilter.apply("test"))
    assertTrue(topicFilter.apply("test1"))
    assertTrue(topicFilter.apply("custom"))
    assertNotEquals(excludeInternal, "__consumer_offsets")

    assertTrue(partitionFilter.apply(partitionInfo("test", 0)))
    assertTrue(partitionFilter.apply(partitionInfo("test", 3)))
    assertFalse(partitionFilter.apply(partitionInfo("test", 1)))

    assertTrue(partitionFilter.apply(partitionInfo("test1", 0)))
    assertTrue(partitionFilter.apply(partitionInfo("test1", 3)))
    assertFalse(partitionFilter.apply(partitionInfo("test1", 1)))

    assertTrue(partitionFilter.apply(partitionInfo("custom", 3)))
    assertFalse(partitionFilter.apply(partitionInfo("custom", 0)))

    assertNotEquals(excludeInternal, partitionFilter.apply(partitionInfo("__consumer_offsets", 1)))
    assertNotEquals(excludeInternal, partitionFilter.apply(partitionInfo("__consumer_offsets", 3)))
    assertFalse(partitionFilter.apply(partitionInfo("__consumer_offsets", 0)))
    assertFalse(partitionFilter.apply(partitionInfo("__consumer_offsets", 2)))
  }

  @Test
  def testPartitionFilterForSingleIndex(): Unit = {
    val (_, partitionFilter) = GetOffsetShell.createTopicPartitionFilterWithPatternList(":1", excludeInternalTopics = false)
    assertTrue(partitionFilter.apply(partitionInfo("test", 1)))
    assertFalse(partitionFilter.apply(partitionInfo("test", 0)))
    assertFalse(partitionFilter.apply(partitionInfo("test", 2)))
  }

  @Test
  def testPartitionFilterForRange(): Unit = {
    val (_, partitionFilter) = GetOffsetShell.createTopicPartitionFilterWithPatternList(":1-3", excludeInternalTopics = false)
    assertFalse(partitionFilter.apply(partitionInfo("test", 0)))
    assertTrue(partitionFilter.apply(partitionInfo("test", 1)))
    assertTrue(partitionFilter.apply(partitionInfo("test", 2)))
    assertFalse(partitionFilter.apply(partitionInfo("test", 3)))
    assertFalse(partitionFilter.apply(partitionInfo("test", 4)))
    assertFalse(partitionFilter.apply(partitionInfo("test", 5)))
  }

  @Test
  def testPartitionFilterForLowerBound(): Unit = {
    val (_, partitionFilter) = GetOffsetShell.createTopicPartitionFilterWithPatternList(":3-", excludeInternalTopics = false)
    assertFalse(partitionFilter.apply(partitionInfo("test", 0)))
    assertFalse(partitionFilter.apply(partitionInfo("test", 1)))
    assertFalse(partitionFilter.apply(partitionInfo("test", 2)))
    assertTrue(partitionFilter.apply(partitionInfo("test", 3)))
    assertTrue(partitionFilter.apply(partitionInfo("test", 4)))
    assertTrue(partitionFilter.apply(partitionInfo("test", 5)))
  }

  @Test
  def testPartitionFilterForUpperBound(): Unit = {
    val (_, partitionFilter) = GetOffsetShell.createTopicPartitionFilterWithPatternList(":-3", excludeInternalTopics = false)
    assertTrue(partitionFilter.apply(partitionInfo("test", 0)))
    assertTrue(partitionFilter.apply(partitionInfo("test", 1)))
    assertTrue(partitionFilter.apply(partitionInfo("test", 2)))
    assertFalse(partitionFilter.apply(partitionInfo("test", 3)))
    assertFalse(partitionFilter.apply(partitionInfo("test", 4)))
    assertFalse(partitionFilter.apply(partitionInfo("test", 5)))
  }

  @Test
  def testPartitionFilterForInvalidSingleIndex(): Unit = {
    assertThrows(classOf[IllegalArgumentException],
      () => GetOffsetShell.createTopicPartitionFilterWithPatternList(":a", excludeInternalTopics = false))
  }

  @Test
  def testPartitionFilterForInvalidRange(): Unit = {
    assertThrows(classOf[IllegalArgumentException],
      () => GetOffsetShell.createTopicPartitionFilterWithPatternList(":a-b", excludeInternalTopics = false))
  }

  @Test
  def testPartitionFilterForInvalidLowerBound(): Unit = {
    assertThrows(classOf[IllegalArgumentException],
      () => GetOffsetShell.createTopicPartitionFilterWithPatternList(":a-", excludeInternalTopics = false))
  }

  @Test
  def testPartitionFilterForInvalidUpperBound(): Unit = {
    assertThrows(classOf[IllegalArgumentException],
      () => GetOffsetShell.createTopicPartitionFilterWithPatternList(":-b", excludeInternalTopics = false))
  }

  private def partitionInfo(topic: String, partition: Int): PartitionInfo = {
    new PartitionInfo(topic, partition, null, null, null)
  }
}
