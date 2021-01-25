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
import org.junit.Assert.{assertFalse, assertTrue, assertEquals}
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.runners.Parameterized.Parameters

import java.util

@RunWith(classOf[Parameterized])
class GetOffsetShellParsingTest(excludeInternal: Boolean) {
  @Test
  def testTopicPartitionFilterForTopicName(): Unit = {
    val filter = GetOffsetShell.createTopicPartitionFilterWithPatternList("test", excludeInternal)
    assertTrue(filter.apply(new PartitionInfo("test", 0, null, null, null)))
    assertTrue(filter.apply(new PartitionInfo("test", 1, null, null, null)))
    assertFalse(filter.apply(new PartitionInfo("test1", 0, null, null, null)))
    assertFalse(filter.apply(new PartitionInfo("__consumer_offsets", 0, null, null, null)))
  }

  @Test
  def testTopicPartitionFilterForInternalTopicName(): Unit = {
    val filter = GetOffsetShell.createTopicPartitionFilterWithPatternList("__consumer_offsets", excludeInternal)
    assertEquals(!excludeInternal, filter.apply(new PartitionInfo("__consumer_offsets", 0, null, null, null)))
    assertEquals(!excludeInternal, filter.apply(new PartitionInfo("__consumer_offsets", 1, null, null, null)))
    assertFalse(filter.apply(new PartitionInfo("test1", 0, null, null, null)))
    assertFalse(filter.apply(new PartitionInfo("test2", 0, null, null, null)))
  }

  @Test
  def testTopicPartitionFilterForTopicNameList(): Unit = {
    val filter = GetOffsetShell.createTopicPartitionFilterWithPatternList("test,test1,__consumer_offsets", excludeInternal)
    assertTrue(filter.apply(new PartitionInfo("test", 0, null, null, null)))
    assertTrue(filter.apply(new PartitionInfo("test1", 1, null, null, null)))
    assertFalse(filter.apply(new PartitionInfo("test2", 0, null, null, null)))

    assertEquals(!excludeInternal, filter.apply(new PartitionInfo("__consumer_offsets", 0, null, null, null)))
  }

  @Test
  def testTopicPartitionFilterForRegex(): Unit = {
    val filter = GetOffsetShell.createTopicPartitionFilterWithPatternList("test.*", excludeInternal)
    assertTrue(filter.apply(new PartitionInfo("test", 0, null, null, null)))
    assertTrue(filter.apply(new PartitionInfo("test1", 1, null, null, null)))
    assertTrue(filter.apply(new PartitionInfo("test2", 0, null, null, null)))
    assertFalse(filter.apply(new PartitionInfo("__consumer_offsets", 0, null, null, null)))
  }

  @Test
  def testTopicPartitionFilterForPartitionIndexSpec(): Unit = {
    val filter = GetOffsetShell.createTopicPartitionFilterWithPatternList(":0", excludeInternal)
    assertTrue(filter.apply(new PartitionInfo("test", 0, null, null, null)))
    assertTrue(filter.apply(new PartitionInfo("test1", 0, null, null, null)))
    assertFalse(filter.apply(new PartitionInfo("test2", 1, null, null, null)))

    assertEquals(!excludeInternal, filter.apply(new PartitionInfo("__consumer_offsets", 0, null, null, null)))
    assertFalse(filter.apply(new PartitionInfo("__consumer_offsets", 1, null, null, null)))
  }

  @Test
  def testTopicPartitionFilterForPartitionRangeSpec(): Unit = {
    val filter = GetOffsetShell.createTopicPartitionFilterWithPatternList(":1-3", excludeInternal)
    assertTrue(filter.apply(new PartitionInfo("test", 1, null, null, null)))
    assertTrue(filter.apply(new PartitionInfo("test1", 2, null, null, null)))
    assertFalse(filter.apply(new PartitionInfo("test2", 0, null, null, null)))
    assertFalse(filter.apply(new PartitionInfo("test2", 3, null, null, null)))

    assertEquals(!excludeInternal, filter.apply(new PartitionInfo("__consumer_offsets", 2, null, null, null)))
    assertFalse(filter.apply(new PartitionInfo("__consumer_offsets", 3, null, null, null)))
  }

  @Test
  def testTopicPartitionFilterForPartitionLowerBoundSpec(): Unit = {
    val filter = GetOffsetShell.createTopicPartitionFilterWithPatternList(":1-", excludeInternal)
    assertTrue(filter.apply(new PartitionInfo("test", 1, null, null, null)))
    assertTrue(filter.apply(new PartitionInfo("test1", 2, null, null, null)))
    assertFalse(filter.apply(new PartitionInfo("test2", 0, null, null, null)))

    assertEquals(!excludeInternal, filter.apply(new PartitionInfo("__consumer_offsets", 2, null, null, null)))
    assertFalse(filter.apply(new PartitionInfo("__consumer_offsets", 0, null, null, null)))
  }

  @Test
  def testTopicPartitionFilterForPartitionUpperBoundSpec(): Unit = {
    val filter = GetOffsetShell.createTopicPartitionFilterWithPatternList(":-3", excludeInternal)
    assertTrue(filter.apply(new PartitionInfo("test", 0, null, null, null)))
    assertTrue(filter.apply(new PartitionInfo("test1", 1, null, null, null)))
    assertTrue(filter.apply(new PartitionInfo("test2", 2, null, null, null)))
    assertFalse(filter.apply(new PartitionInfo("test3", 3, null, null, null)))

    assertEquals(!excludeInternal, filter.apply(new PartitionInfo("__consumer_offsets", 2, null, null, null)))
    assertFalse(filter.apply(new PartitionInfo("__consumer_offsets", 3, null, null, null)))
  }

  @Test
  def testTopicPartitionFilterComplex(): Unit = {
    val filter = GetOffsetShell.createTopicPartitionFilterWithPatternList("test.*:0,__consumer_offsets:1-2,.*:3", excludeInternal)
    assertTrue(filter.apply(new PartitionInfo("test", 0, null, null, null)))
    assertTrue(filter.apply(new PartitionInfo("test", 3, null, null, null)))
    assertFalse(filter.apply(new PartitionInfo("test", 1, null, null, null)))

    assertTrue(filter.apply(new PartitionInfo("test1", 0, null, null, null)))
    assertTrue(filter.apply(new PartitionInfo("test1", 3, null, null, null)))
    assertFalse(filter.apply(new PartitionInfo("test1", 1, null, null, null)))

    assertTrue(filter.apply(new PartitionInfo("custom", 3, null, null, null)))
    assertFalse(filter.apply(new PartitionInfo("custom", 0, null, null, null)))

    assertEquals(!excludeInternal, filter.apply(new PartitionInfo("__consumer_offsets", 1, null, null, null)))
    assertEquals(!excludeInternal, filter.apply(new PartitionInfo("__consumer_offsets", 3, null, null, null)))
    assertFalse(filter.apply(new PartitionInfo("__consumer_offsets", 0, null, null, null)))
    assertFalse(filter.apply(new PartitionInfo("__consumer_offsets", 2, null, null, null)))
  }

  @Test
  def testPartitionFilterForSingleIndex(): Unit = {
    val filter = GetOffsetShell.createPartitionFilter("1")
    assertTrue(filter.apply(1))
    assertFalse(filter.apply(0))
    assertFalse(filter.apply(2))
  }

  @Test
  def testPartitionFilterForRange(): Unit = {
    val filter = GetOffsetShell.createPartitionFilter("1-3")
    assertFalse(filter.apply(0))
    assertTrue(filter.apply(1))
    assertTrue(filter.apply(2))
    assertFalse(filter.apply(3))
    assertFalse(filter.apply(4))
    assertFalse(filter.apply(5))
  }

  @Test
  def testPartitionFilterForLowerBound(): Unit = {
    val filter = GetOffsetShell.createPartitionFilter("3-")
    assertFalse(filter.apply(0))
    assertFalse(filter.apply(1))
    assertFalse(filter.apply(2))
    assertTrue(filter.apply(3))
    assertTrue(filter.apply(4))
    assertTrue(filter.apply(5))
  }

  @Test
  def testPartitionFilterForUpperBound(): Unit = {
    val filter = GetOffsetShell.createPartitionFilter("-3")
    assertTrue(filter.apply(0))
    assertTrue(filter.apply(1))
    assertTrue(filter.apply(2))
    assertFalse(filter.apply(3))
    assertFalse(filter.apply(4))
    assertFalse(filter.apply(5))
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testPartitionFilterForInvalidSingleIndex(): Unit = {
    GetOffsetShell.createPartitionFilter("a")
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testPartitionFilterForInvalidRange(): Unit = {
    GetOffsetShell.createPartitionFilter("a-b")
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testPartitionFilterForInvalidLowerBound(): Unit = {
    GetOffsetShell.createPartitionFilter("a-")
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testPartitionFilterForInvalidUpperBound(): Unit = {
    GetOffsetShell.createPartitionFilter("-b")
  }
}

object GetOffsetShellParsingTest {
  @Parameters
  def parameters: util.Collection[Boolean] = {
    util.Arrays.asList(true, false)
  }
}
