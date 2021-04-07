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
package kafka.log

import java.io.File

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Time
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

class LogSegmentsTest {

  val topicPartition = new TopicPartition("topic", 0)
  var logDir: File = _

  /* create a segment with the given base offset */
  private def createSegment(offset: Long,
                    indexIntervalBytes: Int = 10,
                    time: Time = Time.SYSTEM): LogSegment = {
    LogTestUtils.createSegment(offset, logDir, indexIntervalBytes, time)
  }

  private def assertEntry(segment: LogSegment, tested: java.util.Map.Entry[java.lang.Long, LogSegment]): Unit = {
    assertEquals(segment.baseOffset, tested.getKey())
    assertEquals(segment, tested.getValue())
  }

  @Test
  def testBasicOperations(): Unit = {
    val segments = new LogSegments(topicPartition)
    assertTrue(segments.isEmpty)
    assertFalse(segments.nonEmpty)

    val offset1 = 40
    val seg1 = createSegment(offset1)
    val offset2 = 80
    val seg2 = createSegment(offset2)
    val seg3 = createSegment(offset1)

    // Add seg1
    segments.add(seg1)
    assertFalse(segments.isEmpty)
    assertTrue(segments.nonEmpty)
    assertEquals(1, segments.numberOfSegments)
    assertTrue(segments.contains(offset1))
    assertEquals(Some(seg1), segments.get(offset1))

    // Add seg2
    segments.add(seg2)
    assertFalse(segments.isEmpty)
    assertTrue(segments.nonEmpty)
    assertEquals(2, segments.numberOfSegments)
    assertTrue(segments.contains(offset2))
    assertEquals(Some(seg2), segments.get(offset2))

    // Replace seg1 with seg3
    segments.add(seg3)
    assertFalse(segments.isEmpty)
    assertTrue(segments.nonEmpty)
    assertEquals(2, segments.numberOfSegments)
    assertTrue(segments.contains(offset1))
    assertEquals(Some(seg3), segments.get(offset1))

    // Remove seg2
    segments.remove(offset2)
    assertFalse(segments.isEmpty)
    assertTrue(segments.nonEmpty)
    assertEquals(1, segments.numberOfSegments)
    assertFalse(segments.contains(offset2))

    // Clear all segments including seg3
    segments.clear()
    assertTrue(segments.isEmpty)
    assertFalse(segments.nonEmpty)
    assertEquals(0, segments.numberOfSegments)
    assertFalse(segments.contains(offset1))
  }

  @Test
  def testSegmentAccess(): Unit = {
    val segments = new LogSegments(topicPartition)
    val offset1 = 1
    val seg1 = createSegment(offset1)
    val offset2 = 2
    val seg2 = createSegment(offset2)
    val offset3 = 3
    val seg3 = createSegment(offset3)
    val offset4 = 4
    val seg4 = createSegment(offset4)

    // Test firstEntry, lastEntry
    List(seg1, seg2, seg3, seg4).foreach {
      seg =>
        segments.add(seg)
        assertEntry(seg1, segments.firstEntry.get)
        assertEquals(Some(seg1), segments.firstSegment)
        assertEntry(seg, segments.lastEntry.get)
        assertEquals(Some(seg), segments.lastSegment)
    }

    // Test baseOffsets
    assertEquals(Seq(offset1, offset2, offset3, offset4), segments.baseOffsets)

    // Test values
    assertEquals(Seq(seg1, seg2, seg3, seg4), segments.values.toSeq)

    // Test values(to, from)
    assertThrows(classOf[IllegalArgumentException], () => segments.values(2, 1))
    assertEquals(Seq(), segments.values(1, 1).toSeq)
    assertEquals(Seq(seg1), segments.values(1, 2).toSeq)
    assertEquals(Seq(seg1, seg2), segments.values(1, 3).toSeq)
    assertEquals(Seq(seg1, seg2, seg3), segments.values(1, 4).toSeq)
    assertEquals(Seq(seg2, seg3), segments.values(2, 4).toSeq)
    assertEquals(Seq(seg3), segments.values(3, 4).toSeq)
    assertEquals(Seq(), segments.values(4, 4).toSeq)
    assertEquals(Seq(seg4), segments.values(4, 5).toSeq)

  }

  @Test
  def testClosestMatchOperations(): Unit = {
    val segments = new LogSegments(topicPartition)
    val seg1 = createSegment(1)
    val seg2 = createSegment(3)
    val seg3 = createSegment(5)
    val seg4 = createSegment(7)

    List(seg1, seg2, seg3, seg4).foreach(segments.add)

    // Test floorSegment, floorEntry
    assertEquals(Some(seg1), segments.floorSegment(2))
    assertEntry(seg1, segments.floorEntry(2).get)
    assertEquals(Some(seg2), segments.floorSegment(3))
    assertEntry(seg2, segments.floorEntry(3).get)

    // Test lowerSegment, lowerEntry
    assertEquals(Some(seg1), segments.lowerSegment(3))
    assertEntry(seg1, segments.lowerEntry(3).get)
    assertEquals(Some(seg2), segments.lowerSegment(4))
    assertEntry(seg2, segments.lowerEntry(4).get)

    // Test higherSegment, higherEntry
    assertEquals(Some(seg3), segments.higherSegment(4))
    assertEntry(seg3, segments.higherEntry(4).get)
    assertEquals(Some(seg4), segments.higherSegment(5))
    assertEntry(seg4, segments.higherEntry(5).get)
  }
}
