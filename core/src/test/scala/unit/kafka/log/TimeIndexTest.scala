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

import kafka.utils.TestUtils
import org.apache.kafka.common.errors.InvalidOffsetException
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows}

/**
 * Unit test for time index.
 */
class TimeIndexTest {
  var idx: TimeIndex = null
  val maxEntries = 30
  val baseOffset = 45L

  @BeforeEach
  def setup(): Unit = {
    this.idx = new TimeIndex(nonExistantTempFile(), baseOffset = baseOffset, maxIndexSize = maxEntries * 12)
  }

  @AfterEach
  def teardown(): Unit = {
    if(this.idx != null)
      this.idx.file.delete()
  }

  @Test
  def testLookUp(): Unit = {
    // Empty time index
    assertEquals(TimestampOffset(-1L, baseOffset), idx.lookup(100L))

    // Add several time index entries.
    appendEntries(maxEntries - 1)

    // look for timestamp smaller than the earliest entry
    assertEquals(TimestampOffset(-1L, baseOffset), idx.lookup(9))
    // look for timestamp in the middle of two entries.
    assertEquals(TimestampOffset(20L, 65L), idx.lookup(25))
    // look for timestamp same as the one in the entry
    assertEquals(TimestampOffset(30L, 75L), idx.lookup(30))
  }

  @Test
  def testEntry(): Unit = {
    appendEntries(maxEntries - 1)
    assertEquals(TimestampOffset(10L, 55L), idx.entry(0))
    assertEquals(TimestampOffset(20L, 65L), idx.entry(1))
    assertEquals(TimestampOffset(30L, 75L), idx.entry(2))
    assertEquals(TimestampOffset(40L, 85L), idx.entry(3))
  }

  @Test
  def testEntryOverflow(): Unit = {
    assertThrows(classOf[IllegalArgumentException], () => idx.entry(0))
  }

  @Test
  def testTruncate(): Unit = {
    appendEntries(maxEntries - 1)
    idx.truncate()
    assertEquals(0, idx.entries)

    appendEntries(maxEntries - 1)
    idx.truncateTo(10 + baseOffset)
    assertEquals(0, idx.entries)
  }

  @Test
  def testAppend(): Unit = {
    appendEntries(maxEntries - 1)
    assertThrows(classOf[IllegalArgumentException], () => idx.maybeAppend(10000L, 1000L))
    assertThrows(classOf[InvalidOffsetException], () => idx.maybeAppend(10000L, (maxEntries - 2) * 10, true))
    idx.maybeAppend(10000L, 1000L, true)
  }

  private def appendEntries(numEntries: Int): Unit = {
    for (i <- 1 to numEntries)
      idx.maybeAppend(i * 10, i * 10 + baseOffset)
  }

  def nonExistantTempFile(): File = {
    val file = TestUtils.tempFile()
    file.delete()
    file
  }

  @Test
  def testSanityCheck(): Unit = {
    idx.sanityCheck()
    appendEntries(5)
    val firstEntry = idx.entry(0)
    idx.sanityCheck()
    idx.close()

    var shouldCorruptOffset = false
    var shouldCorruptTimestamp = false
    var shouldCorruptLength = false
    idx = new TimeIndex(idx.file, baseOffset = baseOffset, maxIndexSize = maxEntries * 12) {
      override def lastEntry = {
        val superLastEntry = super.lastEntry
        val offset = if (shouldCorruptOffset) baseOffset - 1 else superLastEntry.offset
        val timestamp = if (shouldCorruptTimestamp) firstEntry.timestamp - 1 else superLastEntry.timestamp
        new TimestampOffset(timestamp, offset)
      }
      override def length = {
        val superLength = super.length
        if (shouldCorruptLength) superLength - 1 else superLength
      }
    }

    shouldCorruptOffset = true
    assertThrows(classOf[CorruptIndexException], () => idx.sanityCheck())
    shouldCorruptOffset = false

    shouldCorruptTimestamp = true
    assertThrows(classOf[CorruptIndexException], () => idx.sanityCheck())
    shouldCorruptTimestamp = false

    shouldCorruptLength = true
    assertThrows(classOf[CorruptIndexException], () => idx.sanityCheck())
    shouldCorruptLength = false

    idx.sanityCheck()
    idx.close()
  }

}

