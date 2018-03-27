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

import kafka.common.InvalidOffsetException
import kafka.utils.TestUtils
import org.junit.{Test, After, Before}
import org.junit.Assert.{assertEquals}
import org.scalatest.junit.JUnitSuite

/**
 * Unit test for time index.
 */
class TimeIndexTest extends JUnitSuite {
  var idx: TimeIndex = null
  val maxEntries = 30
  val baseOffset = 45L
  val indexFile = nonExistantTempFile()
  val maxIndexSize = maxEntries * 12

  def openTimeIndex: TimeIndex = {
    new TimeIndex(indexFile, baseOffset = baseOffset, maxIndexSize = maxIndexSize)
  }

  @Before
  def setup() {
    this.idx = openTimeIndex
  }

  @After
  def teardown() {
    if(this.idx != null)
      this.idx.file.delete()
  }

  @Test
  def testLookUp() {
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
  def testTruncate() {
    appendEntries(maxEntries - 1)
    idx.truncate()
    assertEquals(0, idx.entries)

    appendEntries(maxEntries - 1)
    idx.truncateTo(10 + baseOffset)
    assertEquals(0, idx.entries)
  }

  @Test
  def testAppend() {
    appendEntries(maxEntries - 1)
    intercept[IllegalArgumentException] {
      idx.maybeAppend(10000L, 1000L, skipIndexFullCheck =  false)
    }
    intercept[InvalidOffsetException] {
      idx.maybeAppend(10000L, (maxEntries - 2) * 10, skipIndexFullCheck = true)
    }
    idx.maybeAppend(10000L, 1000L, skipIndexFullCheck = true)
  }

  private def appendEntries(numEntries: Int) {
    for (i <- 1 to numEntries)
      idx.maybeAppend(i * 10, i * 10 + baseOffset, skipIndexFullCheck = false)
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
    intercept[CorruptIndexException](idx.sanityCheck())
    shouldCorruptOffset = false

    shouldCorruptTimestamp = true
    intercept[CorruptIndexException](idx.sanityCheck())
    shouldCorruptTimestamp = false

    shouldCorruptLength = true
    intercept[CorruptIndexException](idx.sanityCheck())
    shouldCorruptLength = false

    idx.sanityCheck()
    idx.close()
  }

  @Test
  def testOffsetOverflow(): Unit = {
    val first = TimestampOffset(0, baseOffset + 0)
    val second = TimestampOffset(10, baseOffset + 1)
    val third = TimestampOffset(23, baseOffset + 2)
    val fourth = TimestampOffset(37, baseOffset + 3)
    val fifth = TimestampOffset(40, baseOffset + Integer.MAX_VALUE)
    val sixth = TimestampOffset(41, baseOffset + Integer.MAX_VALUE + 1L)

    for (offsetPosition <- Seq(first, second, third, fourth, fifth))
      idx.maybeAppend(offsetPosition.timestamp, offsetPosition.offset, skipIndexFullCheck = false)
    assert(5 == idx.entries)

    // try to insert an index entry that would cause offset overflow
    intercept[InvalidOffsetException] { idx.maybeAppend(sixth.timestamp, sixth.offset, skipIndexFullCheck = false) }
    assert(5 == idx.entries)

    // call the internal API for ignoring offset overflow which should silently ignore the overflow, and skip appending to index
    idx.maybeAppend(sixth.timestamp, sixth.offset, skipIndexFullCheck = false, mayHaveIndexOverflow = true)
    assert(5 == idx.entries)
  }

  @Test
  def testOffsetOverflowCorruptionSanity(): Unit = {
    val first = TimestampOffset(0, baseOffset + 0)
    val second = TimestampOffset(10, baseOffset + 1)
    val third = TimestampOffset(23, baseOffset + 2)
    val fourth = TimestampOffset(37, baseOffset + 3)
    val fifth = TimestampOffset(40, baseOffset + Integer.MAX_VALUE)
    val sixth = TimestampOffset(41, baseOffset + Integer.MAX_VALUE + 1L)

    for (offsetPosition <- Seq(first, second, third, fourth, fifth))
      idx.maybeAppend(offsetPosition.timestamp, offsetPosition.offset, skipIndexFullCheck = false)
    assert(5 == idx.entries)

    // forcefully append an index overflow to simulate pre KAFKA-5413 state
    idx.mmap.putLong(sixth.timestamp)
    idx.mmap.putInt((sixth.offset - baseOffset).toInt)
    idx._entries += 1

    // close the index and reload so that state is setup correctly
    idx.close()
    idx = openTimeIndex
    assert(6 == idx.entries)

    // sanity check should now throw an exception because of the index overflow
    intercept[CorruptIndexException] { idx.sanityCheck() }
  }
}

