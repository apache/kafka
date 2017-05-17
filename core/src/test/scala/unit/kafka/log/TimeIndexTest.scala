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

  @Before
  def setup() {
    this.idx = new TimeIndex(file = nonExistantTempFile(), baseOffset = baseOffset, maxIndexSize = maxEntries * 12)
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
      idx.maybeAppend(10000L, 1000L)
    }
    intercept[InvalidOffsetException] {
      idx.maybeAppend(10000L, (maxEntries - 2) * 10, true)
    }
    idx.maybeAppend(10000L, 1000L, true)
  }

  private def appendEntries(numEntries: Int) {
    for (i <- 1 to numEntries)
      idx.maybeAppend(i * 10, i * 10 + baseOffset)
  }

  def nonExistantTempFile(): File = {
    val file = TestUtils.tempFile()
    file.delete()
    file
  }
}

