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
package kafka.log.remote

import kafka.log.remote.RemoteLogIndexTest.generateEntries
import kafka.utils.TestUtils
import org.junit.Assert._
import org.junit.{After, Before, Test}
import org.scalatest.junit.JUnitSuite

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class RemoteLogIndexTest extends JUnitSuite {
  var index: RemoteLogIndex = _
  val startOffset = 19

  @Before
  def setup(): Unit = {
    val file = TestUtils.tempFile()
    index = new RemoteLogIndex(file, startOffset)
  }

  @After
  def teardown(): Unit = {
    index.close()
  }

  @Test
  def testIndexPosition(): Unit = {
    val entriesCt = 10
    val entries = generateEntries(entriesCt, baseOffset = startOffset)
    val positions = index.append(entries)

    def assertLookupEntries(lookupIndex:RemoteLogIndex): Unit = {
      var i = 0
      while (i < entries.size) {
        assertEquals(entries(i), lookupIndex.lookupEntry(positions(i)).get)
        i += 1
      }
    }

    // check looking up entries at respective positions
    assertLookupEntries(index)

    // reopen the index and check for entries
    val reopenedIndex = new RemoteLogIndex(index.file, startOffset)
    // this should throw IllegalArgumentException as  offset with earlier startOffset is already pushed
    // index allows only offsets which are more than earlier.
    assertThrows[IllegalArgumentException] {
      reopenedIndex.append(generateEntries(entriesCt, baseOffset = startOffset))
    }
    // check looking up entries at respective positions
    assertLookupEntries(reopenedIndex)
  }

}

object RemoteLogIndexTest {

  def generateEntries(numEntries: Int, offsetStep: Integer = 100, baseOffset: Long = 1000): Seq[RemoteLogIndexEntry] = {
    require(offsetStep >= 1, "offsetStep should be >= 1")
    require(baseOffset >= 0, " base offset can not be negative")

    val entries = new ArrayBuffer[RemoteLogIndexEntry](numEntries)
    val rdi = "rdi://foo/bar/" + System.currentTimeMillis()
    val rdiBytes = rdi.getBytes
    var firstOffset = baseOffset
    var firstTimestamp: Long = baseOffset * 1000L
    for (i <- 1 to numEntries) {
      val lastOffset = firstOffset + offsetStep - 1
      val lastTimestamp = lastOffset * 1000L
      val dataLength = Math.abs(new Random().nextInt())
      val entry: RemoteLogIndexEntry = RemoteLogIndexEntry(firstOffset, lastOffset, firstTimestamp, lastTimestamp,
        dataLength, rdiBytes)
      firstOffset += offsetStep
      firstTimestamp = firstOffset * 1000L

      entries += entry
    }

    entries.toSeq
  }

}
