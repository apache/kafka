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
package unit.kafka.log

import java.io.File

import kafka.log.remote.{RemoteLogIndex, RemoteLogIndexEntry}
import kafka.utils.TestUtils
import org.junit.Assert._
import org.junit.{After, Before, Test}
import org.scalatest.junit.JUnitSuite

import scala.collection.mutable.ListBuffer
import scala.util.Random

class RemoteLogIndexTest extends JUnitSuite {
  var file: File = _
  var index: RemoteLogIndex = _
  val offset = 0L

  @Before
  def setup(): Unit = {
    file = TestUtils.tempFile()
    index = new RemoteLogIndex(offset, file)
  }

  @After
  def teardown(): Unit = {
    index.close()
  }

  @Test
  def testAppendParseEntries() = {
    val entries = appendEntries(1)
    var entry: Option[RemoteLogIndexEntry] = None
    var size = 0
    var position: Long = 0
    do {
      entry = index.entry(position)
      position = index.position()
      println("position: " + index.position() + " :: entry : " + entry)
      size += 1
    } while (entry.isDefined)
    assertEquals(entries.size, size - 1)
  }

  @Test
  def testIndexPosition() = {
    println("position: " + index.position())

    val entries = appendEntries(10)
    println("after adding an entry, position: " + index.position())
    assertEquals(10, entries.size)
    val position = index.position()
    val newEntries = appendEntries(1, 15000L)
    index.flush()
    println("after adding one more entry, position: " + index.position())

    val entry = index.entry(position)

    assertEquals(newEntries.last, entry.get)
  }

  private def appendEntries(numEntries: Int, baseOffset: Long = 1000): Seq[RemoteLogIndexEntry] = {
    val entries = new ListBuffer[RemoteLogIndexEntry]
    val baseTimestamp = System.currentTimeMillis()
    val rdi = "rdi://foo/bar"
    val rdiBytes = rdi.getBytes
    for (i <- 1 to numEntries) {
      val firstOffset = baseOffset + (i * 100)
      val lastOffset = firstOffset + 99
      val firstTimestamp = baseTimestamp + (100 * i)
      val lastTimestamp = firstTimestamp + 10
      val dataLength = Math.abs(new Random().nextInt())
      val entry: RemoteLogIndexEntry = RemoteLogIndexEntry(firstOffset, lastOffset, firstTimestamp, lastTimestamp, dataLength, rdiBytes)
      index.append(entry)
      entries += entry
    }

    entries.toList
  }

}
