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

import kafka.log.remote.{RemoteLogIndex, RemoteLogIndexEntry}
import kafka.utils.TestUtils
import org.junit.Assert._
import org.junit.{After, Before, Test}
import org.scalatest.junit.JUnitSuite
import unit.kafka.log.RemoteLogIndexTest.generateEntries

import scala.collection.mutable.ListBuffer
import scala.util.Random

class RemoteLogIndexTest extends JUnitSuite {
  var index: RemoteLogIndex = _

  @Before
  def setup(): Unit = {
    val file = TestUtils.tempFile()
    index = new RemoteLogIndex(file, 0)
  }

  @After
  def teardown(): Unit = {
    index.close()
  }

  @Test
  def testIndexPosition() = {
    println("position: " + index.nextEntryPosition())
    val entriesCt = 10
    val entries = generateEntries(entriesCt)
    val positions = index.append(entries)
    var i: Integer = 0
    while (i < entries.size) {
      println("######### " + i)
      assertEquals(entries(i), index.lookupEntry(positions(i)).get)
      i += 1
    }
    println("after adding few entries, position: " + index.nextEntryPosition())
  }


}

object RemoteLogIndexTest {

  def generateEntries(numEntries: Int, offsetStep: Integer = 100, baseOffset: Long = 1000): Seq[RemoteLogIndexEntry] = {
    require(offsetStep > 1, "offsetStep should be greater than 1")
    require(baseOffset >= 0, " base offset can not be negative")

    val entries = new ListBuffer[RemoteLogIndexEntry]
    val rdi = "rdi://foo/bar/" + System.currentTimeMillis()
    val rdiBytes = rdi.getBytes
    var firstOffset = baseOffset
    var firstTimestamp: Long = baseOffset + 10L
    for (i <- 1 to numEntries) {
      val lastOffset = firstOffset + 2
      val lastTimestamp = firstTimestamp + 1
      val dataLength = Math.abs(new Random().nextInt())
      val entry: RemoteLogIndexEntry = RemoteLogIndexEntry(firstOffset, lastOffset, firstTimestamp, lastTimestamp, dataLength, rdiBytes)
      firstOffset += offsetStep
      firstTimestamp += 1

      entries += entry
    }

    entries.toList
  }

}
