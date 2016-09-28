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

package kafka.message

import java.nio.ByteBuffer
import java.nio.channels.{FileChannel, GatheringByteChannel}
import java.nio.file.StandardOpenOption

import org.junit.Assert._
import kafka.utils.TestUtils._
import kafka.log.FileMessageSet
import kafka.utils.TestUtils
import org.scalatest.junit.JUnitSuite
import org.junit.Test

import scala.collection.mutable.ArrayBuffer

trait BaseMessageSetTestCases extends JUnitSuite {

  private class StubByteChannel(bytesToConsumePerBuffer: Int) extends GatheringByteChannel {

    val data = new ArrayBuffer[Byte]

    def write(srcs: Array[ByteBuffer], offset: Int, length: Int): Long = {
      srcs.map { src =>
        val array = new Array[Byte](math.min(bytesToConsumePerBuffer, src.remaining))
        src.get(array)
        data ++= array
        array.length
      }.sum
    }

    def write(srcs: Array[ByteBuffer]): Long = write(srcs, 0, srcs.map(_.remaining).sum)

    def write(src: ByteBuffer): Int = write(Array(src)).toInt

    def isOpen: Boolean = true

    def close() {}

  }


  val messages = Array(new Message("abcd".getBytes), new Message("efgh".getBytes), new Message("ijkl".getBytes))
  
  def createMessageSet(messages: Seq[Message]): MessageSet

  @Test
  def testWrittenEqualsRead() {
    val messageSet = createMessageSet(messages)
    checkEquals(messages.iterator, messageSet.map(m => m.message).iterator)
  }

  @Test
  def testIteratorIsConsistent() {
    val m = createMessageSet(messages)
    // two iterators over the same set should give the same results
    checkEquals(m.iterator, m.iterator)
  }

  @Test
  def testSizeInBytes() {
    assertEquals("Empty message set should have 0 bytes.",
                 0,
                 createMessageSet(Array[Message]()).sizeInBytes)
    assertEquals("Predicted size should equal actual size.", 
                 MessageSet.messageSetSize(messages),
                 createMessageSet(messages).sizeInBytes)
  }

  @Test
  def testWriteTo() {
    // test empty message set
    checkWriteToWithMessageSet(createMessageSet(Array[Message]()))
    checkWriteToWithMessageSet(createMessageSet(messages))
  }

  /* Tests that writing to a channel that doesn't consume all the bytes in the buffer works correctly */
  @Test
  def testWriteToChannelThatConsumesPartially() {
    val bytesToConsumePerBuffer = 50
    val messages = (0 until 10).map(_ => new Message(TestUtils.randomString(100).getBytes))
    val messageSet = createMessageSet(messages)
    val messageSetSize = messageSet.sizeInBytes

    val channel = new StubByteChannel(bytesToConsumePerBuffer)

    var remaining = messageSetSize
    var iterations = 0
    while (remaining > 0) {
      remaining -= messageSet.writeTo(channel, messageSetSize - remaining, remaining)
      iterations += 1
    }

    assertEquals((messageSetSize / bytesToConsumePerBuffer) + 1, iterations)
    checkEquals(new ByteBufferMessageSet(ByteBuffer.wrap(channel.data.toArray)).iterator, messageSet.iterator)
  }

  def checkWriteToWithMessageSet(messageSet: MessageSet) {
    checkWriteWithMessageSet(messageSet, messageSet.writeTo(_, 0, messageSet.sizeInBytes))
  }

  def checkWriteWithMessageSet(set: MessageSet, write: GatheringByteChannel => Long) {
    // do the write twice to ensure the message set is restored to its original state
    for (_ <- 0 to 1) {
      val file = tempFile()
      val channel = FileChannel.open(file.toPath, StandardOpenOption.READ, StandardOpenOption.WRITE)
      try {
        val written = write(channel)
        assertEquals("Expect to write the number of bytes in the set.", set.sizeInBytes, written)
        val newSet = new FileMessageSet(file, channel)
        checkEquals(set.iterator, newSet.iterator)
      } finally channel.close()
    }
  }
  
}

