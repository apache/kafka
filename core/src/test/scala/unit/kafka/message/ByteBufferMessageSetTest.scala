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

import java.nio._

import kafka.utils.TestUtils
import org.junit.Assert._
import org.junit.Test

class ByteBufferMessageSetTest extends BaseMessageSetTestCases {

  override def createMessageSet(messages: Seq[Message]): ByteBufferMessageSet = 
    new ByteBufferMessageSet(NoCompressionCodec, messages: _*)

  @Test
  def testValidBytes() {
    {
      val messages = new ByteBufferMessageSet(NoCompressionCodec, new Message("hello".getBytes), new Message("there".getBytes))
      val buffer = ByteBuffer.allocate(messages.sizeInBytes + 2)
      buffer.put(messages.buffer)
      buffer.putShort(4)
      val messagesPlus = new ByteBufferMessageSet(buffer)
      assertEquals("Adding invalid bytes shouldn't change byte count", messages.validBytes, messagesPlus.validBytes)
    }

    // test valid bytes on empty ByteBufferMessageSet
    {
      assertEquals("Valid bytes on an empty ByteBufferMessageSet should return 0", 0,
        MessageSet.Empty.asInstanceOf[ByteBufferMessageSet].validBytes)
    }
  }

  @Test
  def testValidBytesWithCompression() {
    val messages = new ByteBufferMessageSet(DefaultCompressionCodec, new Message("hello".getBytes), new Message("there".getBytes))
    val buffer = ByteBuffer.allocate(messages.sizeInBytes + 2)
    buffer.put(messages.buffer)
    buffer.putShort(4)
    val messagesPlus = new ByteBufferMessageSet(buffer)
    assertEquals("Adding invalid bytes shouldn't change byte count", messages.validBytes, messagesPlus.validBytes)
  }

  @Test
  def testEquals() {
    var messages = new ByteBufferMessageSet(DefaultCompressionCodec, new Message("hello".getBytes), new Message("there".getBytes))
    var moreMessages = new ByteBufferMessageSet(DefaultCompressionCodec, new Message("hello".getBytes), new Message("there".getBytes))

    assertTrue(messages.equals(moreMessages))

    messages = new ByteBufferMessageSet(NoCompressionCodec, new Message("hello".getBytes), new Message("there".getBytes))
    moreMessages = new ByteBufferMessageSet(NoCompressionCodec, new Message("hello".getBytes), new Message("there".getBytes))

    assertTrue(messages.equals(moreMessages))
  }
  

  @Test
  def testIterator() {
    val messageList = List(
        new Message("msg1".getBytes),
        new Message("msg2".getBytes),
        new Message("msg3".getBytes)
      )

    // test for uncompressed regular messages
    {
      val messageSet = new ByteBufferMessageSet(NoCompressionCodec, messageList: _*)
      TestUtils.checkEquals[Message](messageList.iterator, TestUtils.getMessageIterator(messageSet.iterator))
      //make sure ByteBufferMessageSet is re-iterable.
      TestUtils.checkEquals[Message](messageList.iterator, TestUtils.getMessageIterator(messageSet.iterator))

      //make sure shallow iterator is the same as deep iterator
      TestUtils.checkEquals[Message](TestUtils.getMessageIterator(messageSet.shallowIterator),
                                     TestUtils.getMessageIterator(messageSet.iterator))
    }

    // test for compressed regular messages
    {
      val messageSet = new ByteBufferMessageSet(DefaultCompressionCodec, messageList: _*)
      TestUtils.checkEquals[Message](messageList.iterator, TestUtils.getMessageIterator(messageSet.iterator))
      //make sure ByteBufferMessageSet is re-iterable.
      TestUtils.checkEquals[Message](messageList.iterator, TestUtils.getMessageIterator(messageSet.iterator))
      verifyShallowIterator(messageSet)
    }

    // test for mixed empty and non-empty messagesets uncompressed
    {
      val emptyMessageList : List[Message] = Nil
      val emptyMessageSet = new ByteBufferMessageSet(NoCompressionCodec, emptyMessageList: _*)
      val regularMessgeSet = new ByteBufferMessageSet(NoCompressionCodec, messageList: _*)
      val buffer = ByteBuffer.allocate(emptyMessageSet.buffer.limit() + regularMessgeSet.buffer.limit())
      buffer.put(emptyMessageSet.buffer)
      buffer.put(regularMessgeSet.buffer)
      buffer.rewind
      val mixedMessageSet = new ByteBufferMessageSet(buffer)
      TestUtils.checkEquals[Message](messageList.iterator, TestUtils.getMessageIterator(mixedMessageSet.iterator))
      //make sure ByteBufferMessageSet is re-iterable.
      TestUtils.checkEquals[Message](messageList.iterator, TestUtils.getMessageIterator(mixedMessageSet.iterator))
      //make sure shallow iterator is the same as deep iterator
      TestUtils.checkEquals[Message](TestUtils.getMessageIterator(mixedMessageSet.shallowIterator),
                                     TestUtils.getMessageIterator(mixedMessageSet.iterator))
    }

    // test for mixed empty and non-empty messagesets compressed
    {
      val emptyMessageList : List[Message] = Nil
      val emptyMessageSet = new ByteBufferMessageSet(DefaultCompressionCodec, emptyMessageList: _*)
      val regularMessgeSet = new ByteBufferMessageSet(DefaultCompressionCodec, messageList: _*)
      val buffer = ByteBuffer.allocate(emptyMessageSet.buffer.limit() + regularMessgeSet.buffer.limit())
      buffer.put(emptyMessageSet.buffer)
      buffer.put(regularMessgeSet.buffer)
      buffer.rewind
      val mixedMessageSet = new ByteBufferMessageSet(buffer)
      TestUtils.checkEquals[Message](messageList.iterator, TestUtils.getMessageIterator(mixedMessageSet.iterator))
      //make sure ByteBufferMessageSet is re-iterable.
      TestUtils.checkEquals[Message](messageList.iterator, TestUtils.getMessageIterator(mixedMessageSet.iterator))
      verifyShallowIterator(mixedMessageSet)
    }
  }

  @Test
  def testMessageWithProvidedOffsetSeq() {
    val offsets = Seq(0L, 2L)
    val messages = new ByteBufferMessageSet(
      compressionCodec = NoCompressionCodec,
      offsetSeq = offsets,
      new Message("hello".getBytes),
      new Message("goodbye".getBytes))
    val iter = messages.iterator
    assertEquals("first offset should be 0", 0L, iter.next().offset)
    assertEquals("second offset should be 2", 2L, iter.next().offset)
  }

  /* check that offsets are assigned based on byte offset from the given base offset */
  def checkOffsets(messages: ByteBufferMessageSet, baseOffset: Long) {
    assertTrue("Message set should not be empty", messages.nonEmpty)
    var offset = baseOffset
    for(entry <- messages) {
      assertEquals("Unexpected offset in message set iterator", offset, entry.offset)
      offset += 1
    }
  }

  def verifyShallowIterator(messageSet: ByteBufferMessageSet) {
    //make sure the offsets returned by a shallow iterator is a subset of that of a deep iterator
    val shallowOffsets = messageSet.shallowIterator.map(msgAndOff => msgAndOff.offset).toSet
    val deepOffsets = messageSet.iterator.map(msgAndOff => msgAndOff.offset).toSet
    assertTrue(shallowOffsets.subsetOf(deepOffsets))
  }

}
