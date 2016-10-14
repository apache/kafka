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

import java.io.DataOutputStream
import java.nio._

import kafka.common.LongRef
import kafka.utils.TestUtils
import org.apache.kafka.common.errors.InvalidTimestampException
import org.apache.kafka.common.record.TimestampType
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
      val buffer = ByteBuffer.allocate(emptyMessageSet.buffer.limit + regularMessgeSet.buffer.limit)
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
      val buffer = ByteBuffer.allocate(emptyMessageSet.buffer.limit + regularMessgeSet.buffer.limit)
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

  @Test
  def testLogAppendTime() {
    val now = System.currentTimeMillis()
    // The timestamps should be overwritten
    val messages = getMessages(magicValue = Message.MagicValue_V1, timestamp = 0L, codec = NoCompressionCodec)
    val compressedMessagesWithRecompresion = getMessages(magicValue = Message.MagicValue_V0, codec = DefaultCompressionCodec)
    val compressedMessagesWithoutRecompression =
      getMessages(magicValue = Message.MagicValue_V1, timestamp = 0L, codec = DefaultCompressionCodec)

    val validatingResults = messages.validateMessagesAndAssignOffsets(offsetCounter = new LongRef(0),
                                                                      now = now,
                                                                      sourceCodec = NoCompressionCodec,
                                                                      targetCodec = NoCompressionCodec,
                                                                      messageFormatVersion = 1,
                                                                      messageTimestampType = TimestampType.LOG_APPEND_TIME,
                                                                      messageTimestampDiffMaxMs = 1000L)
    val validatedMessages = validatingResults.validatedMessages

    val validatingCompressedMessagesResults =
      compressedMessagesWithRecompresion.validateMessagesAndAssignOffsets(offsetCounter = new LongRef(0),
                                                                          now = now,
                                                                          sourceCodec = DefaultCompressionCodec,
                                                                          targetCodec = DefaultCompressionCodec,
                                                                          messageFormatVersion = 1,
                                                                          messageTimestampType = TimestampType.LOG_APPEND_TIME,
                                                                          messageTimestampDiffMaxMs = 1000L)
    val validatedCompressedMessages = validatingCompressedMessagesResults.validatedMessages

    val validatingCompressedMessagesWithoutRecompressionResults =
      compressedMessagesWithoutRecompression.validateMessagesAndAssignOffsets(offsetCounter = new LongRef(0),
                                                                              now = now,
                                                                              sourceCodec = DefaultCompressionCodec,
                                                                              targetCodec = DefaultCompressionCodec,
                                                                              messageFormatVersion = 1,
                                                                              messageTimestampType = TimestampType.LOG_APPEND_TIME,
                                                                              messageTimestampDiffMaxMs = 1000L)

    val validatedCompressedMessagesWithoutRecompression = validatingCompressedMessagesWithoutRecompressionResults.validatedMessages

    assertEquals("message set size should not change", messages.size, validatedMessages.size)
    validatedMessages.foreach(messageAndOffset => validateLogAppendTime(messageAndOffset.message))
    assertEquals(s"Max timestamp should be $now", now, validatingResults.maxTimestamp)
    assertEquals(s"The offset of max timestamp should be 0", 0, validatingResults.offsetOfMaxTimestamp)
    assertFalse("Message size should not have been changed", validatingResults.messageSizeMaybeChanged)

    assertEquals("message set size should not change", compressedMessagesWithRecompresion.size, validatedCompressedMessages.size)
    validatedCompressedMessages.foreach(messageAndOffset => validateLogAppendTime(messageAndOffset.message))
    assertTrue("MessageSet should still valid", validatedCompressedMessages.shallowIterator.next().message.isValid)
    assertEquals(s"Max timestamp should be $now", now, validatingCompressedMessagesResults.maxTimestamp)
    assertEquals(s"The offset of max timestamp should be ${compressedMessagesWithRecompresion.size - 1}",
      compressedMessagesWithRecompresion.size - 1, validatingCompressedMessagesResults.offsetOfMaxTimestamp)
    assertTrue("Message size may have been changed", validatingCompressedMessagesResults.messageSizeMaybeChanged)

    assertEquals("message set size should not change", compressedMessagesWithoutRecompression.size,
      validatedCompressedMessagesWithoutRecompression.size)
    validatedCompressedMessagesWithoutRecompression.foreach(messageAndOffset => validateLogAppendTime(messageAndOffset.message))
    assertTrue("MessageSet should still valid", validatedCompressedMessagesWithoutRecompression.shallowIterator.next().message.isValid)
    assertEquals(s"Max timestamp should be $now", now, validatingCompressedMessagesWithoutRecompressionResults.maxTimestamp)
    assertEquals(s"The offset of max timestamp should be ${compressedMessagesWithoutRecompression.size - 1}",
      compressedMessagesWithoutRecompression.size - 1, validatingCompressedMessagesWithoutRecompressionResults.offsetOfMaxTimestamp)
    assertFalse("Message size should not have been changed", validatingCompressedMessagesWithoutRecompressionResults.messageSizeMaybeChanged)

    def validateLogAppendTime(message: Message) {
      message.ensureValid()
      assertEquals(s"Timestamp of message $message should be $now", now, message.timestamp)
      assertEquals(TimestampType.LOG_APPEND_TIME, message.timestampType)
    }
  }

  @Test
  def testCreateTime() {
    val now = System.currentTimeMillis()
    val timestampSeq = Seq(now - 1, now + 1, now)
    val messages =
      new ByteBufferMessageSet(NoCompressionCodec,
                               new Message("hello".getBytes, timestamp = timestampSeq(0), magicValue = Message.MagicValue_V1),
                               new Message("there".getBytes, timestamp = timestampSeq(1), magicValue = Message.MagicValue_V1),
                               new Message("beautiful".getBytes, timestamp = timestampSeq(2), magicValue = Message.MagicValue_V1))
    val compressedMessages =
      new ByteBufferMessageSet(DefaultCompressionCodec,
                               new Message("hello".getBytes, timestamp = timestampSeq(0), magicValue = Message.MagicValue_V1),
                               new Message("there".getBytes, timestamp = timestampSeq(1), magicValue = Message.MagicValue_V1),
                               new Message("beautiful".getBytes, timestamp = timestampSeq(2), magicValue = Message.MagicValue_V1))

    val validatingResults = messages.validateMessagesAndAssignOffsets(offsetCounter = new LongRef(0),
                                                                      now = System.currentTimeMillis(),
                                                                      sourceCodec = NoCompressionCodec,
                                                                      targetCodec = NoCompressionCodec,
                                                                      messageFormatVersion = 1,
                                                                      messageTimestampType = TimestampType.CREATE_TIME,
                                                                      messageTimestampDiffMaxMs = 1000L)
    val validatedMessages = validatingResults.validatedMessages

    val validatingCompressedMessagesResults =
      compressedMessages.validateMessagesAndAssignOffsets(offsetCounter = new LongRef(0),
                                                          now = System.currentTimeMillis(),
                                                          sourceCodec = DefaultCompressionCodec,
                                                          targetCodec = DefaultCompressionCodec,
                                                          messageFormatVersion = 1,
                                                          messageTimestampType = TimestampType.CREATE_TIME,
                                                          messageTimestampDiffMaxMs = 1000L)
    val validatedCompressedMessages = validatingCompressedMessagesResults.validatedMessages

    var i = 0
    for (messageAndOffset <- validatedMessages) {
      messageAndOffset.message.ensureValid()
      assertEquals(messageAndOffset.message.timestamp, timestampSeq(i))
      assertEquals(messageAndOffset.message.timestampType, TimestampType.CREATE_TIME)
      i += 1
    }
    assertEquals(s"Max timestamp should be ${now + 1}", now + 1, validatingResults.maxTimestamp)
    assertEquals(s"Offset of max timestamp should be 1", 1, validatingResults.offsetOfMaxTimestamp)
    assertFalse("Message size should not have been changed", validatingResults.messageSizeMaybeChanged)
    i = 0
    for (messageAndOffset <- validatedCompressedMessages) {
      messageAndOffset.message.ensureValid()
      assertEquals(messageAndOffset.message.timestamp, timestampSeq(i))
      assertEquals(messageAndOffset.message.timestampType, TimestampType.CREATE_TIME)
      i += 1
    }
    assertEquals(s"Max timestamp should be ${now + 1}", now + 1, validatingResults.maxTimestamp)
    assertEquals(s"Offset of max timestamp should be ${validatedCompressedMessages.size - 1}",
      validatedCompressedMessages.size - 1, validatingCompressedMessagesResults.offsetOfMaxTimestamp)
    assertFalse("Message size should not have been changed", validatingCompressedMessagesResults.messageSizeMaybeChanged)
  }

  @Test
  def testInvalidCreateTime() {
    val now = System.currentTimeMillis()
    val messages = getMessages(magicValue = Message.MagicValue_V1, timestamp = now - 1001L, codec = NoCompressionCodec)
    val compressedMessages = getMessages(magicValue = Message.MagicValue_V1, timestamp = now - 1001L, codec = DefaultCompressionCodec)

    try {
      messages.validateMessagesAndAssignOffsets(offsetCounter = new LongRef(0),
                                                now = System.currentTimeMillis(),
                                                sourceCodec = NoCompressionCodec,
                                                targetCodec = NoCompressionCodec,
                                                messageFormatVersion = 1,
                                                messageTimestampType = TimestampType.CREATE_TIME,
                                                messageTimestampDiffMaxMs = 1000L)
      fail("Should throw InvalidMessageException.")
    } catch {
      case e: InvalidTimestampException =>
    }

    try {
      compressedMessages.validateMessagesAndAssignOffsets(offsetCounter = new LongRef(0),
                                                          now = System.currentTimeMillis(),
                                                          sourceCodec = DefaultCompressionCodec,
                                                          targetCodec = DefaultCompressionCodec,
                                                          messageFormatVersion = 1,
                                                          messageTimestampType = TimestampType.CREATE_TIME,
                                                          messageTimestampDiffMaxMs = 1000L)
      fail("Should throw InvalidMessageException.")
    } catch {
      case e: InvalidTimestampException =>
    }
  }

  @Test
  def testAbsoluteOffsetAssignment() {
    val messages = getMessages(magicValue = Message.MagicValue_V0, codec = NoCompressionCodec)
    val compressedMessages = getMessages(magicValue = Message.MagicValue_V0, codec = DefaultCompressionCodec)
    // check uncompressed offsets
    checkOffsets(messages, 0)
    val offset = 1234567
    checkOffsets(messages.validateMessagesAndAssignOffsets(offsetCounter = new LongRef(offset),
                                                           now = System.currentTimeMillis(),
                                                           sourceCodec = NoCompressionCodec,
                                                           targetCodec = NoCompressionCodec,
                                                           messageFormatVersion = 0,
                                                           messageTimestampType = TimestampType.CREATE_TIME,
                                                           messageTimestampDiffMaxMs = 1000L).validatedMessages, offset)

    // check compressed messages
    checkOffsets(compressedMessages, 0)
    checkOffsets(compressedMessages.validateMessagesAndAssignOffsets(offsetCounter = new LongRef(offset),
                                                                     now = System.currentTimeMillis(),
                                                                     sourceCodec = DefaultCompressionCodec,
                                                                     targetCodec = DefaultCompressionCodec,
                                                                     messageFormatVersion = 0,
                                                                     messageTimestampType = TimestampType.CREATE_TIME,
                                                                     messageTimestampDiffMaxMs = 1000L).validatedMessages, offset)

  }

  @Test
  def testRelativeOffsetAssignment() {
    val now = System.currentTimeMillis()
    val messages = getMessages(magicValue = Message.MagicValue_V1, timestamp = now, codec = NoCompressionCodec)
    val compressedMessages = getMessages(magicValue = Message.MagicValue_V1, timestamp = now, codec = DefaultCompressionCodec)

    // check uncompressed offsets
    checkOffsets(messages, 0)
    val offset = 1234567
    val messageWithOffset = messages.validateMessagesAndAssignOffsets(offsetCounter = new LongRef(offset),
                                                                      now = System.currentTimeMillis(),
                                                                      sourceCodec = NoCompressionCodec,
                                                                      targetCodec = NoCompressionCodec,
                                                                      messageTimestampType = TimestampType.CREATE_TIME,
                                                                      messageTimestampDiffMaxMs = 5000L).validatedMessages
    checkOffsets(messageWithOffset, offset)

    // check compressed messages
    checkOffsets(compressedMessages, 0)
    val compressedMessagesWithOffset = compressedMessages.validateMessagesAndAssignOffsets(offsetCounter = new LongRef(offset),
                                                                                           now = System.currentTimeMillis(),
                                                                                           sourceCodec = DefaultCompressionCodec,
                                                                                           targetCodec = DefaultCompressionCodec,
                                                                                           messageTimestampType = TimestampType.CREATE_TIME,
                                                                                           messageTimestampDiffMaxMs = 5000L).validatedMessages
    checkOffsets(compressedMessagesWithOffset, offset)
  }

  @Test(expected = classOf[InvalidMessageException])
  def testInvalidInnerMagicVersion(): Unit = {
    val offset = 1234567
    val messages = messageSetWithInvalidInnerMagic(SnappyCompressionCodec, offset)
    messages.validateMessagesAndAssignOffsets(offsetCounter = new LongRef(offset),
      now = System.currentTimeMillis(),
      sourceCodec = SnappyCompressionCodec,
      targetCodec = SnappyCompressionCodec,
      messageTimestampType = TimestampType.CREATE_TIME,
      messageTimestampDiffMaxMs = 5000L).validatedMessages
  }


  @Test
  def testOffsetAssignmentAfterMessageFormatConversion() {
    // Check up conversion
    val messagesV0 = getMessages(magicValue = Message.MagicValue_V0, codec = NoCompressionCodec)
    val compressedMessagesV0 = getMessages(magicValue = Message.MagicValue_V0, codec = DefaultCompressionCodec)
    // check uncompressed offsets
    checkOffsets(messagesV0, 0)
    val offset = 1234567
    checkOffsets(messagesV0.validateMessagesAndAssignOffsets(offsetCounter = new LongRef(offset),
                                                             now = System.currentTimeMillis(),
                                                             sourceCodec = NoCompressionCodec,
                                                             targetCodec = NoCompressionCodec,
                                                             messageFormatVersion = 1,
                                                             messageTimestampType = TimestampType.LOG_APPEND_TIME,
                                                             messageTimestampDiffMaxMs = 1000L).validatedMessages, offset)

    // check compressed messages
    checkOffsets(compressedMessagesV0, 0)
    checkOffsets(compressedMessagesV0.validateMessagesAndAssignOffsets(offsetCounter = new LongRef(offset),
                                                                       now = System.currentTimeMillis(),
                                                                       sourceCodec = DefaultCompressionCodec,
                                                                       targetCodec = DefaultCompressionCodec,
                                                                       messageFormatVersion = 1,
                                                                       messageTimestampType = TimestampType.LOG_APPEND_TIME,
                                                                       messageTimestampDiffMaxMs = 1000L).validatedMessages, offset)

    // Check down conversion
    val now = System.currentTimeMillis()
    val messagesV1 = getMessages(Message.MagicValue_V1, now, NoCompressionCodec)
    val compressedMessagesV1 = getMessages(Message.MagicValue_V1, now, DefaultCompressionCodec)

    // check uncompressed offsets
    checkOffsets(messagesV1, 0)
    checkOffsets(messagesV1.validateMessagesAndAssignOffsets(offsetCounter = new LongRef(offset),
                                                             now = System.currentTimeMillis(),
                                                             sourceCodec = NoCompressionCodec,
                                                             targetCodec = NoCompressionCodec,
                                                             messageFormatVersion = 0,
                                                             messageTimestampType = TimestampType.CREATE_TIME,
                                                             messageTimestampDiffMaxMs = 5000L).validatedMessages, offset)

    // check compressed messages
    checkOffsets(compressedMessagesV1, 0)
    checkOffsets(compressedMessagesV1.validateMessagesAndAssignOffsets(offsetCounter = new LongRef(offset),
                                                                       now = System.currentTimeMillis(),
                                                                       sourceCodec = DefaultCompressionCodec,
                                                                       targetCodec = DefaultCompressionCodec,
                                                                       messageFormatVersion = 0,
                                                                       messageTimestampType = TimestampType.CREATE_TIME,
                                                                       messageTimestampDiffMaxMs = 5000L).validatedMessages, offset)
  }

  @Test
  def testWriteFullyTo() {
    checkWriteFullyToWithMessageSet(createMessageSet(Array[Message]()))
    checkWriteFullyToWithMessageSet(createMessageSet(messages))
  }

  def checkWriteFullyToWithMessageSet(messageSet: ByteBufferMessageSet) {
    checkWriteWithMessageSet(messageSet, messageSet.writeFullyTo)
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

  private def getMessages(magicValue: Byte = Message.CurrentMagicValue,
                          timestamp: Long = Message.NoTimestamp,
                          codec: CompressionCodec = NoCompressionCodec): ByteBufferMessageSet = {
    if (magicValue == Message.MagicValue_V0) {
      new ByteBufferMessageSet(
        codec,
        new Message("hello".getBytes, Message.NoTimestamp, Message.MagicValue_V0),
        new Message("there".getBytes, Message.NoTimestamp, Message.MagicValue_V0),
        new Message("beautiful".getBytes, Message.NoTimestamp, Message.MagicValue_V0))
    } else {
      new ByteBufferMessageSet(
        codec,
        new Message("hello".getBytes, timestamp = timestamp, magicValue = Message.MagicValue_V1),
        new Message("there".getBytes, timestamp = timestamp, magicValue = Message.MagicValue_V1),
        new Message("beautiful".getBytes, timestamp = timestamp, magicValue = Message.MagicValue_V1))
    }
  }

  private def messageSetWithInvalidInnerMagic(codec: CompressionCodec,
                                              initialOffset: Long): ByteBufferMessageSet = {
    val messages = (0 until 20).map(id =>
      new Message(key = id.toString.getBytes,
        bytes = id.toString.getBytes,
        timestamp = Message.NoTimestamp,
        magicValue = Message.MagicValue_V0))

    val messageWriter = new MessageWriter(math.min(math.max(MessageSet.messageSetSize(messages) / 2, 1024), 1 << 16))
    var lastOffset = initialOffset

    messageWriter.write(
      codec = codec,
      timestamp = System.currentTimeMillis(),
      timestampType = TimestampType.CREATE_TIME,
      magicValue = Message.MagicValue_V1) { outputStream =>

      val output = new DataOutputStream(CompressionFactory(codec, Message.MagicValue_V1, outputStream))
      try {
        for (message <- messages) {
          val innerOffset = lastOffset - initialOffset
          output.writeLong(innerOffset)
          output.writeInt(message.size)
          output.write(message.buffer.array, message.buffer.arrayOffset, message.buffer.limit)
          lastOffset += 1
        }
      } finally {
        output.close()
      }
    }
    val buffer = ByteBuffer.allocate(messageWriter.size + MessageSet.LogOverhead)
    ByteBufferMessageSet.writeMessage(buffer, messageWriter, lastOffset - 1)
    buffer.rewind()

    new ByteBufferMessageSet(buffer)
  }

}
