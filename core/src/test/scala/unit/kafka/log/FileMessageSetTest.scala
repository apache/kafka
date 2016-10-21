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

import java.io._
import java.nio._
import java.nio.channels._

import kafka.common.LongRef
import org.junit.Assert._
import kafka.utils.TestUtils._
import kafka.message._
import kafka.common.KafkaException
import org.easymock.EasyMock
import org.junit.Test

class FileMessageSetTest extends BaseMessageSetTestCases {
  
  val messageSet = createMessageSet(messages)
  
  def createMessageSet(messages: Seq[Message]): FileMessageSet = {
    val set = new FileMessageSet(tempFile())
    set.append(new ByteBufferMessageSet(NoCompressionCodec, messages: _*))
    set.flush()
    set
  }

  /**
   * Test that the cached size variable matches the actual file size as we append messages
   */
  @Test
  def testFileSize() {
    assertEquals(messageSet.channel.size, messageSet.sizeInBytes)
    for(i <- 0 until 20) {
      messageSet.append(singleMessageSet("abcd".getBytes))
      assertEquals(messageSet.channel.size, messageSet.sizeInBytes)
    } 
  }
  
  /**
   * Test that adding invalid bytes to the end of the log doesn't break iteration
   */
  @Test
  def testIterationOverPartialAndTruncation() {
    testPartialWrite(0, messageSet)
    testPartialWrite(2, messageSet)
    testPartialWrite(4, messageSet)
    testPartialWrite(5, messageSet)
    testPartialWrite(6, messageSet)
  }
  
  def testPartialWrite(size: Int, messageSet: FileMessageSet) {
    val buffer = ByteBuffer.allocate(size)
    val originalPosition = messageSet.channel.position
    for(i <- 0 until size)
      buffer.put(0.asInstanceOf[Byte])
    buffer.rewind()
    messageSet.channel.write(buffer)
    // appending those bytes should not change the contents
    checkEquals(messages.iterator, messageSet.map(m => m.message).iterator)
  }
  
  /**
   * Iterating over the file does file reads but shouldn't change the position of the underlying FileChannel.
   */
  @Test
  def testIterationDoesntChangePosition() {
    val position = messageSet.channel.position
    checkEquals(messages.iterator, messageSet.map(m => m.message).iterator)
    assertEquals(position, messageSet.channel.position)
  }
  
  /**
   * Test a simple append and read.
   */
  @Test
  def testRead() {
    var read = messageSet.read(0, messageSet.sizeInBytes)
    checkEquals(messageSet.iterator, read.iterator)
    val items = read.iterator.toList
    val sec = items.tail.head
    read = messageSet.read(position = MessageSet.entrySize(sec.message), size = messageSet.sizeInBytes)
    assertEquals("Try a read starting from the second message", items.tail, read.toList)
    read = messageSet.read(MessageSet.entrySize(sec.message), MessageSet.entrySize(sec.message))
    assertEquals("Try a read of a single message starting from the second message", List(items.tail.head), read.toList)
  }
  
  /**
   * Test the MessageSet.searchFor API.
   */
  @Test
  def testSearch() {
    // append a new message with a high offset
    val lastMessage = new Message("test".getBytes)
    messageSet.append(new ByteBufferMessageSet(NoCompressionCodec, new LongRef(50), lastMessage))
    val messages = messageSet.toSeq
    var position = 0
    val message1Size = MessageSet.entrySize(messages.head.message)
    assertEquals("Should be able to find the first message by its offset",
                 (OffsetPosition(0L, position), message1Size),
                 messageSet.searchForOffsetWithSize(0, 0))
    position += message1Size
    val message2Size = MessageSet.entrySize(messages(1).message)
    assertEquals("Should be able to find second message when starting from 0", 
                 (OffsetPosition(1L, position), message2Size),
                 messageSet.searchForOffsetWithSize(1, 0))
    assertEquals("Should be able to find second message starting from its offset", 
                 (OffsetPosition(1L, position), message2Size),
                 messageSet.searchForOffsetWithSize(1, position))
    position += message2Size + MessageSet.entrySize(messages(2).message)
    val message4Size = MessageSet.entrySize(messages(3).message)
    assertEquals("Should be able to find fourth message from a non-existant offset", 
                 (OffsetPosition(50L, position), message4Size),
                 messageSet.searchForOffsetWithSize(3, position))
    assertEquals("Should be able to find fourth message by correct offset",
                 (OffsetPosition(50L, position), message4Size),
                 messageSet.searchForOffsetWithSize(50,  position))
  }
  
  /**
   * Test that the message set iterator obeys start and end slicing
   */
  @Test
  def testIteratorWithLimits() {
    val message = messageSet.toList(1)
    val start = messageSet.searchForOffsetWithSize(1, 0)._1.position
    val size = message.message.size + 12
    val slice = messageSet.read(start, size)
    assertEquals(List(message), slice.toList)
    val slice2 = messageSet.read(start, size - 1)
    assertEquals(List(), slice2.toList)
  }

  /**
   * Test the truncateTo method lops off messages and appropriately updates the size
   */
  @Test
  def testTruncate() {
    val message = messageSet.toList.head
    val end = messageSet.searchForOffsetWithSize(1, 0)._1.position
    messageSet.truncateTo(end)
    assertEquals(List(message), messageSet.toList)
    assertEquals(MessageSet.entrySize(message.message), messageSet.sizeInBytes)
  }

  /**
    * Test that truncateTo only calls truncate on the FileChannel if the size of the
    * FileChannel is bigger than the target size. This is important because some JVMs
    * change the mtime of the file, even if truncate should do nothing.
    */
  @Test
  def testTruncateNotCalledIfSizeIsSameAsTargetSize() {
    val channelMock = EasyMock.createMock(classOf[FileChannel])

    EasyMock.expect(channelMock.size).andReturn(42L).atLeastOnce()
    EasyMock.expect(channelMock.position(42L)).andReturn(null)
    EasyMock.replay(channelMock)

    val msgSet = new FileMessageSet(tempFile(), channelMock)
    msgSet.truncateTo(42)

    EasyMock.verify(channelMock)
  }

  /**
    * Expect a KafkaException if targetSize is bigger than the size of
    * the FileMessageSet.
    */
  @Test
  def testTruncateNotCalledIfSizeIsBiggerThanTargetSize() {
    val channelMock = EasyMock.createMock(classOf[FileChannel])

    EasyMock.expect(channelMock.size).andReturn(42L).atLeastOnce()
    EasyMock.expect(channelMock.position(42L)).andReturn(null)
    EasyMock.replay(channelMock)

    val msgSet = new FileMessageSet(tempFile(), channelMock)

    try {
      msgSet.truncateTo(43)
      fail("Should throw KafkaException")
    } catch {
      case e: KafkaException => // expected
    }

    EasyMock.verify(channelMock)
  }

  /**
    * see #testTruncateNotCalledIfSizeIsSameAsTargetSize
    */
  @Test
  def testTruncateIfSizeIsDifferentToTargetSize() {
    val channelMock = EasyMock.createMock(classOf[FileChannel])

    EasyMock.expect(channelMock.size).andReturn(42L).atLeastOnce()
    EasyMock.expect(channelMock.position(42L)).andReturn(null).once()
    EasyMock.expect(channelMock.truncate(23L)).andReturn(null).once()
    EasyMock.expect(channelMock.position(23L)).andReturn(null).once()
    EasyMock.replay(channelMock)

    val msgSet = new FileMessageSet(tempFile(), channelMock)
    msgSet.truncateTo(23)

    EasyMock.verify(channelMock)
  }


  /**
   * Test the new FileMessageSet with pre allocate as true
   */
  @Test
  def testPreallocateTrue() {
    val temp = tempFile()
    val set = new FileMessageSet(temp, false, 512 *1024 *1024, true)
    val position = set.channel.position
    val size = set.sizeInBytes()
    assertEquals(0, position)
    assertEquals(0, size)
    assertEquals(512 *1024 *1024, temp.length)
  }

  /**
   * Test the new FileMessageSet with pre allocate as false
   */
  @Test
  def testPreallocateFalse() {
    val temp = tempFile()
    val set = new FileMessageSet(temp, false, 512 *1024 *1024, false)
    val position = set.channel.position
    val size = set.sizeInBytes()
    assertEquals(0, position)
    assertEquals(0, size)
    assertEquals(0, temp.length)
  }

  /**
   * Test the new FileMessageSet with pre allocate as true and file has been clearly shut down, the file will be truncate to end of valid data.
   */
  @Test
  def testPreallocateClearShutdown() {
    val temp = tempFile()
    val set = new FileMessageSet(temp, false, 512 *1024 *1024, true)
    set.append(new ByteBufferMessageSet(NoCompressionCodec, messages: _*))
    val oldposition = set.channel.position
    val oldsize = set.sizeInBytes()
    assertEquals(messageSet.sizeInBytes, oldposition)
    assertEquals(messageSet.sizeInBytes, oldsize)
    set.close()

    val tempReopen = new File(temp.getAbsolutePath())
    val setReopen = new FileMessageSet(tempReopen, true, 512 *1024 *1024, true)
    val position = setReopen.channel.position
    val size = setReopen.sizeInBytes()

    assertEquals(oldposition, position)
    assertEquals(oldposition, size)
    assertEquals(oldposition, tempReopen.length)
  }

  @Test
  def testFormatConversionWithPartialMessage() {
    val message = messageSet.toList(1)
    val start = messageSet.searchForOffsetWithSize(1, 0)._1.position
    val size = message.message.size + 12
    val slice = messageSet.read(start, size - 1)
    val messageV0 = slice.toMessageFormat(Message.MagicValue_V0)
    assertEquals("No message should be there", 0, messageV0.size)
    assertEquals(s"There should be ${size - 1} bytes", size - 1, messageV0.sizeInBytes)
  }

  @Test
  def testMessageFormatConversion() {

    // Prepare messages.
    val offsets = Seq(0L, 2L)
    val messagesV0 = Seq(new Message("hello".getBytes, "k1".getBytes, Message.NoTimestamp, Message.MagicValue_V0),
      new Message("goodbye".getBytes, "k2".getBytes, Message.NoTimestamp, Message.MagicValue_V0))
    val messageSetV0 = new ByteBufferMessageSet(
      compressionCodec = NoCompressionCodec,
      offsetSeq = offsets,
      messages = messagesV0:_*)
    val compressedMessageSetV0 = new ByteBufferMessageSet(
      compressionCodec = DefaultCompressionCodec,
      offsetSeq = offsets,
      messages = messagesV0:_*)

    val messagesV1 = Seq(new Message("hello".getBytes, "k1".getBytes, 1L, Message.MagicValue_V1),
                         new Message("goodbye".getBytes, "k2".getBytes, 2L, Message.MagicValue_V1))
    val messageSetV1 = new ByteBufferMessageSet(
      compressionCodec = NoCompressionCodec,
      offsetSeq = offsets,
      messages = messagesV1:_*)
    val compressedMessageSetV1 = new ByteBufferMessageSet(
      compressionCodec = DefaultCompressionCodec,
      offsetSeq = offsets,
      messages = messagesV1:_*)

    // Down conversion
    // down conversion for non-compressed messages
    var fileMessageSet = new FileMessageSet(tempFile())
    fileMessageSet.append(messageSetV1)
    fileMessageSet.flush()
    var convertedMessageSet = fileMessageSet.toMessageFormat(Message.MagicValue_V0)
    verifyConvertedMessageSet(convertedMessageSet, Message.MagicValue_V0)

    // down conversion for compressed messages
    fileMessageSet = new FileMessageSet(tempFile())
    fileMessageSet.append(compressedMessageSetV1)
    fileMessageSet.flush()
    convertedMessageSet = fileMessageSet.toMessageFormat(Message.MagicValue_V0)
    verifyConvertedMessageSet(convertedMessageSet, Message.MagicValue_V0)

    // Up conversion. In reality we only do down conversion, but up conversion should work as well.
    // up conversion for non-compressed messages
    fileMessageSet = new FileMessageSet(tempFile())
    fileMessageSet.append(messageSetV0)
    fileMessageSet.flush()
    convertedMessageSet = fileMessageSet.toMessageFormat(Message.MagicValue_V1)
    verifyConvertedMessageSet(convertedMessageSet, Message.MagicValue_V1)

    // up conversion for compressed messages
    fileMessageSet = new FileMessageSet(tempFile())
    fileMessageSet.append(compressedMessageSetV0)
    fileMessageSet.flush()
    convertedMessageSet = fileMessageSet.toMessageFormat(Message.MagicValue_V1)
    verifyConvertedMessageSet(convertedMessageSet, Message.MagicValue_V1)

    def verifyConvertedMessageSet(convertedMessageSet: MessageSet, magicByte: Byte) {
      var i = 0
      for (messageAndOffset <- convertedMessageSet) {
        assertEquals("magic byte should be 1", magicByte, messageAndOffset.message.magic)
        assertEquals("offset should not change", offsets(i), messageAndOffset.offset)
        assertEquals("key should not change", messagesV0(i).key, messageAndOffset.message.key)
        assertEquals("payload should not change", messagesV0(i).payload, messageAndOffset.message.payload)
        i += 1
      }
    }
  }
}
