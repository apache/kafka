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

package kafka.javaapi.message

import java.nio._
import junit.framework.Assert._
import org.junit.Test
import kafka.message.{DefaultCompressionCodec, CompressionCodec, NoCompressionCodec, Message}

class ByteBufferMessageSetTest extends kafka.javaapi.message.BaseMessageSetTestCases {

  override def createMessageSet(messages: Seq[Message],
                                compressed: CompressionCodec = NoCompressionCodec): ByteBufferMessageSet =
    new ByteBufferMessageSet(compressed, getMessageList(messages: _*))
  
  @Test
  def testValidBytes() {
    val messageList = new ByteBufferMessageSet(compressionCodec = NoCompressionCodec,
                                               messages = getMessageList(new Message("hello".getBytes()),
                                                                      new Message("there".getBytes())))
    val buffer = ByteBuffer.allocate(messageList.sizeInBytes.toInt + 2)
    buffer.put(messageList.getBuffer)
    buffer.putShort(4)
    val messageListPlus = new ByteBufferMessageSet(buffer)
    assertEquals("Adding invalid bytes shouldn't change byte count", messageList.validBytes, messageListPlus.validBytes)
  }

  @Test
  def testValidBytesWithCompression () {
    val messageList = new ByteBufferMessageSet(compressionCodec = DefaultCompressionCodec,
                                               messages = getMessageList(new Message("hello".getBytes()),
                                                                         new Message("there".getBytes())))
    val buffer = ByteBuffer.allocate(messageList.sizeInBytes.toInt + 2)
    buffer.put(messageList.getBuffer)
    buffer.putShort(4)
    val messageListPlus = new ByteBufferMessageSet(buffer, 0, 0)
    assertEquals("Adding invalid bytes shouldn't change byte count", messageList.validBytes, messageListPlus.validBytes)
  }

  @Test
  def testEquals() {
    val messageList = new ByteBufferMessageSet(compressionCodec = NoCompressionCodec,
                                            messages = getMessageList(new Message("hello".getBytes()),
                                                                      new Message("there".getBytes())))
    val moreMessages = new ByteBufferMessageSet(compressionCodec = NoCompressionCodec,
                                                messages = getMessageList(new Message("hello".getBytes()),
                                                                          new Message("there".getBytes())))

    assertEquals(messageList, moreMessages)
    assertTrue(messageList.equals(moreMessages))
  }

  @Test
  def testEqualsWithCompression () {
    val messageList = new ByteBufferMessageSet(compressionCodec = DefaultCompressionCodec,
                                            messages = getMessageList(new Message("hello".getBytes()),
                                                                      new Message("there".getBytes())))
    val moreMessages = new ByteBufferMessageSet(compressionCodec = DefaultCompressionCodec,
                                                messages = getMessageList(new Message("hello".getBytes()),
                                                                          new Message("there".getBytes())))

    assertEquals(messageList, moreMessages)
    assertTrue(messageList.equals(moreMessages))
  }

  private def getMessageList(messages: Message*): java.util.List[Message] = {
    val messageList = new java.util.ArrayList[Message]()
    messages.foreach(m => messageList.add(m))
    messageList
  }
}
