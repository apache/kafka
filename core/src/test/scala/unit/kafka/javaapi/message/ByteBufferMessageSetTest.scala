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

import org.junit.Assert._
import org.junit.Test
import kafka.message.{DefaultCompressionCodec, CompressionCodec, NoCompressionCodec, Message}

class ByteBufferMessageSetTest extends kafka.javaapi.message.BaseMessageSetTestCases {
  
  override def createMessageSet(messages: Seq[Message], compressed: CompressionCodec = NoCompressionCodec): ByteBufferMessageSet =
    new ByteBufferMessageSet(new kafka.message.ByteBufferMessageSet(compressed, messages: _*).buffer)

  val msgSeq: Seq[Message] = Seq(new Message("hello".getBytes()), new Message("there".getBytes()))

  @Test
  def testEquals() {
    val messageList = createMessageSet(msgSeq, NoCompressionCodec)
    val moreMessages = createMessageSet(msgSeq, NoCompressionCodec)
    assertEquals(messageList, moreMessages)
    assertTrue(messageList.equals(moreMessages))
  }

  @Test
  def testEqualsWithCompression () {
    val messageList = createMessageSet(msgSeq, DefaultCompressionCodec)
    val moreMessages = createMessageSet(msgSeq, DefaultCompressionCodec)
    assertEquals(messageList, moreMessages)
    assertTrue(messageList.equals(moreMessages))
  }
}
