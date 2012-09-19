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

import kafka.utils.TestUtils
import org.scalatest.junit.JUnitSuite
import org.junit.Test
import junit.framework.Assert._

class CompressionUtilTest extends JUnitSuite {

  
  @Test
  def testSimpleCompressDecompress() {

    val messages = List[Message](new Message("hi there".getBytes), new Message("I am fine".getBytes), new Message("I am not so well today".getBytes))

    val message = CompressionUtils.compress(messages)

    val decompressedMessages = CompressionUtils.decompress(message)

    TestUtils.checkLength(decompressedMessages.iterator,3)

    TestUtils.checkEquals(messages.iterator, TestUtils.getMessageIterator(decompressedMessages.iterator))
  }

  @Test
  def testComplexCompressDecompress() {

    val messages = List[Message](new Message("hi there".getBytes), new Message("I am fine".getBytes), new Message("I am not so well today".getBytes))

    val message = CompressionUtils.compress(messages.slice(0, 2))

    val complexMessages = List[Message](message):::messages.slice(2,3)

    val complexMessage = CompressionUtils.compress(complexMessages)

    val decompressedMessages = CompressionUtils.decompress(complexMessage)

    TestUtils.checkLength(TestUtils.getMessageIterator(decompressedMessages.iterator),3)

    TestUtils.checkEquals(messages.iterator, TestUtils.getMessageIterator(decompressedMessages.iterator))
  }

  @Test
  def testSnappyCompressDecompressExplicit() {

    val messages = List[Message](new Message("hi there".getBytes), new Message("I am fine".getBytes), new Message("I am not so well today".getBytes))

    val message = CompressionUtils.compress(messages,SnappyCompressionCodec)

    assertEquals(message.compressionCodec,SnappyCompressionCodec)

    val decompressedMessages = CompressionUtils.decompress(message)

    TestUtils.checkLength(decompressedMessages.iterator,3)

    TestUtils.checkEquals(messages.iterator, TestUtils.getMessageIterator(decompressedMessages.iterator))
  }
}
