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
import org.scalatest.junit.JUnitSuite
import org.junit.Test
import kafka.utils.TestUtils
import kafka.message.{DefaultCompressionCodec, NoCompressionCodec, CompressionCodec, Message}

trait BaseMessageSetTestCases extends JUnitSuite {
  
  val messages = Array(new Message("abcd".getBytes()), new Message("efgh".getBytes()))
  def createMessageSet(messages: Seq[Message], compressed: CompressionCodec = NoCompressionCodec): MessageSet
  def toMessageIterator(messageSet: MessageSet): Iterator[Message] = {
    import scala.collection.JavaConversions._
    messageSet.map(m => m.message).iterator
  }

  @Test
  def testWrittenEqualsRead {
    val messageSet = createMessageSet(messages)
    TestUtils.checkEquals(messages.iterator, toMessageIterator(messageSet))
  }

  @Test
  def testIteratorIsConsistent() {
    import scala.collection.JavaConversions._
    val m = createMessageSet(messages)
    // two iterators over the same set should give the same results
    TestUtils.checkEquals(m.iterator, m.iterator)
  }

  @Test
  def testIteratorIsConsistentWithCompression() {
    import scala.collection.JavaConversions._
    val m = createMessageSet(messages, DefaultCompressionCodec)
    // two iterators over the same set should give the same results
    TestUtils.checkEquals(m.iterator, m.iterator)
  }

  @Test
  def testSizeInBytes() {
    assertEquals("Empty message set should have 0 bytes.",
                 0,
                 createMessageSet(Array[Message]()).sizeInBytes)
    assertEquals("Predicted size should equal actual size.", 
                 kafka.message.MessageSet.messageSetSize(messages),
                 createMessageSet(messages).sizeInBytes)
  }

  @Test
  def testSizeInBytesWithCompression () {
    assertEquals("Empty message set should have 0 bytes.",
                 0,           // overhead of the GZIP output stream
                 createMessageSet(Array[Message](), DefaultCompressionCodec).sizeInBytes)
  }
}
