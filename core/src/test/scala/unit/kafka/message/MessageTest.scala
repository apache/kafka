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
import java.util.HashMap

import org.apache.kafka.common.protocol.Errors

import scala.collection._
import org.junit.Assert._
import org.scalatest.junit.JUnitSuite
import org.junit.{Before, Test}
import kafka.utils.TestUtils
import org.apache.kafka.common.utils.ByteUtils

case class MessageTestVal(key: Array[Byte],
                          payload: Array[Byte],
                          codec: CompressionCodec,
                          timestamp: Long,
                          magicValue: Byte,
                          message: Message)

class MessageTest extends JUnitSuite {

  var messages = new mutable.ArrayBuffer[MessageTestVal]()

  @Before
  def setUp(): Unit = {
    val keys = Array(null, "key".getBytes, "".getBytes)
    val vals = Array("value".getBytes, "".getBytes, null)
    val codecs = Array(NoCompressionCodec, GZIPCompressionCodec, SnappyCompressionCodec, LZ4CompressionCodec)
    val timestamps = Array(Message.NoTimestamp, 0L, 1L)
    val magicValues = Array(Message.MagicValue_V0, Message.MagicValue_V1)
    for(k <- keys; v <- vals; codec <- codecs; t <- timestamps; mv <- magicValues) {
      val timestamp = ensureValid(mv, t)
      messages += MessageTestVal(k, v, codec, timestamp, mv, new Message(v, k, timestamp, codec, mv))
    }

    def ensureValid(magicValue: Byte, timestamp: Long): Long =
      if (magicValue > Message.MagicValue_V0) timestamp else Message.NoTimestamp
  }

  @Test
  def testFieldValues(): Unit = {
    for(v <- messages) {
      // check payload
      if(v.payload == null) {
        assertTrue(v.message.isNull)
        assertEquals("Payload should be null", null, v.message.payload)
      } else {
        TestUtils.checkEquals(ByteBuffer.wrap(v.payload), v.message.payload)
      }
      // check timestamp
      if (v.magicValue > Message.MagicValue_V0)
        assertEquals("Timestamp should be the same", v.timestamp, v.message.timestamp)
       else
        assertEquals("Timestamp should be the NoTimestamp", Message.NoTimestamp, v.message.timestamp)

      // check magic value
      assertEquals(v.magicValue, v.message.magic)
      // check key
      if(v.message.hasKey)
        TestUtils.checkEquals(ByteBuffer.wrap(v.key), v.message.key)
      else
        assertEquals(null, v.message.key)
      // check compression codec
      assertEquals(v.codec, v.message.compressionCodec)
    }
  }

  @Test
  def testChecksum(): Unit = {
    for(v <- messages) {
      assertTrue("Auto-computed checksum should be valid", v.message.isValid)
      // garble checksum
      val badChecksum: Int = (v.message.checksum + 1 % Int.MaxValue).toInt
      ByteUtils.writeUnsignedInt(v.message.buffer, Message.CrcOffset, badChecksum)
      assertFalse("Message with invalid checksum should be invalid", v.message.isValid)
    }
  }

  @Test
  def testEquality(): Unit = {
    for (v <- messages) {
      assertFalse("Should not equal null", v.message.equals(null))
      assertFalse("Should not equal a random string", v.message.equals("asdf"))
      assertTrue("Should equal itself", v.message.equals(v.message))
      val copy = new Message(bytes = v.payload, key = v.key, v.timestamp, codec = v.codec, v.magicValue)
      assertTrue("Should equal another message with the same content.", v.message.equals(copy))
    }
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testInvalidTimestampAndMagicValueCombination(): Unit = {
      new Message("hello".getBytes, 0L, Message.MagicValue_V0)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testInvalidTimestamp(): Unit = {
    new Message("hello".getBytes, -3L, Message.MagicValue_V1)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testInvalidMagicByte(): Unit = {
    new Message("hello".getBytes, 0L, 2.toByte)
  }

  @Test
  def testIsHashable(): Unit = {
    // this is silly, but why not
    val m = new HashMap[Message, Message]()
    for(v <- messages)
      m.put(v.message, v.message)
    for(v <- messages)
      assertEquals(v.message, m.get(v.message))
  }

  @Test
  def testExceptionMapping(): Unit = {
    val expected = Errors.CORRUPT_MESSAGE
    val actual = Errors.forException(new InvalidMessageException())

    assertEquals("InvalidMessageException should map to a corrupt message error", expected, actual)
  }

}

