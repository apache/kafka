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
import scala.collection._
import junit.framework.Assert._
import org.scalatest.junit.JUnitSuite
import org.junit.{Before, Test}
import kafka.utils.TestUtils
import kafka.utils.Utils

case class MessageTestVal(val key: Array[Byte], 
                          val payload: Array[Byte], 
                          val codec: CompressionCodec, 
                          val message: Message)

class MessageTest extends JUnitSuite {
  
  var messages = new mutable.ArrayBuffer[MessageTestVal]()
  
  @Before
  def setUp(): Unit = {
    val keys = Array(null, "key".getBytes, "".getBytes)
    val vals = Array("value".getBytes, "".getBytes, null)
    val codecs = Array(NoCompressionCodec, GZIPCompressionCodec, SnappyCompressionCodec, LZ4CompressionCodec)
    for(k <- keys; v <- vals; codec <- codecs)
      messages += new MessageTestVal(k, v, codec, new Message(v, k, codec))
  }
  
  @Test
  def testFieldValues {
    for(v <- messages) {
      if(v.payload == null) {
        assertTrue(v.message.isNull)
        assertEquals("Payload should be null", null, v.message.payload)
      } else {
        TestUtils.checkEquals(ByteBuffer.wrap(v.payload), v.message.payload)
      }
      assertEquals(Message.CurrentMagicValue, v.message.magic)
      if(v.message.hasKey)
        TestUtils.checkEquals(ByteBuffer.wrap(v.key), v.message.key)
      else
        assertEquals(null, v.message.key)
      assertEquals(v.codec, v.message.compressionCodec)
    }
  }

  @Test
  def testChecksum() {
    for(v <- messages) {
      assertTrue("Auto-computed checksum should be valid", v.message.isValid)
      // garble checksum
      val badChecksum: Int = (v.message.checksum + 1 % Int.MaxValue).toInt
      Utils.writeUnsignedInt(v.message.buffer, Message.CrcOffset, badChecksum)
      assertFalse("Message with invalid checksum should be invalid", v.message.isValid)
    }
  }
  
  @Test
  def testEquality() {
    for(v <- messages) {
      assertFalse("Should not equal null", v.message.equals(null))
      assertFalse("Should not equal a random string", v.message.equals("asdf"))
      assertTrue("Should equal itself", v.message.equals(v.message))
      val copy = new Message(bytes = v.payload, key = v.key, codec = v.codec)
      assertTrue("Should equal another message with the same content.", v.message.equals(copy))
    }
  }
  
  @Test
  def testIsHashable() {
    // this is silly, but why not
    val m = new HashMap[Message, Message]()
    for(v <- messages)
      m.put(v.message, v.message)
    for(v <- messages)
      assertEquals(v.message, m.get(v.message))
  }
  
}
 	
