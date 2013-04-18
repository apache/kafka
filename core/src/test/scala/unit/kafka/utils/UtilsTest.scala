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

package kafka.utils

import java.util.Arrays
import java.nio.ByteBuffer
import java.io._
import org.apache.log4j.Logger
import org.scalatest.junit.JUnitSuite
import org.junit.Assert._
import kafka.common.KafkaException
import org.junit.Test


class UtilsTest extends JUnitSuite {
  
  private val logger = Logger.getLogger(classOf[UtilsTest]) 

  @Test
  def testSwallow() {
    Utils.swallow(logger.info, throw new KafkaException("test"))
  }

  @Test
  def testCircularIterator() {
    val l = List(1, 2)
    val itl = Utils.circularIterator(l)
    assertEquals(1, itl.next())
    assertEquals(2, itl.next())
    assertEquals(1, itl.next())
    assertEquals(2, itl.next())
    assertFalse(itl.hasDefiniteSize)

    val s = Set(1, 2)
    val its = Utils.circularIterator(s)
    assertEquals(1, its.next())
    assertEquals(2, its.next())
    assertEquals(1, its.next())
    assertEquals(2, its.next())
    assertEquals(1, its.next())
  }

  @Test
  def testReadBytes() {
    for(testCase <- List("", "a", "abcd")) {
      val bytes = testCase.getBytes
      assertTrue(Arrays.equals(bytes, Utils.readBytes(ByteBuffer.wrap(bytes))))
    }
  }
  
  @Test
  def testReplaceSuffix() {
    assertEquals("blah.foo.text", Utils.replaceSuffix("blah.foo.txt", ".txt", ".text"))
    assertEquals("blah.foo", Utils.replaceSuffix("blah.foo.txt", ".txt", ""))
    assertEquals("txt.txt", Utils.replaceSuffix("txt.txt.txt", ".txt", ""))
    assertEquals("foo.txt", Utils.replaceSuffix("foo", "", ".txt"))
  }
  
  @Test
  def testReadInt() {
    val values = Array(0, 1, -1, Byte.MaxValue, Short.MaxValue, 2 * Short.MaxValue, Int.MaxValue/2, Int.MinValue/2, Int.MaxValue, Int.MinValue, Int.MaxValue)
    val buffer = ByteBuffer.allocate(4 * values.size)
    for(i <- 0 until values.length) {
      buffer.putInt(i*4, values(i))
      assertEquals("Written value should match read value.", values(i), Utils.readInt(buffer.array, i*4))
    }

  }

  @Test
  def testCsvList() {
    val emptyString:String = ""
    val nullString:String = null
    val emptyList = Utils.parseCsvList(emptyString)
    val emptyListFromNullString = Utils.parseCsvList(nullString)
    val emptyStringList = Seq.empty[String]
    assertTrue(emptyList!=null)
    assertTrue(emptyListFromNullString!=null)
    assertTrue(emptyStringList.equals(emptyListFromNullString))
    assertTrue(emptyStringList.equals(emptyList))
  }
}
