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
import junit.framework.Assert._
import java.util.{Collections, Arrays}
import org.junit._
import org.scalatest.junit.JUnitSuite
import scala.collection._
import scala.util.Random
import kafka.utils._

class OffsetIndexTest extends JUnitSuite {
  
  var idx: OffsetIndex = null
  val maxEntries = 30
  
  @Before
  def setup() {
    this.idx = new OffsetIndex(file = nonExistantTempFile(), baseOffset = 45L, mutable = true, maxIndexSize = 30 * 8)
  }
  
  @After
  def teardown() {
    if(this.idx != null)
      this.idx.file.delete()
  }

  @Test
  def randomLookupTest() {
    assertEquals("Not present value should return physical offset 0.", OffsetPosition(idx.baseOffset, 0), idx.lookup(92L))
    
    // append some random values
    val base = idx.baseOffset.toInt + 1
    val size = idx.maxEntries
    val vals: Seq[(Long, Int)] = monotonicSeq(base, size).map(_.toLong).zip(monotonicSeq(0, size))
    vals.foreach{x => idx.append(x._1, x._2)}
    
    // should be able to find all those values
    for((logical, physical) <- vals)
      assertEquals("Should be able to find values that are present.", OffsetPosition(logical, physical), idx.lookup(logical))
      
    // for non-present values we should find the offset of the largest value less than or equal to this 
    val valMap = new immutable.TreeMap[Long, (Long, Int)]() ++ vals.map(p => (p._1, p))
    val offsets = (idx.baseOffset until vals.last._1.toInt).toArray
    Collections.shuffle(Arrays.asList(offsets))
    for(offset <- offsets.take(30)) {
      val rightAnswer = 
        if(offset < valMap.firstKey)
          OffsetPosition(idx.baseOffset, 0)
        else
          OffsetPosition(valMap.to(offset).last._1, valMap.to(offset).last._2._2)
      assertEquals("The index should give the same answer as the sorted map", rightAnswer, idx.lookup(offset))
    }
  }
  
  @Test
  def lookupExtremeCases() {
    assertEquals("Lookup on empty file", OffsetPosition(idx.baseOffset, 0), idx.lookup(idx.baseOffset))
    for(i <- 0 until idx.maxEntries)
      idx.append(idx.baseOffset + i + 1, i)
    // check first and last entry
    assertEquals(OffsetPosition(idx.baseOffset, 0), idx.lookup(idx.baseOffset))
    assertEquals(OffsetPosition(idx.baseOffset + idx.maxEntries, idx.maxEntries - 1), idx.lookup(idx.baseOffset + idx.maxEntries))
  }
  
  @Test
  def appendTooMany() {
    for(i <- 0 until idx.maxEntries) {
      val offset = idx.baseOffset + i + 1
      idx.append(offset, i)
    }
    assertWriteFails("Append should fail on a full index", idx, idx.maxEntries + 1, classOf[IllegalStateException])
  }

  
  @Test
  def testReadOnly() {
    /* add some random values */
    val vals = List((49, 1), (52, 2), (55, 3))
    for((logical, physical) <- vals)
      idx.append(logical, physical)
    
    idx.makeReadOnly()
    
    assertEquals("File length should just contain added entries.", vals.size * 8L, idx.file.length())
    assertEquals("Last offset field should be initialized", vals.last._1, idx.lastOffset)
    
    for((logical, physical) <- vals)
    	assertEquals("Should still be able to find everything.", OffsetPosition(logical, physical), idx.lookup(logical))
    	
    assertWriteFails("Append should fail on read-only index", idx, 60, classOf[IllegalStateException])
  }
  
  @Test(expected = classOf[IllegalArgumentException])
  def appendOutOfOrder() {
    idx.append(51, 0)
    idx.append(50, 1)
  }
  
  @Test
  def reopenAsReadonly() {
    val first = OffsetPosition(51, 0)
    val sec = OffsetPosition(52, 1)
    idx.append(first.offset, first.position)
    idx.append(sec.offset, sec.position)
    idx.close()
    val idxRo = new OffsetIndex(file = idx.file, baseOffset = idx.baseOffset, mutable = false)
    assertEquals(first, idxRo.lookup(first.offset))
    assertEquals(sec, idxRo.lookup(sec.offset))
    assertWriteFails("Append should fail on read-only index", idxRo, 53, classOf[IllegalStateException])
  }
  
  @Test
  def truncate() {
	val idx = new OffsetIndex(file = nonExistantTempFile(), baseOffset = 0L, mutable = true, maxIndexSize = 10 * 8)
    for(i <- 1 until 10)
      idx.append(i, i)
      
    idx.truncateTo(12)
    assertEquals("Index should be unchanged by truncate past the end", OffsetPosition(9, 9), idx.lookup(10))
    idx.truncateTo(10)
    assertEquals("Index should be unchanged by truncate at the end", OffsetPosition(9, 9), idx.lookup(10))
    idx.truncateTo(9)
    assertEquals("Index should truncate off last entry", OffsetPosition(8, 8), idx.lookup(10))
    idx.truncateTo(5)
    assertEquals("4 should be the last entry in the index", OffsetPosition(4, 4), idx.lookup(10))
    assertEquals("4 should be the last entry in the index", 4, idx.lastOffset)
    
    idx.truncate()
    assertEquals("Full truncation should leave no entries", 0, idx.entries())
  }
  
  def assertWriteFails[T](message: String, idx: OffsetIndex, offset: Int, klass: Class[T]) {
    try {
      idx.append(offset, 1)
      fail(message)
    } catch {
      case e: Exception => assertEquals("Got an unexpected exception.", klass, e.getClass)
    }
  }
  
  def makeIndex(baseOffset: Long, mutable: Boolean, vals: Seq[(Long, Int)]): OffsetIndex = {
    val idx = new OffsetIndex(file = nonExistantTempFile(), baseOffset = baseOffset, mutable = mutable, maxIndexSize = 2 * vals.size * 8)
    for ((logical, physical) <- vals)
      idx.append(logical, physical)
    idx
  }

  def monotonicSeq(base: Int, len: Int): Seq[Int] = {
    val rand = new Random(1L)
    val vals = new mutable.ArrayBuffer[Int](len)
    var last = base
    for (i <- 0 until len) {
      last += rand.nextInt(15) + 1
      vals += last
    }
    vals
  }
  
  def nonExistantTempFile(): File = {
    val file = TestUtils.tempFile()
    file.delete()
    file
  }
}