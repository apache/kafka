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
import java.nio.file.Files

import org.junit.Assert._
import java.util.{Arrays, Collections}

import org.junit._
import org.scalatest.Assertions.intercept

import scala.collection._
import scala.util.Random
import kafka.utils.TestUtils
import org.apache.kafka.common.errors.InvalidOffsetException

import scala.annotation.nowarn

class OffsetIndexTest {
  
  var idx: OffsetIndex = null
  val maxEntries = 30
  val baseOffset = 45L
  
  @Before
  def setup(): Unit = {
    this.idx = new OffsetIndex(nonExistentTempFile(), baseOffset, maxIndexSize = 30 * 8)
  }
  
  @After
  def teardown(): Unit = {
    if(this.idx != null)
      this.idx.file.delete()
  }

  @nowarn("cat=deprecation")
  @Test
  def randomLookupTest(): Unit = {
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
  def lookupExtremeCases(): Unit = {
    assertEquals("Lookup on empty file", OffsetPosition(idx.baseOffset, 0), idx.lookup(idx.baseOffset))
    for(i <- 0 until idx.maxEntries)
      idx.append(idx.baseOffset + i + 1, i)
    // check first and last entry
    assertEquals(OffsetPosition(idx.baseOffset, 0), idx.lookup(idx.baseOffset))
    assertEquals(OffsetPosition(idx.baseOffset + idx.maxEntries, idx.maxEntries - 1), idx.lookup(idx.baseOffset + idx.maxEntries))
  }

  @Test
  def testEntry(): Unit = {
    for (i <- 0 until idx.maxEntries)
      idx.append(idx.baseOffset + i + 1, i)
    for (i <- 0 until idx.maxEntries)
      assertEquals(OffsetPosition(idx.baseOffset + i + 1, i), idx.entry(i))
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testEntryOverflow(): Unit = {
    idx.entry(0)
  }
  
  @Test
  def appendTooMany(): Unit = {
    for(i <- 0 until idx.maxEntries) {
      val offset = idx.baseOffset + i + 1
      idx.append(offset, i)
    }
    assertWriteFails("Append should fail on a full index", idx, idx.maxEntries + 1, classOf[IllegalArgumentException])
  }
  
  @Test(expected = classOf[InvalidOffsetException])
  def appendOutOfOrder(): Unit = {
    idx.append(51, 0)
    idx.append(50, 1)
  }

  @Test
  def testFetchUpperBoundOffset(): Unit = {
    val first = OffsetPosition(baseOffset + 0, 0)
    val second = OffsetPosition(baseOffset + 1, 10)
    val third = OffsetPosition(baseOffset + 2, 23)
    val fourth = OffsetPosition(baseOffset + 3, 37)

    assertEquals(None, idx.fetchUpperBoundOffset(first, 5))

    for (offsetPosition <- Seq(first, second, third, fourth))
      idx.append(offsetPosition.offset, offsetPosition.position)

    assertEquals(Some(second), idx.fetchUpperBoundOffset(first, 5))
    assertEquals(Some(second), idx.fetchUpperBoundOffset(first, 10))
    assertEquals(Some(third), idx.fetchUpperBoundOffset(first, 23))
    assertEquals(Some(third), idx.fetchUpperBoundOffset(first, 22))
    assertEquals(Some(fourth), idx.fetchUpperBoundOffset(second, 24))
    assertEquals(None, idx.fetchUpperBoundOffset(fourth, 1))
    assertEquals(None, idx.fetchUpperBoundOffset(first, 200))
    assertEquals(None, idx.fetchUpperBoundOffset(second, 200))
  }

  @Test
  def testReopen(): Unit = {
    val first = OffsetPosition(51, 0)
    val sec = OffsetPosition(52, 1)
    idx.append(first.offset, first.position)
    idx.append(sec.offset, sec.position)
    idx.close()
    val idxRo = new OffsetIndex(idx.file, baseOffset = idx.baseOffset)
    assertEquals(first, idxRo.lookup(first.offset))
    assertEquals(sec, idxRo.lookup(sec.offset))
    assertEquals(sec.offset, idxRo.lastOffset)
    assertEquals(2, idxRo.entries)
    assertWriteFails("Append should fail on read-only index", idxRo, 53, classOf[IllegalArgumentException])
  }
  
  @Test
  def truncate(): Unit = {
	val idx = new OffsetIndex(nonExistentTempFile(), baseOffset = 0L, maxIndexSize = 10 * 8)
	idx.truncate()
    for(i <- 1 until 10)
      idx.append(i, i)
      
    // now check the last offset after various truncate points and validate that we can still append to the index.      
    idx.truncateTo(12)
    assertEquals("Index should be unchanged by truncate past the end", OffsetPosition(9, 9), idx.lookup(10))
    assertEquals("9 should be the last entry in the index", 9, idx.lastOffset)
    
    idx.append(10, 10)
    idx.truncateTo(10)
    assertEquals("Index should be unchanged by truncate at the end", OffsetPosition(9, 9), idx.lookup(10))
    assertEquals("9 should be the last entry in the index", 9, idx.lastOffset)
    idx.append(10, 10)
    
    idx.truncateTo(9)
    assertEquals("Index should truncate off last entry", OffsetPosition(8, 8), idx.lookup(10))
    assertEquals("8 should be the last entry in the index", 8, idx.lastOffset)
    idx.append(9, 9)
    
    idx.truncateTo(5)
    assertEquals("4 should be the last entry in the index", OffsetPosition(4, 4), idx.lookup(10))
    assertEquals("4 should be the last entry in the index", 4, idx.lastOffset)
    idx.append(5, 5)
    
    idx.truncate()
    assertEquals("Full truncation should leave no entries", 0, idx.entries)
    idx.append(0, 0)
  }

  @Test
  def forceUnmapTest(): Unit = {
    val idx = new OffsetIndex(nonExistentTempFile(), baseOffset = 0L, maxIndexSize = 10 * 8)
    idx.forceUnmap()
    // mmap should be null after unmap causing lookup to throw a NPE
    intercept[NullPointerException](idx.lookup(1))
  }

  @Test
  def testSanityLastOffsetEqualToBaseOffset(): Unit = {
    // Test index sanity for the case where the last offset appended to the index is equal to the base offset
    val baseOffset = 20L
    val idx = new OffsetIndex(nonExistentTempFile(), baseOffset = baseOffset, maxIndexSize = 10 * 8)
    idx.append(baseOffset, 0)
    idx.sanityCheck()
  }
  
  def assertWriteFails[T](message: String, idx: OffsetIndex, offset: Int, klass: Class[T]): Unit = {
    try {
      idx.append(offset, 1)
      fail(message)
    } catch {
      case e: Exception => assertEquals("Got an unexpected exception.", klass, e.getClass)
    }
  }

  def monotonicSeq(base: Int, len: Int): Seq[Int] = {
    val rand = new Random(1L)
    val vals = new mutable.ArrayBuffer[Int](len)
    var last = base
    for (_ <- 0 until len) {
      last += rand.nextInt(15) + 1
      vals += last
    }
    vals
  }
  
  def nonExistentTempFile(): File = {
    val file = TestUtils.tempFile()
    Files.delete(file.toPath)
    file
  }

}
