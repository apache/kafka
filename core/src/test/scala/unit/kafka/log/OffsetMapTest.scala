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

import java.nio._

import kafka.utils.Exit
import org.junit._
import org.junit.Assert._

class OffsetMapTest {
  
  @Test
  def testBasicValidation(): Unit = {
    validateMap(10)
    validateMap(10, strategy = "header", headerKey = "sequence")
    validateMap(10, strategy = "timestamp")
    validateMap(100)
    validateMap(100, strategy = "header", headerKey = "sequence")
    validateMap(100, strategy = "timestamp")
    validateMap(1000)
    validateMap(1000, strategy = "header", headerKey = "sequence")
    validateMap(1000, strategy = "timestamp")
    validateMap(5000)
    validateMap(5000, strategy = "header", headerKey = "sequence")
    validateMap(5000, strategy = "timestamp")
  }
  
  @Test
  def testInit(): Unit = {
    val map = new SkimpyOffsetMap(4000)
    map.reinitialize()

    for (i <- 0 until 10)
      map.put(new FakeRecord(key(i), i))

    for (i <- 0 until 10) {
      assertEquals(i.toLong, map.getOffset(key(i)))
      assertEquals(-1L, map.getVersion(key(i)))
      assertEquals(true, map.shouldRetainRecord(new FakeRecord(key(i), i)))
      assertEquals(false, map.shouldRetainRecord(new FakeRecord(key(i), i-1)))
    }

    map.reinitialize(Defaults.CompactionStrategyOffset)
    for (i <- 0 until 10) {
      assertEquals(map.getOffset(key(i)), -1L)
      assertEquals(map.getVersion(key(i)), -1L)
    }
  }
  
  @Test
  def testGetWhenFull(): Unit = {
    val map = new SkimpyOffsetMap(4096)
    map.reinitialize(Defaults.CompactionStrategyOffset)
    var i = 37L  //any value would do
    while (map.size < map.slots) {
      map.put(new FakeRecord(key(i), i))
      i = i + 1L
    }

    assertEquals(map.getOffset(key(i)), -1L)
    assertEquals(map.getVersion(key(i)), -1L)
    assertEquals(map.getOffset(key(i-1L)), i-1L)
    assertEquals(map.getVersion(key(i-1L)), -1L)
    assertNotEquals(map.latestOffset, i)
    assertEquals(true, map.shouldRetainRecord(new FakeRecord(key(i-1L), i-1L)))
  }
  
  @Test
  def testTimestampLogCompaction(): Unit = {
    val map = new SkimpyOffsetMap(4096)
    map.reinitialize("timestamp")

    // put offset keys from 0 - 9 with corresponding offset 0-9 respectively
    val time = System.currentTimeMillis()
    for (i <- 0 until 10)
      map.put(new FakeRecord(key(i), i, fakeTimestamp = time+i))

    // map should hold offset 0 for key 0
    assertEquals(0, map.getOffset(key(0)))
    assertTrue(map.getVersion(key(0)) > 0)
    // latest offset in map should be 9
    assertEquals(9, map.latestOffset)
    // size should 10
    assertEquals(10, map.size)

    // putting latest value for key=0 first followed by old value
    val fr1 = new FakeRecord(key(0), 10, fakeTimestamp = time+10)
    map.put(fr1)
    val fr2 = new FakeRecord(key(0), 11, fakeTimestamp = time-1000)
    map.put(fr2)

    // map should hold offset 10 for key 0
    assertEquals(10, map.getOffset(key(0)))
    assertEquals(fr1.timestamp, map.getVersion(key(0)))
    assertEquals(true, map.shouldRetainRecord(fr1))
    assertEquals(false, map.shouldRetainRecord(fr2))

    // latest offset in map should be 11
    assertEquals(11, map.latestOffset)
    // size should 10
    assertEquals(10, map.size)

    map.put(new FakeRecord(key(10), 12, fakeTimestamp = time+12))

    // latest offset in map should be 12
    assertEquals(12, map.latestOffset)
    // size should 11
    assertEquals(11, map.size)
  }
  
  @Test
  def testSequenceLogCompaction(): Unit = {
    val map = new SkimpyOffsetMap(4096)
    map.reinitialize("header", "sequence")

    // put offset keys from 0 - 9 with corresponding offset 0-9 respectively
    for (i <- 0 until 10)
      map.put(new FakeRecord(key(i), i, 100+i))

    // putting latest value for key=0 first followed by old value
    map.put(new FakeRecord(key(0), 10, 111))
    map.put(new FakeRecord(key(0), 11, 110))

    // map should hold offset 10 for key 0
    assertEquals(10, map.getOffset(key(0)))
    assertEquals(111, map.getVersion(key(0)))
    assertEquals(true, map.shouldRetainRecord(new FakeRecord(key(0), 10, 111)))
    // latest offset in map should be 11
    assertEquals(11, map.latestOffset)
    // size should 10
    assertEquals(10, map.size)

    // putting latest value for key=1 first followed by old value
    map.put(new FakeRecord(key(1), 12, 112))
    map.put(new FakeRecord(key(1), 13, 114))
    map.put(new FakeRecord(key(1), 14, 115))
    map.put(new FakeRecord(key(1), 15, 113))
    map.put(new FakeRecord(key(1), 16, 120))
    map.put(new FakeRecord(key(1), 17, 119))

    // map should hold offset 16 for key 1
    assertEquals(16, map.getOffset(key(1)))
    assertEquals(120, map.getVersion(key(1)))
    assertEquals(true, map.shouldRetainRecord(new FakeRecord(key(1), 16, 120)))
    assertEquals(false, map.shouldRetainRecord(new FakeRecord(key(1), 17, 119)))
    // latest offset in map should be 17
    assertEquals(17, map.latestOffset)
    // size should 10
    assertEquals(10, map.size)

    map.put(new FakeRecord(key(1), 18, 118))
    map.put(new FakeRecord(key(1), 19, 121))
    map.put(new FakeRecord(key(1), 20, 116))
    map.put(new FakeRecord(key(1), 21, 117))

    // map should hold offset 19 for key 1
    assertEquals(19, map.getOffset(key(1)))
    assertEquals(121, map.getVersion(key(1)))
    assertEquals(true, map.shouldRetainRecord(new FakeRecord(key(1), 19, 121)))
    assertEquals(false, map.shouldRetainRecord(new FakeRecord(key(1), 21, 117)))
    // latest offset in map should be 21
    assertEquals(21, map.latestOffset)
    // size should 10
    assertEquals(10, map.size)

    map.put(new FakeRecord(key(10), 22, 22))

    // latest offset in map should be 22
    assertEquals(22, map.latestOffset)
    // size should 11
    assertEquals(11, map.size)

    // checking negative header value
    map.put(new FakeRecord(key(1), 23, -117))

    // latest offset in map should be 23
    assertEquals(23, map.latestOffset)
    // map should hold offset 19 for key 1
    assertEquals(19, map.getOffset(key(1)))
    assertEquals(121, map.getVersion(key(1)))
    // size should 11
    assertEquals(11, map.size)
  }
  
  @Test
  def testSameOffsetMapInstanceForDifferentCompactionStrategy(): Unit = {
    val map = new SkimpyOffsetMap(4096)

    /*
     * offset based compaction
     */
    map.reinitialize()

    for (i <- 0 until 10)
      map.put(new FakeRecord(key(i), i))

    // map should hold offset 0 for key 0
    assertEquals(0, map.getOffset(key(0)))
    assertEquals(-1L, map.getVersion(key(0)))
    assertEquals(true, map.shouldRetainRecord(new FakeRecord(key(0), 0)))
    // latest offset in map should be 9
    assertEquals(9, map.latestOffset)
    // size should 10
    assertEquals(10, map.size)

    // putting latest value for key=0 first followed by old value
    map.put(new FakeRecord(key(0), 10))
    map.put(new FakeRecord(key(0), 11))

    // map should hold offset 11 for key 0
    assertEquals(11, map.getOffset(key(0)))
    assertEquals(-1L, map.getVersion(key(0)))
    assertEquals(false, map.shouldRetainRecord(new FakeRecord(key(0), 10)))
    assertEquals(true, map.shouldRetainRecord(new FakeRecord(key(0), 11)))
    // latest offset in map should be 11
    assertEquals(11, map.latestOffset)
    // size should 10
    assertEquals(10, map.size)

    /*
     * timestamp based compaction
     */
    map.reinitialize("timestamp")

    // put offset keys from 0 - 9 with corresponding offset 0-9 respectively
    val time = System.currentTimeMillis()
    for (i <- 0 until 10)
      map.put(new FakeRecord(key(i), i, fakeTimestamp = time+i))

    // map should hold offset 0 for key 0
    assertEquals(0, map.getOffset(key(0)))
    assertTrue(map.getVersion(key(0)) > 0)
    // latest offset in map should be 9
    assertEquals(9, map.latestOffset)
    // size should 10
    assertEquals(10, map.size)

    // putting latest value for key=0 first followed by old value
    val fr1 = new FakeRecord(key(0), 10, fakeTimestamp = time+10)
    map.put(fr1)
    val fr2 = new FakeRecord(key(0), 11, fakeTimestamp = time-1000)
    map.put(fr2)

    // map should hold offset 10 for key 0
    assertEquals(10, map.getOffset(key(0)))
    assertEquals(fr1.timestamp, map.getVersion(key(0)))
    assertEquals(true, map.shouldRetainRecord(fr1))
    assertEquals(false, map.shouldRetainRecord(fr2))
    // latest offset in map should be 11
    assertEquals(11, map.latestOffset)
    // size should 10
    assertEquals(10, map.size)

    /* 
     * header sequence based compaction
     */
    map.reinitialize("header", "sequence")

    // put offset keys from 0 - 9 with corresponding offset 0-9 respectively
    for (i <- 0 until 10)
      map.put(new FakeRecord(key(i), i, 100+i))

    // putting latest value for key=0 first followed by old value
    map.put(new FakeRecord(key(0), 10, 111))
    map.put(new FakeRecord(key(0), 11, 110))

    // map should hold offset 10 for key 0
    assertEquals(10, map.getOffset(key(0)))
    assertEquals(111, map.getVersion(key(0)))
    assertEquals(true, map.shouldRetainRecord(new FakeRecord(key(0), 10, 111)))
    assertEquals(false, map.shouldRetainRecord(new FakeRecord(key(0), 11, 110)))
    // latest offset in map should be 11
    assertEquals(11, map.latestOffset)
    // size should 10
    assertEquals(10, map.size)
  }

  def key(key: Long) = ByteBuffer.wrap(key.toString.getBytes)
  
  def validateMap(items: Int, loadFactor: Double = 0.5, strategy: String = Defaults.CompactionStrategyOffset, headerKey: String = ""): SkimpyOffsetMap = {
    val map = new SkimpyOffsetMap((items/loadFactor * 24).toInt)
    map.reinitialize(strategy, headerKey)

    val isOffsetStrategy = Defaults.CompactionStrategyOffset.equalsIgnoreCase(strategy)
    val isTimestampStrategy = Defaults.CompactionStrategyTimestamp.equalsIgnoreCase(strategy)
    val currentTimeMillis = System.currentTimeMillis()
    for (i <- 1 until items) {
      if (isOffsetStrategy)
        map.put(new FakeRecord(key(i), i))
      else if (isTimestampStrategy)
        map.put(new FakeRecord(key(i), i, fakeTimestamp = (currentTimeMillis + i)))
      else
        map.put(new FakeRecord(key(i), i, items+i))
    }

    for (i <- 1 until items) {
      assertEquals(i.toLong, map.getOffset(key(i)))
      if (isOffsetStrategy) {
        assertEquals(-1L, map.getVersion(key(i)))
        assertEquals(true, map.shouldRetainRecord(new FakeRecord(key(i), i)))
        assertEquals(false, map.shouldRetainRecord(new FakeRecord(key(i), i-1)))
      } else if (isTimestampStrategy) {
        assertEquals(currentTimeMillis + i, map.getVersion(key(i)))
        assertEquals(true, map.shouldRetainRecord(new FakeRecord(key(i), i, fakeTimestamp = currentTimeMillis + i)))
        assertEquals(false, map.shouldRetainRecord(new FakeRecord(key(i), i, fakeTimestamp = currentTimeMillis)))
      } else {
        assertEquals(items + i, map.getVersion(key(i)))
        assertEquals(true, map.shouldRetainRecord(new FakeRecord(key(i), i, items + i)))
        assertEquals(false, map.shouldRetainRecord(new FakeRecord(key(i), i, items)))
      }
    }

    map
  }
}

object OffsetMapTest {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.println("USAGE: java OffsetMapTest size load")
      Exit.exit(1)
    }

    val test = new OffsetMapTest()
    val size = args(0).toInt
    val load = args(1).toDouble
    val start = System.nanoTime
    val map = test.validateMap(size, load)
    val ellapsedMs = (System.nanoTime - start) / 1000.0 / 1000.0
    println(s"${map.size} entries in map of size ${map.slots} in $ellapsedMs ms")
    println("Collision rate: %.1f%%".format(100*map.collisionRate))
  }
}
