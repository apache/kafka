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
    validateMap(10, strategy = "sequence")
    validateMap(10, strategy = "timestamp")
    validateMap(100)
    validateMap(100, strategy = "sequence")
    validateMap(100, strategy = "timestamp")
    validateMap(1000)
    validateMap(1000, strategy = "sequence")
    validateMap(1000, strategy = "timestamp")
    validateMap(5000)
    validateMap(5000, strategy = "sequence")
    validateMap(5000, strategy = "timestamp")
  }
  
  @Test
  def testClear(): Unit = {
    val map = new SkimpyOffsetMap(4000)
    map.init(Defaults.CompactionStrategy)

    for (i <- 0 until 10)
      map.put(new FakeRecord(key(i), i))

    for (i <- 0 until 10)
      assertEquals(i.toLong, map.get(key(i)))

    map.clear()
    for (i <- 0 until 10)
      assertEquals(map.get(key(i)), -1L)
  }
  
  @Test
  def testGetWhenFull(): Unit = {
    val map = new SkimpyOffsetMap(4096)
    map.init(Defaults.CompactionStrategy)
    var i = 37L  //any value would do
    while (map.size < map.slots) {
      map.put(new FakeRecord(key(i), i))
      i = i + 1L
    }

    assertEquals(map.get(key(i)), -1L)
    assertEquals(map.get(key(i-1L)), i-1L)
  }
  
  @Test
  def testSequenceLogCompaction(): Unit = {
    val map = new SkimpyOffsetMap(4096)
    map.init("sequence")

    // put offset keys from 0 - 9 with corresponding offset 0-9 respectively
    for (i <- 0 until 10)
      map.put(new FakeRecord(key(i), i, 100+i))

    // putting latest value for key=0 first followed by old value
    map.put(new FakeRecord(key(0), 10, 111))
    map.put(new FakeRecord(key(0), 11, 110))

    // map should hold offset 10 for key 0
    assertEquals(map.get(key(0)), 10)
    // latest offset in map should be 11
    assertEquals(map.latestOffset, 11)

    // putting latest value for key=1 first followed by old value
    map.put(new FakeRecord(key(1), 12, 112))
    map.put(new FakeRecord(key(1), 13, 114))
    map.put(new FakeRecord(key(1), 14, 115))
    map.put(new FakeRecord(key(1), 15, 113))
    map.put(new FakeRecord(key(1), 16, 120))
    map.put(new FakeRecord(key(1), 17, 119))

    // map should hold offset 16 for key 1
    assertEquals(map.get(key(1)), 16)
    // latest offset in map should be 17
    assertEquals(map.latestOffset, 17)

    map.put(new FakeRecord(key(1), 18, 118))
    map.put(new FakeRecord(key(1), 19, 121))
    map.put(new FakeRecord(key(1), 20, 116))
    map.put(new FakeRecord(key(1), 21, 117))

    // map should hold offset 19 for key 1
    assertEquals(map.get(key(1)), 19)
    // latest offset in map should be 21
    assertEquals(map.latestOffset, 21)
  }

  def key(key: Long) = ByteBuffer.wrap(key.toString.getBytes)
  
  def validateMap(items: Int, loadFactor: Double = 0.5, strategy: String = Defaults.CompactionStrategy): SkimpyOffsetMap = {
    val map = new SkimpyOffsetMap((items/loadFactor * 24).toInt)
    map.init(strategy)

    val isOffsetStrategy = Defaults.CompactionStrategy.equalsIgnoreCase(strategy)
    for (i <- 0 until items) {
      if (isOffsetStrategy)
        map.put(new FakeRecord(key(i), i))
      else
        map.put(new FakeRecord(key(i), i, items+i))
    }

    for (i <- 0 until items)
      assertEquals(map.get(key(i)), i.toLong)

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
    println(map.size + " entries in map of size " + map.slots + " in " + ellapsedMs + " ms")
    println("Collision rate: %.1f%%".format(100*map.collisionRate))
  }
}
