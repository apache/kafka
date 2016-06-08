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
import org.junit._
import org.scalatest.junit.JUnitSuite
import org.junit.Assert._

class OffsetMapTest extends JUnitSuite {
  
  @Test
  def testBasicValidation() {
    validateMap(10)
    validateMap(100)
    validateMap(1000)
    validateMap(5000)
  }
  
  @Test
  def testClear() {
    val map = new SkimpyOffsetMap(4000)
    for(i <- 0 until 10)
      map.put(key(i), i)
    for(i <- 0 until 10)
      assertEquals(i.toLong, map.get(key(i)))
    map.clear()
    for(i <- 0 until 10)
      assertEquals(map.get(key(i)), -1L)
  }
  
  @Test
  def testGetWhenFull() {
    val map = new SkimpyOffsetMap(4096)
    var i = 37L  //any value would do
    while (map.size < map.slots) {
      map.put(key(i), i)
      i = i + 1L
    }
    assertEquals(map.get(key(i)), -1L)
    assertEquals(map.get(key(i-1L)), i-1L)
  }

  def key(key: Long) = ByteBuffer.wrap(key.toString.getBytes)
  
  def validateMap(items: Int, loadFactor: Double = 0.5): SkimpyOffsetMap = {
    val map = new SkimpyOffsetMap((items/loadFactor * 24).toInt)
    for(i <- 0 until items)
      map.put(key(i), i)
    var misses = 0
    for(i <- 0 until items)
      assertEquals(map.get(key(i)), i.toLong)
    map
  }
  
}

object OffsetMapTest {
  def main(args: Array[String]) {
    if(args.length != 2) {
      System.err.println("USAGE: java OffsetMapTest size load")
      System.exit(1)
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