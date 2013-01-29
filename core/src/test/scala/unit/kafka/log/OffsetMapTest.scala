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
import junit.framework.Assert._

class OffsetMapTest extends JUnitSuite {
  
  @Test
  def testBasicValidation() {
    validateMap(10)
    validateMap(100)
    validateMap(1000)
  }
  
  @Test
  def testClear() {
    val map = new SkimpyOffsetMap(4000, 0.75)
    for(i <- 0 until 10)
      map.put(key(i), i)
    for(i <- 0 until 10)
      assertEquals(i.toLong, map.get(key(i)))
    map.clear()
    for(i <- 0 until 10)
      assertEquals(map.get(key(i)), -1L)
  }
  
  @Test
  def testCapacity() {
    val map = new SkimpyOffsetMap(1024, 0.75)
    var i = 0
    while(map.size < map.capacity) {
      map.put(key(i), i)
      i += 1
    }
    // now the map is full, it should throw an exception
    intercept[IllegalStateException] {
      map.put(key(i), i)
    }
  }
  
  def key(key: Int) = ByteBuffer.wrap(key.toString.getBytes)
  
  def validateMap(items: Int) {
    val map = new SkimpyOffsetMap(items * 2 * 24, 0.75)
    for(i <- 0 until items)
      map.put(key(i), i)
    var misses = 0
    for(i <- 0 until items) {
      map.get(key(i)) match {
        case -1L => misses += 1
        case offset => assertEquals(i.toLong, offset) 
      }
    }
    println("Miss rate: " + (misses.toDouble / items))
  }
  
}

object OffsetMapTest {
  def main(args: Array[String]) {
    if(args.length != 1) {
      System.err.println("USAGE: java OffsetMapTest size")
      System.exit(1)
    }
    val test = new OffsetMapTest()
    test.validateMap(args(0).toInt)
  }
}