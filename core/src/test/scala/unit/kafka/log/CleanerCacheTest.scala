/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.scalatest.junit.JUnitSuite
import org.junit.Assert._
import unit.kafka.log.FakeRecord

class CleanerCacheTest extends JUnitSuite {

  @Test
  def testBasicValidationWithDefaultStrategy() {
    validateMap(10)
    validateMap(100)
    validateMap(1000)
    validateMap(5000)
  }

  @Test
  def testBasicValidationWithTimestampStrategy() {
    validateMap(10, strategy = Constants.TimestampStrategy)
    validateMap(100, strategy = Constants.TimestampStrategy)
    validateMap(1000, strategy = Constants.TimestampStrategy)
    validateMap(5000, strategy = Constants.TimestampStrategy)
  }
  
  @Test
  def testBasicValidationWithHeaderStrategy() {
    validateMap(10, strategy = "version")
    validateMap(100, strategy = "version")
    validateMap(1000, strategy = "version")
    validateMap(5000, strategy = "version")
  }
  
  @Test
  def testClearWithDefaultStrategy() {
    val cache = new SkimpyCleanerCache(4000)
    for (i <- 0 until 10)
      cache.putIfGreater(new FakeRecord(key(i), 100 + i, 200 + i, 300 + i))
    for (i <- 0 until 10) {
      assertEquals(100 + i, cache.offset(key(i)))
      assertEquals(-1L, cache.version(key(i)))
    }
    cache.clear()
    for (i <- 0 until 10) {
      assertEquals(-1L, cache.offset(key(i)))
      assertEquals(-1L, cache.version(key(i)))
    }
  }

  @Test
  def testClearWithTimestampStrategy() {
    val cache = new SkimpyCleanerCache(4000, strategy = Constants.TimestampStrategy)
    for (i <- 0 until 10)
      cache.putIfGreater(new FakeRecord(key(i), 100 + i, 200 + i, 300 + i))
    for (i <- 0 until 10) {
      assertEquals(-1L, cache.offset(key(i)))
      assertEquals(300 + i, cache.version(key(i)))
    }
    cache.clear()
    for (i <- 0 until 10) {
      assertEquals(-1L, cache.offset(key(i)))
      assertEquals(-1L, cache.version(key(i)))
    }
  }

  @Test
  def testClearWithHeaderStrategy() {
    val cache = new SkimpyCleanerCache(4000, strategy = "version")
    for (i <- 0 until 10)
      cache.putIfGreater(new FakeRecord(key(i), 100 + i, 200 + i, 300 + i))
    for (i <- 0 until 10) {
      assertEquals(100 + i, cache.offset(key(i)))
      assertEquals(200 + i, cache.version(key(i)))
    }
    cache.clear()
    for (i <- 0 until 10) {
      assertEquals(-1L, cache.offset(key(i)))
      assertEquals(-1L, cache.version(key(i)))
    }
  }

  @Test
  def testGetWhenFullWithDefaultStrategy() {
    val cache = new SkimpyCleanerCache(4096)
    var i = 37L //any value would do
    while (cache.size < cache.slots) {
      cache.putIfGreater(new FakeRecord(key(i), 100 + i, 200 + i, 300 + i))
      i = i + 1L
    }
    assertEquals(-1L, cache.offset(key(i)))
    assertEquals(-1L, cache.version(key(i)))
    assertEquals(100 + (i - 1), cache.offset(key(i - 1L)))
    assertEquals(-1, cache.version(key(i - 1L)))
  }

  @Test
  def testGetWhenFullWithTimestampStrategy() {
    val cache = new SkimpyCleanerCache(4096, strategy = Constants.TimestampStrategy)
    var i = 37L //any value would do
    while (cache.size < cache.slots) {
      cache.putIfGreater(new FakeRecord(key(i), 100 + i, 200 + i, 300 + i))
      i = i + 1L
    }
    assertEquals(-1L, cache.offset(key(i)))
    assertEquals(-1L, cache.version(key(i)))
    assertEquals(-1L, cache.offset(key(i - 1L)))
    assertEquals(300 + (i - 1), cache.version(key(i - 1L)))
  }

  @Test
  def testGetWhenFullWithHeaderStrategy() {
    val cache = new SkimpyCleanerCache(4096, strategy = "version")
    var i = 37L //any value would do
    while (cache.size < cache.slots) {
      cache.putIfGreater(new FakeRecord(key(i), 100 + i, 200 + i, 300 + i))
      i = i + 1L
    }
    assertEquals(-1L, cache.offset(key(i)))
    assertEquals(-1L, cache.version(key(i)))
    assertEquals(100 + (i - 1), cache.offset(key(i - 1L)))
    assertEquals(200 + (i - 1), cache.version(key(i - 1L)))
  }

  @Test
  def testPutAndGetWithDefaultStrategy() {
    val cache = new SkimpyCleanerCache(4096)
    val size = 16
    for (i <- 1 to size) {
      cache.putIfGreater(new FakeRecord(key(i), 100 + i, 200 + i, 300 + i))
    }
    assertEquals(size, cache.size)
    for (i <- 1 to size) {
      assertEquals(100 + i, cache.offset(key(i)))
      assertEquals(-1L, cache.version(key(i)))
    }
  }

  @Test
  def testPutAndGetWithTimestampStrategy() {
    val cache = new SkimpyCleanerCache(4096, strategy = Constants.TimestampStrategy)
    val size = 16
    for (i <- 1 to size) {
      cache.putIfGreater(new FakeRecord(key(i), 100 + i, 200 + i, 300 + i))
    }
    assertEquals(size, cache.size)
    for (i <- 1 to size) {
      assertEquals(-1L, cache.offset(key(i)))
      assertEquals(300 + i, cache.version(key(i)))
    }
  }

  @Test
  def testPutAndGetWithHeaderStrategy() {
    val cache = new SkimpyCleanerCache(4096, strategy = "version")
    val size = 16
    for (i <- 1 to size) {
      cache.putIfGreater(new FakeRecord(key(i), 100 + i, 200 + i, 300 + i))
    }
    assertEquals(size, cache.size)
    for (i <- 1 to size) {
      assertEquals(100 + i, cache.offset(key(i)))
      assertEquals(200 + i, cache.version(key(i)))
    }
  }

  @Test
  def testPutIfGreaterWithDefaultStrategy() {
    val cache = new SkimpyCleanerCache(4096)

    cache.putIfGreater(new FakeRecord(key(1), 5, 2, 3))
    assertEquals(1, cache.size)
    assertEquals(5, cache.offset(key(1)))
    assertEquals(-1, cache.version(key(1)))

    cache.putIfGreater(new FakeRecord(key(1), 4, 1, 2))
    assertEquals(1, cache.size)
    assertEquals(5, cache.offset(key(1)))
    assertEquals(-1, cache.version(key(1)))

    cache.putIfGreater(new FakeRecord(key(1), 6, 3, 4))
    assertEquals(1, cache.size)
    assertEquals(6, cache.offset(key(1)))
    assertEquals(-1, cache.version(key(1)))
  }

  @Test
  def testPutIfGreaterWithTimestampStrategy() {
    val cache = new SkimpyCleanerCache(4096, strategy = Constants.TimestampStrategy)

    cache.putIfGreater(new FakeRecord(key(1), 1, 2, 3))
    assertEquals(1, cache.size)
    assertEquals(-1, cache.offset(key(1)))
    assertEquals(3, cache.version(key(1)))

    cache.putIfGreater(new FakeRecord(key(1), 1, 1, 2))
    assertEquals(1, cache.size)
    assertEquals(-1, cache.offset(key(1)))
    assertEquals(3, cache.version(key(1)))

    cache.putIfGreater(new FakeRecord(key(1), 1, 3, 4))
    assertEquals(1, cache.size)
    assertEquals(-1, cache.offset(key(1)))
    assertEquals(4, cache.version(key(1)))
  }

  @Test
  def testPutIfGreaterWithHeaderStrategy() {
    val cache = new SkimpyCleanerCache(4096, strategy = "version")

    cache.putIfGreater(new FakeRecord(key(1), 1, 2, 3))
    assertEquals(1, cache.size)
    assertEquals(1, cache.offset(key(1)))
    assertEquals(2, cache.version(key(1)))

    cache.putIfGreater(new FakeRecord(key(1), 1, 1, 2))
    assertEquals(1, cache.size)
    assertEquals(1, cache.offset(key(1)))
    assertEquals(2, cache.version(key(1)))

    cache.putIfGreater(new FakeRecord(key(1), 1, 3, 4))
    assertEquals(1, cache.size)
    assertEquals(1, cache.offset(key(1)))
    assertEquals(3, cache.version(key(1)))
  }

  @Test
  def testPutIfGreaterWithHeaderStrategyAndNoVersion() {
    val cache = new SkimpyCleanerCache(4096, strategy = "version")

    cache.putIfGreater(new FakeRecord(key(1), 2, -1, 3))
    assertEquals(1, cache.size)
    assertEquals(2, cache.offset(key(1)))
    assertEquals(-1, cache.version(key(1)))

    cache.putIfGreater(new FakeRecord(key(1), 1, -1, 2))
    assertEquals(1, cache.size)
    assertEquals(2, cache.offset(key(1)))
    assertEquals(-1, cache.version(key(1)))

    cache.putIfGreater(new FakeRecord(key(1), 3, -1, 4))
    assertEquals(1, cache.size)
    assertEquals(3, cache.offset(key(1)))
    assertEquals(-1, cache.version(key(1)))
  }

  @Test
  def testGreaterWithDefaultStrategy() {
    val cache = new SkimpyCleanerCache(4096)

    cache.putIfGreater(new FakeRecord(key(1), 2, 2, 2))
    assertEquals(false, cache.greater(new FakeRecord(key(1), 1, 3, 3)))
    assertEquals(true, cache.greater(new FakeRecord(key(1), 2, 1, 1)))
    assertEquals(true, cache.greater(new FakeRecord(key(1), 3, 1, 1)))
  }

  @Test
  def testGreaterWithTimestampStrategy() {
    val cache = new SkimpyCleanerCache(4096, strategy = Constants.TimestampStrategy)

    cache.putIfGreater(new FakeRecord(key(1), 2, 2, 2))
    assertEquals(false, cache.greater(new FakeRecord(key(1), 3, 3, 1)))
    assertEquals(true, cache.greater(new FakeRecord(key(1), 1, 1, 3)))
    // check when version is the same as cached value
    assertEquals(true, cache.greater(new FakeRecord(key(1), 1, 3, 2)))
    assertEquals(true, cache.greater(new FakeRecord(key(1), 2, 1, 2)))
    assertEquals(true, cache.greater(new FakeRecord(key(1), 3, 1, 2)))
  }

  @Test
  def testGreaterWithHeaderStrategy() {
    val cache = new SkimpyCleanerCache(4096, strategy = "version")

    cache.putIfGreater(new FakeRecord(key(1), 2, 2, 2))
    assertEquals(false, cache.greater(new FakeRecord(key(1), 3, 1, 3)))
    assertEquals(true, cache.greater(new FakeRecord(key(1), 1, 3, 1)))
    // check when version is the same as cached value
    assertEquals(false, cache.greater(new FakeRecord(key(1), 1, 2, 3)))
    assertEquals(true, cache.greater(new FakeRecord(key(1), 2, 2, 1)))
    assertEquals(true, cache.greater(new FakeRecord(key(1), 3, 2, 1)))
  }

  private def key(key: Long): ByteBuffer = ByteBuffer.wrap(key.toString.getBytes)

  def validateMap(items: Int, loadFactor: Double = 0.5, strategy: String = Constants.OffsetStrategy): SkimpyCleanerCache = {
    val cache = new SkimpyCleanerCache((items / loadFactor * 24).toInt, strategy = strategy)
    for (i <- 0 until items)
      cache.putIfGreater(new FakeRecord(key(i), 100 + i, 200 + i, 300 + i))
    for (i <- 0 until items) {
      strategy match {
        case Constants.OffsetStrategy =>
          assertEquals(100 + i, cache.offset(key(i)))
          assertEquals(-1, cache.version(key(i)))
        case Constants.TimestampStrategy =>
          assertEquals(-1, cache.offset(key(i)))
          assertEquals(300 + i, cache.version(key(i)))
        case _ =>
          assertEquals(100 + i, cache.offset(key(i)))
          assertEquals(200 + i, cache.version(key(i)))
      }
    }
    cache
  }

}

object CleanerCacheTest {
  def main(args: Array[String]) {
    if(args.length != 2) {
      System.err.println("USAGE: java CleanerCacheTest size load")
      Exit.exit(1)
    }
    val test = new CleanerCacheTest()
    val size = args(0).toInt
    val load = args(1).toDouble
    val start = System.nanoTime
    val map = test.validateMap(size, load)
    val ellapsedMs = (System.nanoTime - start) / 1000.0 / 1000.0
    println(map.size + " entries in map of size " + map.slots + " in " + ellapsedMs + " ms")
    println("Collision rate: %.1f%%".format(100*map.collisionRate))
  }
}
