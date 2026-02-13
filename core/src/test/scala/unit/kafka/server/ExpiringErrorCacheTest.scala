/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import org.apache.kafka.server.util.MockTime
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{BeforeEach, Test}
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Random
import java.util.concurrent.{CountDownLatch, TimeUnit}

class ExpiringErrorCacheTest {

  private var mockTime: MockTime = _
  private var cache: ExpiringErrorCache = _
  
  @BeforeEach
  def setUp(): Unit = {
    mockTime = new MockTime()
  }

  // Basic Functionality Tests
  
  @Test
  def testPutAndGet(): Unit = {
    cache = new ExpiringErrorCache(10, mockTime)
    
    cache.put("topic1", "error1", 1000L)
    cache.put("topic2", "error2", 2000L)
    
    val errors = cache.getErrorsForTopics(Set("topic1", "topic2"), mockTime.milliseconds())
    assertEquals(2, errors.size)
    assertEquals("error1", errors("topic1"))
    assertEquals("error2", errors("topic2"))
  }
  
  @Test
  def testGetNonExistentTopic(): Unit = {
    cache = new ExpiringErrorCache(10, mockTime)
    
    cache.put("topic1", "error1", 1000L)
    
    val errors = cache.getErrorsForTopics(Set("topic1", "topic2"), mockTime.milliseconds())
    assertEquals(1, errors.size)
    assertEquals("error1", errors("topic1"))
    assertFalse(errors.contains("topic2"))
  }
  
  @Test
  def testUpdateExistingEntry(): Unit = {
    cache = new ExpiringErrorCache(10, mockTime)
    
    cache.put("topic1", "error1", 1000L)
    assertEquals("error1", cache.getErrorsForTopics(Set("topic1"), mockTime.milliseconds())("topic1"))
    
    // Update with new error
    cache.put("topic1", "error2", 2000L)
    assertEquals("error2", cache.getErrorsForTopics(Set("topic1"), mockTime.milliseconds())("topic1"))
  }
  
  @Test
  def testGetMultipleTopics(): Unit = {
    cache = new ExpiringErrorCache(10, mockTime)
    
    cache.put("topic1", "error1", 1000L)
    cache.put("topic2", "error2", 1000L)
    cache.put("topic3", "error3", 1000L)
    
    val errors = cache.getErrorsForTopics(Set("topic1", "topic3", "topic4"), mockTime.milliseconds())
    assertEquals(2, errors.size)
    assertEquals("error1", errors("topic1"))
    assertEquals("error3", errors("topic3"))
    assertFalse(errors.contains("topic2"))
    assertFalse(errors.contains("topic4"))
  }

  // Expiration Tests
  
  @Test
  def testExpiredEntryNotReturned(): Unit = {
    cache = new ExpiringErrorCache(10, mockTime)
    
    cache.put("topic1", "error1", 1000L)
    
    // Entry should be available before expiration
    assertEquals(1, cache.getErrorsForTopics(Set("topic1"), mockTime.milliseconds()).size)
    
    // Advance time past expiration
    mockTime.sleep(1001L)
    
    // Entry should not be returned after expiration
    assertTrue(cache.getErrorsForTopics(Set("topic1"), mockTime.milliseconds()).isEmpty)
  }
  
  @Test
  def testExpiredEntriesCleanedOnPut(): Unit = {
    cache = new ExpiringErrorCache(10, mockTime)
    
    // Add entries with different TTLs
    cache.put("topic1", "error1", 1000L)
    cache.put("topic2", "error2", 2000L)
    
    // Advance time to expire topic1 but not topic2
    mockTime.sleep(1500L)
    
    // Add a new entry - this should trigger cleanup
    cache.put("topic3", "error3", 1000L)
    
    // Verify only non-expired entries remain
    val errors = cache.getErrorsForTopics(Set("topic1", "topic2", "topic3"), mockTime.milliseconds())
    assertEquals(2, errors.size)
    assertFalse(errors.contains("topic1"))
    assertEquals("error2", errors("topic2"))
    assertEquals("error3", errors("topic3"))
  }
  
  @Test
  def testMixedExpiredAndValidEntries(): Unit = {
    cache = new ExpiringErrorCache(10, mockTime)
    
    cache.put("topic1", "error1", 500L)
    cache.put("topic2", "error2", 1000L)
    cache.put("topic3", "error3", 1500L)
    
    // Advance time to expire only topic1
    mockTime.sleep(600L)
    
    val errors = cache.getErrorsForTopics(Set("topic1", "topic2", "topic3"), mockTime.milliseconds())
    assertEquals(2, errors.size)
    assertFalse(errors.contains("topic1"))
    assertTrue(errors.contains("topic2"))
    assertTrue(errors.contains("topic3"))
  }

  // Capacity Enforcement Tests
  
  @Test
  def testCapacityEnforcement(): Unit = {
    cache = new ExpiringErrorCache(3, mockTime)
    
    // Add 5 entries, exceeding capacity of 3
    for (i <- 1 to 5) {
      cache.put(s"topic$i", s"error$i", 1000L)
      // Small time advance between entries to ensure different insertion order
      mockTime.sleep(10L)
    }
    
    val errors = cache.getErrorsForTopics((1 to 5).map(i => s"topic$i").toSet, mockTime.milliseconds())
    assertEquals(3, errors.size)
    
    // The cache evicts by earliest expiration time
    // Since all have same TTL, earliest inserted (topic1, topic2) should be evicted
    assertFalse(errors.contains("topic1"))
    assertFalse(errors.contains("topic2"))
    assertTrue(errors.contains("topic3"))
    assertTrue(errors.contains("topic4"))
    assertTrue(errors.contains("topic5"))
  }
  
  @Test
  def testEvictionOrder(): Unit = {
    cache = new ExpiringErrorCache(3, mockTime)
    
    // Add entries with different TTLs
    cache.put("topic1", "error1", 3000L) // Expires at 3000
    mockTime.sleep(100L)
    cache.put("topic2", "error2", 1000L) // Expires at 1100
    mockTime.sleep(100L)
    cache.put("topic3", "error3", 2000L) // Expires at 2200
    mockTime.sleep(100L)
    cache.put("topic4", "error4", 500L)  // Expires at 800
    
    // With capacity 3, topic4 (earliest expiration) should be evicted
    val errors = cache.getErrorsForTopics(Set("topic1", "topic2", "topic3", "topic4"), mockTime.milliseconds())
    assertEquals(3, errors.size)
    assertTrue(errors.contains("topic1"))
    assertTrue(errors.contains("topic2"))
    assertTrue(errors.contains("topic3"))
    assertFalse(errors.contains("topic4"))
  }
  
  @Test
  def testCapacityWithDifferentTTLs(): Unit = {
    cache = new ExpiringErrorCache(2, mockTime)
    
    cache.put("topic1", "error1", 5000L) // Long TTL
    cache.put("topic2", "error2", 100L)  // Short TTL
    cache.put("topic3", "error3", 3000L) // Medium TTL
    
    // topic2 has earliest expiration, so it should be evicted
    val errors = cache.getErrorsForTopics(Set("topic1", "topic2", "topic3"), mockTime.milliseconds())
    assertEquals(2, errors.size)
    assertTrue(errors.contains("topic1"))
    assertFalse(errors.contains("topic2"))
    assertTrue(errors.contains("topic3"))
  }

  // Update and Stale Entry Tests
  
  @Test
  def testUpdateDoesNotLeaveStaleEntries(): Unit = {
    cache = new ExpiringErrorCache(3, mockTime)
    
    // Fill cache to capacity
    cache.put("topic1", "error1", 1000L)
    cache.put("topic2", "error2", 1000L)
    cache.put("topic3", "error3", 1000L)
    
    // Update topic2 with longer TTL
    cache.put("topic2", "error2_updated", 5000L)
    
    // Add new entry to trigger eviction
    cache.put("topic4", "error4", 1000L)
    
    // Should evict topic1 or topic3 (earliest expiration), not the updated topic2
    val errors = cache.getErrorsForTopics(Set("topic1", "topic2", "topic3", "topic4"), mockTime.milliseconds())
    assertEquals(3, errors.size)
    assertTrue(errors.contains("topic2"))
    assertEquals("error2_updated", errors("topic2"))
  }
  
  @Test
  def testStaleEntriesInQueueHandledCorrectly(): Unit = {
    cache = new ExpiringErrorCache(10, mockTime)
    
    // Add and update same topic multiple times
    cache.put("topic1", "error1", 1000L)
    cache.put("topic1", "error2", 2000L)
    cache.put("topic1", "error3", 3000L)
    
    // Only latest value should be returned
    val errors = cache.getErrorsForTopics(Set("topic1"), mockTime.milliseconds())
    assertEquals(1, errors.size)
    assertEquals("error3", errors("topic1"))
    
    // Advance time to expire first two entries
    mockTime.sleep(2500L)
    
    // Force cleanup by adding new entry
    cache.put("topic2", "error_new", 1000L)
    
    // topic1 should still be available with latest value
    val errorsAfterCleanup = cache.getErrorsForTopics(Set("topic1"), mockTime.milliseconds())
    assertEquals(1, errorsAfterCleanup.size)
    assertEquals("error3", errorsAfterCleanup("topic1"))
  }

  // Edge Cases
  
  @Test
  def testEmptyCache(): Unit = {
    cache = new ExpiringErrorCache(10, mockTime)
    
    val errors = cache.getErrorsForTopics(Set("topic1", "topic2"), mockTime.milliseconds())
    assertTrue(errors.isEmpty)
  }
  
  @Test
  def testSingleEntryCache(): Unit = {
    cache = new ExpiringErrorCache(1, mockTime)
    
    cache.put("topic1", "error1", 1000L)
    cache.put("topic2", "error2", 1000L)
    
    // Only most recent should remain
    val errors = cache.getErrorsForTopics(Set("topic1", "topic2"), mockTime.milliseconds())
    assertEquals(1, errors.size)
    assertFalse(errors.contains("topic1"))
    assertTrue(errors.contains("topic2"))
  }
  
  @Test
  def testZeroTTL(): Unit = {
    cache = new ExpiringErrorCache(10, mockTime)
    
    cache.put("topic1", "error1", 0L)
    
    // Entry expires immediately
    assertTrue(cache.getErrorsForTopics(Set("topic1"), mockTime.milliseconds()).isEmpty)
  }
  
  @Test
  def testClearOperation(): Unit = {
    cache = new ExpiringErrorCache(10, mockTime)
    
    cache.put("topic1", "error1", 1000L)
    cache.put("topic2", "error2", 1000L)
    
    assertEquals(2, cache.getErrorsForTopics(Set("topic1", "topic2"), mockTime.milliseconds()).size)
    
    cache.clear()
    
    assertTrue(cache.getErrorsForTopics(Set("topic1", "topic2"), mockTime.milliseconds()).isEmpty)
  }

  // Concurrent Access Tests
  
  @Test
  def testConcurrentPutOperations(): Unit = {
    cache = new ExpiringErrorCache(100, mockTime)
    val numThreads = 10
    val numTopicsPerThread = 20
    val latch = new CountDownLatch(numThreads)
    
    (1 to numThreads).foreach { threadId =>
      Future {
        try {
          for (i <- 1 to numTopicsPerThread) {
            cache.put(s"topic_${threadId}_$i", s"error_${threadId}_$i", 1000L)
          }
        } finally {
          latch.countDown()
        }
      }
    }
    
    assertTrue(latch.await(5, TimeUnit.SECONDS))
    
    // Verify all entries were added
    val allTopics = (1 to numThreads).flatMap { threadId =>
      (1 to numTopicsPerThread).map(i => s"topic_${threadId}_$i")
    }.toSet
    
    val errors = cache.getErrorsForTopics(allTopics, mockTime.milliseconds())
    assertEquals(100, errors.size) // Limited by cache capacity
  }
  
  @Test
  def testConcurrentPutAndGet(): Unit = {
    cache = new ExpiringErrorCache(100, mockTime)
    val numOperations = 1000
    val random = new Random()
    val topics = (1 to 50).map(i => s"topic$i").toArray
    
    val futures = (1 to numOperations).map { _ =>
      Future {
        if (random.nextBoolean()) {
          // Put operation
          val topic = topics(random.nextInt(topics.length))
          cache.put(topic, s"error_${random.nextInt()}", 1000L)
        } else {
          // Get operation
          val topicsToGet = Set(topics(random.nextInt(topics.length)))
          cache.getErrorsForTopics(topicsToGet, mockTime.milliseconds())
        }
      }
    }
    
    // Wait for all operations to complete
    Future.sequence(futures).map(_ => ())
  }
  
  @Test
  def testConcurrentUpdates(): Unit = {
    cache = new ExpiringErrorCache(50, mockTime)
    val numThreads = 10
    val numUpdatesPerThread = 100
    val sharedTopics = (1 to 10).map(i => s"shared_topic$i").toArray
    val latch = new CountDownLatch(numThreads)
    
    (1 to numThreads).foreach { threadId =>
      Future {
        try {
          val random = new Random()
          for (i <- 1 to numUpdatesPerThread) {
            val topic = sharedTopics(random.nextInt(sharedTopics.length))
            cache.put(topic, s"error_thread${threadId}_update$i", 1000L)
          }
        } finally {
          latch.countDown()
        }
      }
    }
    
    assertTrue(latch.await(5, TimeUnit.SECONDS))
    
    // Verify all shared topics have some value
    val errors = cache.getErrorsForTopics(sharedTopics.toSet, mockTime.milliseconds())
    sharedTopics.foreach { topic =>
      assertTrue(errors.contains(topic), s"Topic $topic should have a value")
      assertTrue(errors(topic).startsWith("error_thread"), s"Value should be from one of the threads")
    }
  }
}