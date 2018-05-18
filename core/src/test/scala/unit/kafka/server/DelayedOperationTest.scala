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

package kafka.server

import java.util.Random
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock

import kafka.utils.CoreUtils.inLock
import kafka.utils.TestUtils
import org.apache.kafka.common.utils.Time
import org.junit.{After, Before, Test}
import org.junit.Assert._

class DelayedOperationTest {

  var purgatory: DelayedOperationPurgatory[MockDelayedOperation] = null
  var executorService: ExecutorService = null

  @Before
  def setUp() {
    purgatory = DelayedOperationPurgatory[MockDelayedOperation](purgatoryName = "mock")
  }

  @After
  def tearDown() {
    purgatory.shutdown()
    if (executorService != null)
      executorService.shutdown()
  }

  @Test
  def testRequestSatisfaction() {
    val r1 = new MockDelayedOperation(100000L)
    val r2 = new MockDelayedOperation(100000L)
    assertEquals("With no waiting requests, nothing should be satisfied", 0, purgatory.checkAndComplete("test1"))
    assertFalse("r1 not satisfied and hence watched", purgatory.tryCompleteElseWatch(r1, Array("test1")))
    assertEquals("Still nothing satisfied", 0, purgatory.checkAndComplete("test1"))
    assertFalse("r2 not satisfied and hence watched", purgatory.tryCompleteElseWatch(r2, Array("test2")))
    assertEquals("Still nothing satisfied", 0, purgatory.checkAndComplete("test2"))
    r1.completable = true
    assertEquals("r1 satisfied", 1, purgatory.checkAndComplete("test1"))
    assertEquals("Nothing satisfied", 0, purgatory.checkAndComplete("test1"))
    r2.completable = true
    assertEquals("r2 satisfied", 1, purgatory.checkAndComplete("test2"))
    assertEquals("Nothing satisfied", 0, purgatory.checkAndComplete("test2"))
  }

  @Test
  def testRequestExpiry() {
    val expiration = 20L
    val start = Time.SYSTEM.hiResClockMs
    val r1 = new MockDelayedOperation(expiration)
    val r2 = new MockDelayedOperation(200000L)
    assertFalse("r1 not satisfied and hence watched", purgatory.tryCompleteElseWatch(r1, Array("test1")))
    assertFalse("r2 not satisfied and hence watched", purgatory.tryCompleteElseWatch(r2, Array("test2")))
    r1.awaitExpiration()
    val elapsed = Time.SYSTEM.hiResClockMs - start
    assertTrue("r1 completed due to expiration", r1.isCompleted)
    assertFalse("r2 hasn't completed", r2.isCompleted)
    assertTrue(s"Time for expiration $elapsed should at least $expiration", elapsed >= expiration)
  }

  @Test
  def testRequestPurge() {
    val r1 = new MockDelayedOperation(100000L)
    val r2 = new MockDelayedOperation(100000L)
    val r3 = new MockDelayedOperation(100000L)
    purgatory.tryCompleteElseWatch(r1, Array("test1"))
    purgatory.tryCompleteElseWatch(r2, Array("test1", "test2"))
    purgatory.tryCompleteElseWatch(r3, Array("test1", "test2", "test3"))

    assertEquals("Purgatory should have 3 total delayed operations", 3, purgatory.delayed)
    assertEquals("Purgatory should have 6 watched elements", 6, purgatory.watched)

    // complete the operations, it should immediately be purged from the delayed operation
    r2.completable = true
    r2.tryComplete()
    assertEquals("Purgatory should have 2 total delayed operations instead of " + purgatory.delayed, 2, purgatory.delayed)

    r3.completable = true
    r3.tryComplete()
    assertEquals("Purgatory should have 1 total delayed operations instead of " + purgatory.delayed, 1, purgatory.delayed)

    // checking a watch should purge the watch list
    purgatory.checkAndComplete("test1")
    assertEquals("Purgatory should have 4 watched elements instead of " + purgatory.watched, 4, purgatory.watched)

    purgatory.checkAndComplete("test2")
    assertEquals("Purgatory should have 2 watched elements instead of " + purgatory.watched, 2, purgatory.watched)

    purgatory.checkAndComplete("test3")
    assertEquals("Purgatory should have 1 watched elements instead of " + purgatory.watched, 1, purgatory.watched)
  }

  @Test
  def shouldCancelForKeyReturningCancelledOperations() {
    purgatory.tryCompleteElseWatch(new MockDelayedOperation(10000L), Seq("key"))
    purgatory.tryCompleteElseWatch(new MockDelayedOperation(10000L), Seq("key"))
    purgatory.tryCompleteElseWatch(new MockDelayedOperation(10000L), Seq("key2"))

    val cancelledOperations = purgatory.cancelForKey("key")
    assertEquals(2, cancelledOperations.size)
    assertEquals(1, purgatory.delayed)
    assertEquals(1, purgatory.watched)
  }

  @Test
  def shouldReturnNilOperationsOnCancelForKeyWhenKeyDoesntExist() {
    val cancelledOperations = purgatory.cancelForKey("key")
    assertEquals(Nil, cancelledOperations)
  }

  /**
    * Verify that if there is lock contention between two threads attempting to complete,
    * completion is performed without any blocking in either thread.
    */
  @Test
  def testTryCompleteLockContention(): Unit = {
    executorService = Executors.newSingleThreadExecutor()
    val completionAttemptsRemaining = new AtomicInteger(Int.MaxValue)
    val tryCompleteSemaphore = new Semaphore(1)
    val key = "key"

    val op = new MockDelayedOperation(100000L, None, None) {
      override def tryComplete() = {
        val shouldComplete = completionAttemptsRemaining.decrementAndGet <= 0
        tryCompleteSemaphore.acquire()
        try {
          if (shouldComplete)
            forceComplete()
          else
            false
        } finally {
          tryCompleteSemaphore.release()
        }
      }
    }

    purgatory.tryCompleteElseWatch(op, Seq(key))
    completionAttemptsRemaining.set(2)
    tryCompleteSemaphore.acquire()
    val future = runOnAnotherThread(purgatory.checkAndComplete(key), shouldComplete = false)
    TestUtils.waitUntilTrue(() => tryCompleteSemaphore.hasQueuedThreads, "Not attempting to complete")
    purgatory.checkAndComplete(key) // this should not block even though lock is not free
    assertFalse("Operation should not have completed", op.isCompleted)
    tryCompleteSemaphore.release()
    future.get(10, TimeUnit.SECONDS)
    assertTrue("Operation should have completed", op.isCompleted)
  }

  /**
    * Test `tryComplete` with multiple threads to verify that there are no timing windows
    * when completion is not performed even if the thread that makes the operation completable
    * may not be able to acquire the operation lock. Since it is difficult to test all scenarios,
    * this test uses random delays with a large number of threads.
    */
  @Test
  def testTryCompleteWithMultipleThreads(): Unit = {
    val executor = Executors.newScheduledThreadPool(20)
    this.executorService = executor
    val random = new Random
    val maxDelayMs = 10
    val completionAttempts = 20

    class TestDelayOperation(index: Int) extends MockDelayedOperation(10000L) {
      val key = s"key$index"
      val completionAttemptsRemaining = new AtomicInteger(completionAttempts)

      override def tryComplete(): Boolean = {
        val shouldComplete = completable
        Thread.sleep(random.nextInt(maxDelayMs))
        if (shouldComplete)
          forceComplete()
        else
          false
      }
    }
    val ops = (0 until 100).map { index =>
      val op = new TestDelayOperation(index)
      purgatory.tryCompleteElseWatch(op, Seq(op.key))
      op
    }

    def scheduleTryComplete(op: TestDelayOperation, delayMs: Long): Future[_] = {
      executor.schedule(new Runnable {
        override def run(): Unit = {
          if (op.completionAttemptsRemaining.decrementAndGet() == 0)
            op.completable = true
          purgatory.checkAndComplete(op.key)
        }
      }, delayMs, TimeUnit.MILLISECONDS)
    }

    (1 to completionAttempts).flatMap { _ =>
      ops.map { op => scheduleTryComplete(op, random.nextInt(maxDelayMs)) }
    }.foreach { future => future.get }

    ops.foreach { op => assertTrue("Operation should have completed", op.isCompleted) }
  }

  @Test
  def testDelayedOperationLock() {
    verifyDelayedOperationLock(new MockDelayedOperation(100000L), mismatchedLocks = false)
  }

  @Test
  def testDelayedOperationLockOverride() {
    def newMockOperation = {
      val lock = new ReentrantLock
      new MockDelayedOperation(100000L, Some(lock), Some(lock))
    }
    verifyDelayedOperationLock(newMockOperation, mismatchedLocks = false)

    verifyDelayedOperationLock(new MockDelayedOperation(100000L, None, Some(new ReentrantLock)),
        mismatchedLocks = true)
  }

  def verifyDelayedOperationLock(mockDelayedOperation: => MockDelayedOperation, mismatchedLocks: Boolean) {
    val key = "key"
    executorService = Executors.newSingleThreadExecutor
    def createDelayedOperations(count: Int): Seq[MockDelayedOperation] = {
      (1 to count).map { _ =>
        val op = mockDelayedOperation
        purgatory.tryCompleteElseWatch(op, Seq(key))
        assertFalse("Not completable", op.isCompleted)
        op
      }
    }

    def createCompletableOperations(count: Int): Seq[MockDelayedOperation] = {
      (1 to count).map { _ =>
        val op = mockDelayedOperation
        op.completable = true
        op
      }
    }

    def checkAndComplete(completableOps: Seq[MockDelayedOperation], expectedComplete: Seq[MockDelayedOperation]): Unit = {
      completableOps.foreach(op => op.completable = true)
      val completed = purgatory.checkAndComplete(key)
      assertEquals(expectedComplete.size, completed)
      expectedComplete.foreach(op => assertTrue("Should have completed", op.isCompleted))
      val expectedNotComplete = completableOps.toSet -- expectedComplete
      expectedNotComplete.foreach(op => assertFalse("Should not have completed", op.isCompleted))
    }

    // If locks are free all completable operations should complete
    var ops = createDelayedOperations(2)
    checkAndComplete(ops, ops)

    // Lock held by current thread, completable operations should complete
    ops = createDelayedOperations(2)
    inLock(ops(1).lock) {
      checkAndComplete(ops, ops)
    }

    // Lock held by another thread, should not block, only operations that can be
    // locked without blocking on the current thread should complete
    ops = createDelayedOperations(2)
    runOnAnotherThread(ops(0).lock.lock(), true)
    try {
      checkAndComplete(ops, Seq(ops(1)))
    } finally {
      runOnAnotherThread(ops(0).lock.unlock(), true)
      checkAndComplete(Seq(ops(0)), Seq(ops(0)))
    }

    // Lock acquired by response callback held by another thread, should not block
    // if the response lock is used as operation lock, only operations
    // that can be locked without blocking on the current thread should complete
    ops = createDelayedOperations(2)
    ops(0).responseLockOpt.foreach { lock =>
      runOnAnotherThread(lock.lock(), true)
      try {
        try {
          checkAndComplete(ops, Seq(ops(1)))
          assertFalse("Should have failed with mismatched locks", mismatchedLocks)
        } catch {
          case e: IllegalStateException =>
            assertTrue("Should not have failed with valid locks", mismatchedLocks)
        }
      } finally {
        runOnAnotherThread(lock.unlock(), true)
        checkAndComplete(Seq(ops(0)), Seq(ops(0)))
      }
    }

    // Immediately completable operations should complete without locking
    ops = createCompletableOperations(2)
    ops.foreach { op =>
      assertTrue("Should have completed", purgatory.tryCompleteElseWatch(op, Seq(key)))
      assertTrue("Should have completed", op.isCompleted)
    }
  }

  private def runOnAnotherThread(fun: => Unit, shouldComplete: Boolean): Future[_] = {
    val future = executorService.submit(new Runnable {
      def run() = fun
    })
    if (shouldComplete)
      future.get()
    else
      assertFalse("Should not have completed", future.isDone)
    future
  }

  class MockDelayedOperation(delayMs: Long,
                             lockOpt: Option[ReentrantLock] = None,
                             val responseLockOpt: Option[ReentrantLock] = None)
                             extends DelayedOperation(delayMs, lockOpt) {
    var completable = false

    def awaitExpiration() {
      synchronized {
        wait()
      }
    }

    override def tryComplete() = {
      if (completable)
        forceComplete()
      else
        false
    }

    override def onExpiration() {

    }

    override def onComplete() {
      responseLockOpt.foreach { lock =>
        if (!lock.tryLock())
          throw new IllegalStateException("Response callback lock could not be acquired in callback")
      }
      synchronized {
        notify()
      }
    }
  }

}
