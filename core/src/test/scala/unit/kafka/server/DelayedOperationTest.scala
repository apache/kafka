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
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.api.Assertions._

import scala.jdk.CollectionConverters._

class DelayedOperationTest {

  var purgatory: DelayedOperationPurgatory[DelayedOperation] = _
  var executorService: ExecutorService = _

  @BeforeEach
  def setUp(): Unit = {
    purgatory = DelayedOperationPurgatory[DelayedOperation](purgatoryName = "mock")
  }

  @AfterEach
  def tearDown(): Unit = {
    purgatory.shutdown()
    if (executorService != null)
      executorService.shutdown()
  }

  @Test
  def testLockInTryCompleteElseWatch(): Unit = {
    val op = new DelayedOperation(100000L) {
      override def onExpiration(): Unit = {}
      override def onComplete(): Unit = {}
      override def tryComplete(): Boolean = {
        assertTrue(lock.asInstanceOf[ReentrantLock].isHeldByCurrentThread)
        false
      }
      override def safeTryComplete(): Boolean = {
        fail("tryCompleteElseWatch should not use safeTryComplete")
        super.safeTryComplete()
      }
    }
    purgatory.tryCompleteElseWatch(op, Seq("key"))
  }

  @Test
  def testSafeTryCompleteOrElse(): Unit = {
    def op(shouldComplete: Boolean) = new DelayedOperation(100000L) {
      override def onExpiration(): Unit = {}
      override def onComplete(): Unit = {}
      override def tryComplete(): Boolean = {
        assertTrue(lock.asInstanceOf[ReentrantLock].isHeldByCurrentThread)
        shouldComplete
      }
    }
    var pass = false
    assertFalse(op(false).safeTryCompleteOrElse {
      pass = true
    })
    assertTrue(pass)
    assertTrue(op(true).safeTryCompleteOrElse {
      fail("this method should NOT be executed")
    })
  }

  @Test
  def testRequestSatisfaction(): Unit = {
    val r1 = new MockDelayedOperation(100000L)
    val r2 = new MockDelayedOperation(100000L)
    assertEquals(0, purgatory.checkAndComplete("test1"), "With no waiting requests, nothing should be satisfied")
    assertFalse(purgatory.tryCompleteElseWatch(r1, Array("test1")), "r1 not satisfied and hence watched")
    assertEquals(0, purgatory.checkAndComplete("test1"), "Still nothing satisfied")
    assertFalse(purgatory.tryCompleteElseWatch(r2, Array("test2")), "r2 not satisfied and hence watched")
    assertEquals(0, purgatory.checkAndComplete("test2"), "Still nothing satisfied")
    r1.completable = true
    assertEquals(1, purgatory.checkAndComplete("test1"), "r1 satisfied")
    assertEquals(0, purgatory.checkAndComplete("test1"), "Nothing satisfied")
    r2.completable = true
    assertEquals(1, purgatory.checkAndComplete("test2"), "r2 satisfied")
    assertEquals(0, purgatory.checkAndComplete("test2"), "Nothing satisfied")
  }

  @Test
  def testRequestExpiry(): Unit = {
    val expiration = 20L
    val start = Time.SYSTEM.hiResClockMs
    val r1 = new MockDelayedOperation(expiration)
    val r2 = new MockDelayedOperation(200000L)
    assertFalse(purgatory.tryCompleteElseWatch(r1, Array("test1")), "r1 not satisfied and hence watched")
    assertFalse(purgatory.tryCompleteElseWatch(r2, Array("test2")), "r2 not satisfied and hence watched")
    r1.awaitExpiration()
    val elapsed = Time.SYSTEM.hiResClockMs - start
    assertTrue(r1.isCompleted, "r1 completed due to expiration")
    assertFalse(r2.isCompleted, "r2 hasn't completed")
    assertTrue(elapsed >= expiration, s"Time for expiration $elapsed should at least $expiration")
  }

  @Test
  def testDelayedFuture(): Unit = {
    val purgatoryName = "testDelayedFuture"
    val purgatory = new DelayedFuturePurgatory(purgatoryName, brokerId = 0)
    val result = new AtomicInteger()

    def hasExecutorThread: Boolean = Thread.getAllStackTraces.keySet.asScala.map(_.getName)
      .exists(_.contains(s"DelayedExecutor-$purgatoryName"))
    def updateResult(futures: List[CompletableFuture[Integer]]): Unit =
      result.set(futures.filterNot(_.isCompletedExceptionally).map(_.get.intValue).sum)

    assertFalse(hasExecutorThread, "Unnecessary thread created")

    // Two completed futures: callback should be executed immediately on the same thread
    val futures1 = List(CompletableFuture.completedFuture(10.asInstanceOf[Integer]),
      CompletableFuture.completedFuture(11.asInstanceOf[Integer]))
    val r1 = purgatory.tryCompleteElseWatch[Integer](100000L, futures1, () => updateResult(futures1))
    assertTrue(r1.isCompleted, "r1 not completed")
    assertEquals(21, result.get())
    assertFalse(hasExecutorThread, "Unnecessary thread created")

    // Two delayed futures: callback should wait for both to complete
    result.set(-1)
    val futures2 = List(new CompletableFuture[Integer], new CompletableFuture[Integer])
    val r2 = purgatory.tryCompleteElseWatch[Integer](100000L, futures2, () => updateResult(futures2))
    assertFalse(r2.isCompleted, "r2 should be incomplete")
    futures2.head.complete(20)
    assertFalse(r2.isCompleted)
    assertEquals(-1, result.get())
    futures2(1).complete(21)
    TestUtils.waitUntilTrue(() => r2.isCompleted, "r2 not completed")
    TestUtils.waitUntilTrue(() => result.get == 41, "callback not invoked")
    assertTrue(hasExecutorThread, "Thread not created for executing delayed task")

    // One immediate and one delayed future: callback should wait for delayed task to complete
    result.set(-1)
    val futures3 = List(new CompletableFuture[Integer], CompletableFuture.completedFuture(31.asInstanceOf[Integer]))
    val r3 = purgatory.tryCompleteElseWatch[Integer](100000L, futures3, () => updateResult(futures3))
    assertFalse(r3.isCompleted, "r3 should be incomplete")
    assertEquals(-1, result.get())
    futures3.head.complete(30)
    TestUtils.waitUntilTrue(() => r3.isCompleted, "r3 not completed")
    TestUtils.waitUntilTrue(() => result.get == 61, "callback not invoked")


    // One future doesn't complete within timeout. Should expire and invoke callback after timeout.
    result.set(-1)
    val start = Time.SYSTEM.hiResClockMs
    val expirationMs = 2000L
    val futures4 = List(new CompletableFuture[Integer], new CompletableFuture[Integer])
    val r4 = purgatory.tryCompleteElseWatch[Integer](expirationMs, futures4, () => updateResult(futures4))
    futures4.head.complete(40)
    TestUtils.waitUntilTrue(() => futures4(1).isDone, "r4 futures not expired")
    assertTrue(r4.isCompleted, "r4 not completed after timeout")
    val elapsed = Time.SYSTEM.hiResClockMs - start
    assertTrue(elapsed >= expirationMs, s"Time for expiration $elapsed should at least $expirationMs")
    assertEquals(40, futures4.head.get)
    assertEquals(classOf[org.apache.kafka.common.errors.TimeoutException],
      assertThrows(classOf[ExecutionException], () => futures4(1).get).getCause.getClass)
    assertEquals(40, result.get())
  }

  @Test
  def testRequestPurge(): Unit = {
    val r1 = new MockDelayedOperation(100000L)
    val r2 = new MockDelayedOperation(100000L)
    val r3 = new MockDelayedOperation(100000L)
    purgatory.tryCompleteElseWatch(r1, Array("test1"))
    purgatory.tryCompleteElseWatch(r2, Array("test1", "test2"))
    purgatory.tryCompleteElseWatch(r3, Array("test1", "test2", "test3"))

    assertEquals(3, purgatory.numDelayed, "Purgatory should have 3 total delayed operations")
    assertEquals(6, purgatory.watched, "Purgatory should have 6 watched elements")

    // complete the operations, it should immediately be purged from the delayed operation
    r2.completable = true
    r2.tryComplete()
    assertEquals(2, purgatory.numDelayed, "Purgatory should have 2 total delayed operations instead of " + purgatory.numDelayed)

    r3.completable = true
    r3.tryComplete()
    assertEquals(1, purgatory.numDelayed, "Purgatory should have 1 total delayed operations instead of " + purgatory.numDelayed)

    // checking a watch should purge the watch list
    purgatory.checkAndComplete("test1")
    assertEquals(4, purgatory.watched, "Purgatory should have 4 watched elements instead of " + purgatory.watched)

    purgatory.checkAndComplete("test2")
    assertEquals(2, purgatory.watched, "Purgatory should have 2 watched elements instead of " + purgatory.watched)

    purgatory.checkAndComplete("test3")
    assertEquals(1, purgatory.watched, "Purgatory should have 1 watched elements instead of " + purgatory.watched)
  }

  @Test
  def shouldCancelForKeyReturningCancelledOperations(): Unit = {
    purgatory.tryCompleteElseWatch(new MockDelayedOperation(10000L), Seq("key"))
    purgatory.tryCompleteElseWatch(new MockDelayedOperation(10000L), Seq("key"))
    purgatory.tryCompleteElseWatch(new MockDelayedOperation(10000L), Seq("key2"))

    val cancelledOperations = purgatory.cancelForKey("key")
    assertEquals(2, cancelledOperations.size)
    assertEquals(1, purgatory.numDelayed)
    assertEquals(1, purgatory.watched)
  }

  @Test
  def shouldReturnNilOperationsOnCancelForKeyWhenKeyDoesntExist(): Unit = {
    val cancelledOperations = purgatory.cancelForKey("key")
    assertEquals(Nil, cancelledOperations)
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

    ops.foreach { op => assertTrue(op.isCompleted, "Operation should have completed") }
  }

  def verifyDelayedOperationLock(mockDelayedOperation: => MockDelayedOperation, mismatchedLocks: Boolean): Unit = {
    val key = "key"
    executorService = Executors.newSingleThreadExecutor
    def createDelayedOperations(count: Int): Seq[MockDelayedOperation] = {
      (1 to count).map { _ =>
        val op = mockDelayedOperation
        purgatory.tryCompleteElseWatch(op, Seq(key))
        assertFalse(op.isCompleted, "Not completable")
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
      expectedComplete.foreach(op => assertTrue(op.isCompleted, "Should have completed"))
      val expectedNotComplete = completableOps.toSet -- expectedComplete
      expectedNotComplete.foreach(op => assertFalse(op.isCompleted, "Should not have completed"))
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
          assertFalse(mismatchedLocks, "Should have failed with mismatched locks")
        } catch {
          case e: IllegalStateException =>
            assertTrue(mismatchedLocks, "Should not have failed with valid locks")
        }
      } finally {
        runOnAnotherThread(lock.unlock(), true)
        checkAndComplete(Seq(ops(0)), Seq(ops(0)))
      }
    }

    // Immediately completable operations should complete without locking
    ops = createCompletableOperations(2)
    ops.foreach { op =>
      assertTrue(purgatory.tryCompleteElseWatch(op, Seq(key)), "Should have completed")
      assertTrue(op.isCompleted, "Should have completed")
    }
  }

  private def runOnAnotherThread(fun: => Unit, shouldComplete: Boolean): Future[_] = {
    val future = executorService.submit(new Runnable {
      def run() = fun
    })
    if (shouldComplete)
      future.get()
    else
      assertFalse(future.isDone, "Should not have completed")
    future
  }

  class MockDelayedOperation(delayMs: Long,
                             lockOpt: Option[ReentrantLock] = None,
                             val responseLockOpt: Option[ReentrantLock] = None)
                             extends DelayedOperation(delayMs, lockOpt) {
    var completable = false

    def awaitExpiration(): Unit = {
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

    override def onExpiration(): Unit = {

    }

    override def onComplete(): Unit = {
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
