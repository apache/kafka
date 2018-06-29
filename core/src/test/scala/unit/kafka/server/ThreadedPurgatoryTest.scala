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
package kafka.server

import java.util
import java.util.concurrent.{Semaphore, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong, AtomicReference}

import org.apache.kafka.common.utils.{MockTime, Time}
import org.junit.Assert.{assertEquals, assertFalse, assertTrue}
import org.junit.rules.Timeout
import org.junit.{Rule, Test}

import scala.collection.mutable

object ThreadedPurgatoryTest  {
  val TestOpCounter = new AtomicInteger(0)
}

class ThreadedPurgatoryTest  {
  @Rule
  def globalTimeout = Timeout.millis(10000) //Timeout.millis(120000)

  class TestDelayable(val error: AtomicReference[String],
                      val enteredCheck: Option[Semaphore] = None,
                      val canCompleteCheck: Option[Semaphore] = None) extends Delayable {
    val id = ThreadedPurgatoryTest.TestOpCounter.addAndGet(1)
    var remainingWorkItems = 1
    var completed = false
    var expired = false
    var numChecks = 0

    override def complete(timedOut: Boolean): Unit = synchronized {
      if (completed)
        error.compareAndSet(null, "complete invoked more than once.")
      else if (expired)
        error.compareAndSet(null, "complete invoked after expired == true.")
      else if (timedOut) {
        expired = true
      }
      completed = true
    }

    override def check() = {
      enteredCheck.map { sem => sem.release() }
      canCompleteCheck.map { sem => sem.acquire() }
      synchronized {
        numChecks = numChecks + 1
        remainingWorkItems == 0
      }
    }

    def setRemainingWorkItems(remainingWorkItems: Integer) = synchronized {
      this.remainingWorkItems = remainingWorkItems
    }

    override def toString() = {
      s"TestDelayable$id"
    }

    def getCompleted() = synchronized {
      completed
    }

    def getExpired() = synchronized {
      expired
    }

    def getNumChecks() = synchronized {
      numChecks
    }
  }

  private def iteratorToSeq[T](iter: util.Iterator[T]): Seq[T] = {
    val buffer = new mutable.ArrayBuffer[T]
    while (iter.hasNext) {
      buffer += iter.next()
    }
    buffer
  }

  @Test
  def testWatchMultiMapOperations(): Unit = {
    val map = new WatchMultiMap()
    assertEquals(0, map.map.size())
    assertEquals(Seq(), iteratorToSeq(map.iterator(Integer.valueOf(1))))

    val error = new AtomicReference[String](null)
    val delayable1 = new TestDelayable(error)
    map.put(new WatchIndex(Integer.valueOf(1), 1), delayable1)
    assertEquals(Seq(delayable1), iteratorToSeq(map.iterator(Integer.valueOf(1))))

    val delayable2 = new TestDelayable(error)
    val delayable3 = new TestDelayable(error)
    map.put(new WatchIndex(Integer.valueOf(1), 2), delayable2)
    map.put(new WatchIndex(Integer.valueOf(2), 3), delayable3)
    assertEquals(Seq(delayable1, delayable2), iteratorToSeq(map.iterator(Integer.valueOf(1))))
    assertEquals(Seq(delayable3), iteratorToSeq(map.iterator(Integer.valueOf(2))))
    assertEquals(Seq(), iteratorToSeq(map.iterator(Integer.valueOf(3))))

    map.remove(new WatchIndex(Integer.valueOf(1), 1), delayable1)
    assertEquals(Seq(), iteratorToSeq(map.iterator(Integer.valueOf(0))))
    assertEquals(Seq(delayable2), iteratorToSeq(map.iterator(Integer.valueOf(1))))
    assertEquals(Seq(delayable3), iteratorToSeq(map.iterator(Integer.valueOf(2))))
    assertEquals(Seq(), iteratorToSeq(map.iterator(Integer.valueOf(3))))
  }

  class BadlyHashedObject {
    override def hashCode() = 34
    override def equals(other: Any): Boolean =
      other match {
        case that: BadlyHashedObject => that.eq(this)
        case _ => false
      }
  }

  @Test
  def testWatchMultiMapIterator(): Unit = {
    val map = new WatchMultiMap()
    val obj1 = new BadlyHashedObject()
    val obj2 = new BadlyHashedObject()
    val obj3 = new BadlyHashedObject()
    val obj4 = new BadlyHashedObject()
    val obj5 = new BadlyHashedObject()
    val nextTime = new AtomicLong(1)

    assertEquals(0, map.map.size())
    assertEquals(Seq(), iteratorToSeq(map.iterator(obj1)))

    val error = new AtomicReference[String](null)
    val delayable1 = new TestDelayable(error)
    val delayable2 = new TestDelayable(error)
    map.put(new WatchIndex(obj1, nextTime.getAndIncrement()), delayable1)
    map.put(new WatchIndex(obj1, nextTime.getAndIncrement()), delayable2)
    map.put(new WatchIndex(obj2, nextTime.getAndIncrement()), delayable2)
    map.put(new WatchIndex(obj3, nextTime.getAndIncrement()), delayable1)
    map.put(new WatchIndex(obj3, nextTime.getAndIncrement()), delayable2)
    map.put(new WatchIndex(obj4, nextTime.getAndIncrement()), delayable1)
    assertEquals(Seq(delayable1, delayable2), iteratorToSeq(map.iterator(obj1)))
    assertEquals(Seq(delayable2), iteratorToSeq(map.iterator(obj2)))
    assertEquals(Seq(delayable1, delayable2), iteratorToSeq(map.iterator(obj3)))
    assertEquals(Seq(delayable1), iteratorToSeq(map.iterator(obj4)))
    assertEquals(Seq(), iteratorToSeq(map.iterator(obj5)))
  }

  @Test
  def testPurgatoryStartShutdown(): Unit = {
    val purgatory = new ThreadedPurgatory(Time.SYSTEM, "ThreadedPurgatory", 1)
    purgatory.close()
  }

  @Test
  def testPurgatoryOperations(): Unit = {
    val time = new MockTime(0, 0, 0)
    val purgatory = new ThreadedPurgatory(time, "ThreadedPurgatory", 2)
    val error = new AtomicReference[String](null)
    val delayable1 = new TestDelayable(error)
    val delayable2 = new TestDelayable(error)
    try {
      purgatory.register(delayable1, Seq(Integer.valueOf(1), Integer.valueOf(2)), 1)
      purgatory.register(delayable2, Seq(Integer.valueOf(2), Integer.valueOf(3)), 2)
      assertTrue(purgatory.contains(delayable1))
      assertTrue(purgatory.contains(delayable2))
      purgatory.scheduleCheck(delayable1)
      while (delayable1.getNumChecks() == 0)
        Thread.sleep(1)
      time.advanceHighRestTimeNs(1)
      while (!delayable1.getExpired())
        Thread.sleep(1)
      assertEquals(0, delayable2.getNumChecks())
      assertFalse(purgatory.contains(delayable1))
      assertTrue(purgatory.contains(delayable2))
      delayable2.setRemainingWorkItems(0)
      purgatory.scheduleCheck(delayable2)
      while (delayable2.getNumChecks() == 0)
        Thread.sleep(1)
      while (!delayable2.getCompleted())
        Thread.sleep(1)
    } finally {
      purgatory.close()
    }
    assertEquals(null, error.get())
  }

  @Test
  def testScheduleCheckWhileChecking(): Unit = {
    val time = new MockTime(0, 0, 0)
    val purgatory = new ThreadedPurgatory(time, "ThreadedPurgatory", 2)
    val error = new AtomicReference[String](null)
    val enteredCheck = new Semaphore(0)
    val canCompleteCheck = new Semaphore(0)
    val delayable1 = new TestDelayable(error, Option(enteredCheck), Option(canCompleteCheck))
    try {
      purgatory.register(delayable1, Seq(Integer.valueOf(1), Integer.valueOf(2)), 1)
      assertTrue(purgatory.contains(delayable1))
      purgatory.scheduleCheck(delayable1)
      purgatory.scheduleCheck(delayable1)
      enteredCheck.acquire()
      purgatory.scheduleCheck(delayable1)
      purgatory.scheduleCheck(delayable1)
      canCompleteCheck.release(2)
      while (delayable1.getNumChecks() != 2)
        Thread.sleep(1)
    } finally {
      purgatory.close()
    }
    assertEquals(null, error.get())
  }

  @Test
  def testScheduleByWatchKey(): Unit = {
    val time = new MockTime(0, 0, 0)
    val purgatory = new ThreadedPurgatory(time, "ThreadedPurgatory", 2)
    val error = new AtomicReference[String](null)
    val delayable1 = new TestDelayable(error)
    val delayable2 = new TestDelayable(error)
    try {
      purgatory.register(delayable1, Seq(Integer.valueOf(1), Integer.valueOf(2)), 1)
      purgatory.register(delayable2, Seq(Integer.valueOf(2), Integer.valueOf(3)), 1)
      assertTrue(purgatory.contains(delayable1))
      assertTrue(purgatory.contains(delayable2))
      purgatory.scheduleWatchKeysCheck(Seq(Integer.valueOf(2), Integer.valueOf(3)))
      while ((delayable1.getNumChecks() == 0) || (delayable2.getNumChecks() == 0))
        Thread.sleep(1)
      assertEquals(1, delayable1.getNumChecks())
      assertEquals(1, delayable2.getNumChecks())
      assertTrue(purgatory.contains(delayable1))
      assertTrue(purgatory.contains(delayable2))
      delayable1.setRemainingWorkItems(0)
      delayable2.setRemainingWorkItems(0)
      purgatory.scheduleWatchKeysCheck(Seq(Integer.valueOf(0), Integer.valueOf(1)))
      while (delayable1.getNumChecks() == 0)
        Thread.sleep(1)
      while (!delayable1.getCompleted())
        Thread.sleep(1)
      assertEquals(2, delayable1.getNumChecks())
      assertEquals(1, delayable2.getNumChecks())
      assertFalse(purgatory.contains(delayable1))
      assertTrue(purgatory.contains(delayable2))
    } finally {
      purgatory.close()
    }
    assertEquals(null, error.get())
  }
}
