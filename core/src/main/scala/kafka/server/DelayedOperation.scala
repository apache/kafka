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

import kafka.utils._
import kafka.metrics.KafkaMetricsGroup

import java.util
import java.util.concurrent._
import java.util.concurrent.atomic._
import scala.collection._

import com.yammer.metrics.core.Gauge


/**
 * An operation whose processing needs to be delayed for at most the given delayMs. For example
 * a delayed produce operation could be waiting for specified number of acks; or
 * a delayed fetch operation could be waiting for a given number of bytes to accumulate.
 *
 * The logic upon completing a delayed operation is defined in onComplete() and will be called exactly once.
 * Once an operation is completed, isCompleted() will return true. onComplete() can be triggered by either
 * forceComplete(), which forces calling onComplete() after delayMs if the operation is not yet completed,
 * or tryComplete(), which first checks if the operation can be completed or not now, and if yes calls
 * forceComplete().
 *
 * A subclass of DelayedOperation needs to provide an implementation of both onComplete() and tryComplete().
 */
abstract class DelayedOperation(delayMs: Long) extends DelayedItem(delayMs) {
  private val completed = new AtomicBoolean(false)

  /*
   * Force completing the delayed operation, if not already completed.
   * This function can be triggered when
   *
   * 1. The operation has been verified to be completable inside tryComplete()
   * 2. The operation has expired and hence needs to be completed right now
   *
   * Return true iff the operation is completed by the caller: note that
   * concurrent threads can try to complete the same operation, but only
   * the first thread will succeed in completing the operation and return
   * true, others will still return false
   */
  def forceComplete(): Boolean = {
    if (completed.compareAndSet(false, true)) {
      onComplete()
      true
    } else {
      false
    }
  }

  /**
   * Check if the delayed operation is already completed
   */
  def isCompleted(): Boolean = completed.get()

  /**
   * Call-back to execute when a delayed operation expires, but before completion.
   */
  def onExpiration(): Unit

  /**
   * Process for completing an operation; This function needs to be defined
   * in subclasses and will be called exactly once in forceComplete()
   */
  def onComplete(): Unit

  /*
   * Try to complete the delayed operation by first checking if the operation
   * can be completed by now. If yes execute the completion logic by calling
   * forceComplete() and return true iff forceComplete returns true; otherwise return false
   *
   * This function needs to be defined in subclasses
   */
  def tryComplete(): Boolean
}

/**
 * A helper purgatory class for bookkeeping delayed operations with a timeout, and expiring timed out operations.
 */
class DelayedOperationPurgatory[T <: DelayedOperation](purgatoryName: String, brokerId: Int = 0, purgeInterval: Int = 1000)
        extends Logging with KafkaMetricsGroup {

  /* a list of operation watching keys */
  private val watchersForKey = new Pool[Any, Watchers](Some((key: Any) => new Watchers))

  /* background thread expiring operations that have timed out */
  private val expirationReaper = new ExpiredOperationReaper

  private val metricsTags = Map("delayedOperation" -> purgatoryName)

  newGauge(
    "PurgatorySize",
    new Gauge[Int] {
      def value = watched()
    },
    metricsTags
  )

  newGauge(
    "NumDelayedOperations",
    new Gauge[Int] {
      def value = delayed()
    },
    metricsTags
  )

  expirationReaper.start()

  /**
   * Check if the operation can be completed, if not watch it based on the given watch keys
   *
   * Note that a delayed operation can be watched on multiple keys. It is possible that
   * an operation is completed after it has been added to the watch list for some, but
   * not all of the keys. In this case, the operation is considered completed and won't
   * be added to the watch list of the remaining keys. The expiration reaper thread will
   * remove this operation from any watcher list in which the operation exists.
   *
   * @param operation the delayed operation to be checked
   * @param watchKeys keys for bookkeeping the operation
   * @return true iff the delayed operations can be completed by the caller
   */
  def tryCompleteElseWatch(operation: T, watchKeys: Seq[Any]): Boolean = {
    assert(watchKeys.size > 0, "The watch key list can't be empty")

    // The cost of tryComplete() is typically proportional to the number of keys. Calling
    // tryComplete() for each key is going to be expensive if there are many keys. Instead,
    // we do the check in the following way. Call tryComplete(). If the operation is not completed,
    // we just add the operation to all keys. Then we call tryComplete() again. At this time, if
    // the operation is still not completed, we are guaranteed that it won't miss any future triggering
    // event since the operation is already on the watcher list for all keys. This does mean that
    // if the operation is completed (by another thread) between the two tryComplete() calls, the
    // operation is unnecessarily added for watch. However, this is a less severe issue since the
    // expire reaper will clean it up periodically.

    var isCompletedByMe = operation synchronized operation.tryComplete()
    if (isCompletedByMe)
      return true

    for(key <- watchKeys) {
      // If the operation is already completed, stop adding it to the rest of the watcher list.
      if (operation.isCompleted())
        return false
      val watchers = watchersFor(key)
      watchers.watch(operation)
    }

    isCompletedByMe = operation synchronized operation.tryComplete()
    if (isCompletedByMe)
      return true

    // if it cannot be completed by now and hence is watched, add to the expire queue also
    if (! operation.isCompleted())
      expirationReaper.enqueue(operation)

    false
  }

  /**
   * Check if some some delayed operations can be completed with the given watch key,
   * and if yes complete them.
   *
   * @return the number of completed operations during this process
   */
  def checkAndComplete(key: Any): Int = {
    val watchers = watchersForKey.get(key)
    if(watchers == null)
      0
    else
      watchers.tryCompleteWatched()
  }

  /**
   * Return the total size of watch lists the purgatory. Since an operation may be watched
   * on multiple lists, and some of its watched entries may still be in the watch lists
   * even when it has been completed, this number may be larger than the number of real operations watched
   */
  def watched() = watchersForKey.values.map(_.watched).sum

  /**
   * Return the number of delayed operations in the expiry queue
   */
  def delayed() = expirationReaper.delayed

  /*
   * Return the watch list of the given key
   */
  private def watchersFor(key: Any) = watchersForKey.getAndMaybePut(key)

  /**
   * Shutdown the expire reaper thread
   */
  def shutdown() {
    expirationReaper.shutdown()
  }

  /**
   * A linked list of watched delayed operations based on some key
   */
  private class Watchers {
    private val operations = new util.LinkedList[T]

    def watched = operations.size()

    // add the element to watch
    def watch(t: T) {
      synchronized {
        operations.add(t)
      }
    }

    // traverse the list and try to complete some watched elements
    def tryCompleteWatched(): Int = {
      var completed = 0
      synchronized {
        val iter = operations.iterator()
        while(iter.hasNext) {
          val curr = iter.next
          if (curr.isCompleted()) {
            // another thread has completed this operation, just remove it
            iter.remove()
          } else {
            if(curr synchronized curr.tryComplete()) {
              iter.remove()
              completed += 1
            }
          }
        }
      }
      completed
    }

    // traverse the list and purge elements that are already completed by others
    def purgeCompleted(): Int = {
      var purged = 0
      synchronized {
        val iter = operations.iterator()
        while (iter.hasNext) {
          val curr = iter.next
          if(curr.isCompleted()) {
            iter.remove()
            purged += 1
          }
        }
      }
      purged
    }
  }

  /**
   * A background reaper to expire delayed operations that have timed out
   */
  private class ExpiredOperationReaper extends ShutdownableThread(
    "ExpirationReaper-%d".format(brokerId),
    false) {

    /* The queue storing all delayed operations */
    private val delayedQueue = new DelayQueue[T]

    /*
     * Return the number of delayed operations kept by the reaper
     */
    def delayed() = delayedQueue.size()

    /*
     * Add an operation to be expired
     */
    def enqueue(t: T) {
      delayedQueue.add(t)
    }

    /**
     * Try to get the next expired event and force completing it
     */
    private def expireNext() {
      val curr = delayedQueue.poll(200L, TimeUnit.MILLISECONDS)
      if (curr != null.asInstanceOf[T]) {
        // if there is an expired operation, try to force complete it
        val completedByMe: Boolean = curr synchronized {
          curr.onExpiration()
          curr.forceComplete()
        }
        if (completedByMe)
          debug("Force complete expired delayed operation %s".format(curr))
      }
    }

    /**
     * Delete all satisfied events from the delay queue and the watcher lists
     */
    private def purgeCompleted(): Int = {
      var purged = 0

      // purge the delayed queue
      val iter = delayedQueue.iterator()
      while (iter.hasNext) {
        val curr = iter.next()
        if (curr.isCompleted()) {
          iter.remove()
          purged += 1
        }
      }

      purged
    }

    override def doWork() {
      // try to get the next expired operation and force completing it
      expireNext()
      // see if we need to purge the watch lists
      if (DelayedOperationPurgatory.this.watched() >= purgeInterval) {
        debug("Begin purging watch lists")
        val purged = watchersForKey.values.map(_.purgeCompleted()).sum
        debug("Purged %d elements from watch lists.".format(purged))
      }
      // see if we need to purge the delayed operation queue
      if (delayed() >= purgeInterval) {
        debug("Begin purging delayed queue")
        val purged = purgeCompleted()
        debug("Purged %d operations from delayed queue.".format(purged))
      }
    }
  }
}
