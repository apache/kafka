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

import kafka.network._
import kafka.utils._
import kafka.metrics.KafkaMetricsGroup

import java.util
import java.util.concurrent._
import java.util.concurrent.atomic._
import scala.collection._

import com.yammer.metrics.core.Gauge


/**
 * A request whose processing needs to be delayed for at most the given delayMs
 * The associated keys are used for bookeeping, and represent the "trigger" that causes this request to check if it is satisfied,
 * for example a key could be a (topic, partition) pair.
 */
class DelayedRequest(val keys: Seq[Any], val request: RequestChannel.Request, delayMs: Long) extends DelayedItem[RequestChannel.Request](request, delayMs) {
  val satisfied = new AtomicBoolean(false)
}

/**
 * A helper class for dealing with asynchronous requests with a timeout. A DelayedRequest has a request to delay
 * and also a list of keys that can trigger the action. Implementations can add customized logic to control what it means for a given
 * request to be satisfied. For example it could be that we are waiting for user-specified number of acks on a given (topic, partition)
 * to be able to respond to a request or it could be that we are waiting for a given number of bytes to accumulate on a given request
 * to be able to respond to that request (in the simple case we might wait for at least one byte to avoid busy waiting).
 *
 * For us the key is generally a (topic, partition) pair.
 * By calling 
 *   val isSatisfiedByMe = checkAndMaybeWatch(delayedRequest)
 * we will check if a request is satisfied already, and if not add the request for watch on all its keys.
 *
 * It is up to the user to then call
 *   val satisfied = update(key, request) 
 * when a request relevant to the given key occurs. This triggers bookeeping logic and returns back any requests satisfied by this
 * new request.
 *
 * An implementation provides extends two helper functions
 *   def checkSatisfied(request: R, delayed: T): Boolean
 * this function returns true if the given request (in combination with whatever previous requests have happened) satisfies the delayed
 * request delayed. This method will likely also need to do whatever bookkeeping is necessary.
 *
 * The second function is
 *   def expire(delayed: T)
 * this function handles delayed requests that have hit their time limit without being satisfied.
 *
 */
abstract class RequestPurgatory[T <: DelayedRequest](brokerId: Int = 0, purgeInterval: Int = 1000)
        extends Logging with KafkaMetricsGroup {

  /* a list of requests watching each key */
  private val watchersForKey = new Pool[Any, Watchers](Some((key: Any) => new Watchers))

  /* background thread expiring requests that have been waiting too long */
  private val expiredRequestReaper = new ExpiredRequestReaper
  private val expirationThread = Utils.newThread(name="request-expiration-task", runnable=expiredRequestReaper, daemon=false)

  newGauge(
    "PurgatorySize",
    new Gauge[Int] {
      def value = watched()
    }
  )

  newGauge(
    "NumDelayedRequests",
    new Gauge[Int] {
      def value = delayed()
    }
  )

  expirationThread.start()

  /**
   * Is this request satisfied by the caller thread?
   */
  private def isSatisfiedByMe(delayedRequest: T): Boolean = {
    if(delayedRequest.satisfied.compareAndSet(false, true))
      return true
    else
      return false
  }

  /**
   * Try to add the request for watch on all keys. Return true iff the request is
   * satisfied and the satisfaction is done by the caller.
   */
  def checkAndMaybeWatch(delayedRequest: T): Boolean = {
    if (delayedRequest.keys.size <= 0)
      return isSatisfiedByMe(delayedRequest)

    // The cost of checkSatisfied() is typically proportional to the number of keys. Calling
    // checkSatisfied() for each key is going to be expensive if there are many keys. Instead,
    // we do the check in the following way. Call checkSatisfied(). If the request is not satisfied,
    // we just add the request to all keys. Then we call checkSatisfied() again. At this time, if
    // the request is still not satisfied, we are guaranteed that it won't miss any future triggering
    // events since the request is already on the watcher list for all keys. This does mean that
    // if the request is satisfied (by another thread) between the two checkSatisfied() calls, the
    // request is unnecessarily added for watch. However, this is a less severe issue since the
    // expire reaper will clean it up periodically.

    var isSatisfied = delayedRequest synchronized checkSatisfied(delayedRequest)
    if (isSatisfied)
      return isSatisfiedByMe(delayedRequest)

    for(key <- delayedRequest.keys) {
      val lst = watchersFor(key)
      if (!lst.addIfNotSatisfied(delayedRequest)) {
        // The request is already satisfied by another thread. No need to watch for the rest of
        // the keys.
        return false
      }
    }

    isSatisfied = delayedRequest synchronized checkSatisfied(delayedRequest)
    if (isSatisfied)
      return isSatisfiedByMe(delayedRequest)
    else {
      // If the request is still not satisfied, add to the expire queue also.
      expiredRequestReaper.enqueue(delayedRequest)

      return false
    }
  }

  /**
   * Update any watchers and return a list of newly satisfied requests.
   */
  def update(key: Any): Seq[T] = {
    val w = watchersForKey.get(key)
    if(w == null)
      Seq.empty
    else
      w.collectSatisfiedRequests()
  }

  /*
   * Return the size of the watched lists in the purgatory, which is the size of watch lists.
   * Since an operation may still be in the watch lists even when it has been completed,
   * this number may be larger than the number of real operations watched
   */
  def watched() = watchersForKey.values.map(_.watched).sum

  /*
   * Return the number of requests in the expiry reaper's queue
   */
  def delayed() = expiredRequestReaper.delayed()

  /*
   * Return the watch list for the given watch key
   */
  private def watchersFor(key: Any) = watchersForKey.getAndMaybePut(key)
  
  /**
   * Check if this delayed request is already satisfied
   */
  protected def checkSatisfied(request: T): Boolean

  /**
   * Handle an expired delayed request
   */
  protected def expire(delayed: T)

  /**
   * Shutdown the expire reaper thread
   */
  def shutdown() {
    expiredRequestReaper.shutdown()
  }

  /**
   * A linked list of DelayedRequests watching some key with some associated
   * bookkeeping logic.
   */
  private class Watchers {
    private val requests = new util.LinkedList[T]

    // return the size of the watch list
    def watched() = requests.size()

    // add the element to the watcher list if it's not already satisfied
    def addIfNotSatisfied(t: T): Boolean = {
      if (t.satisfied.get)
        return false

      synchronized {
        requests.add(t)
      }

      return true
    }

    // traverse the list and purge satisfied elements
    def purgeSatisfied(): Int = {
      synchronized {
        val iter = requests.iterator()
        var purged = 0
        while(iter.hasNext) {
          val curr = iter.next
          if(curr.satisfied.get()) {
            iter.remove()
            purged += 1
          }
        }
        purged
      }
    }

    // traverse the list and try to satisfy watched elements
    def collectSatisfiedRequests(): Seq[T] = {
      val response = new mutable.ArrayBuffer[T]
      synchronized {
        val iter = requests.iterator()
        while(iter.hasNext) {
          val curr = iter.next
          if(curr.satisfied.get) {
            // another thread has satisfied this request, remove it
            iter.remove()
          } else {
            // synchronize on curr to avoid any race condition with expire
            // on client-side.
            val satisfied = curr synchronized checkSatisfied(curr)
            if(satisfied) {
              iter.remove()
              val updated = curr.satisfied.compareAndSet(false, true)
              if(updated == true) {
                response += curr
              }
            }
          }
        }
      }
      response
    }
  }

  /**
   * Runnable to expire requests that have sat unfullfilled past their deadline
   */
  private class ExpiredRequestReaper extends Runnable with Logging {
    this.logIdent = "ExpiredRequestReaper-%d ".format(brokerId)
    private val running = new AtomicBoolean(true)
    private val shutdownLatch = new CountDownLatch(1)

    private val delayedQueue = new DelayQueue[T]

    def delayed() = delayedQueue.size()
    
    /** Main loop for the expiry thread */
    def run() {
      while(running.get) {
        try {
          val curr = pollExpired()
          if (curr != null) {
            curr synchronized {
              expire(curr)
            }
          }
          // see if we need to purge the watch lists
          if (RequestPurgatory.this.watched() >= purgeInterval) {
            debug("Begin purging watch lists")
            val numPurgedFromWatchers = watchersForKey.values.map(_.purgeSatisfied()).sum
            debug("Purged %d elements from watch lists.".format(numPurgedFromWatchers))
          }
          // see if we need to purge the delayed request queue
          if (delayed() >= purgeInterval) {
            debug("Begin purging delayed queue")
            val purged = purgeSatisfied()
            debug("Purged %d requests from delayed queue.".format(purged))
          }
        } catch {
          case e: Exception =>
            error("Error in long poll expiry thread: ", e)
        }
      }
      shutdownLatch.countDown()
    }

    /** Add a request to be expired */
    def enqueue(t: T) {
      delayedQueue.add(t)
    }

    /** Shutdown the expiry thread*/
    def shutdown() {
      debug("Shutting down.")
      running.set(false)
      shutdownLatch.await()
      debug("Shut down complete.")
    }

    /**
     * Get the next expired event
     */
    private def pollExpired(): T = {
      while(true) {
        val curr = delayedQueue.poll(200L, TimeUnit.MILLISECONDS)
        if (curr == null)
          return null.asInstanceOf[T]
        val updated = curr.satisfied.compareAndSet(false, true)
        if(updated) {
          return curr
        }
      }
      throw new RuntimeException("This should not happen")
    }

    /**
     * Delete all satisfied events from the delay queue and the watcher lists
     */
    private def purgeSatisfied(): Int = {
      var purged = 0

      // purge the delayed queue
      val iter = delayedQueue.iterator()
      while(iter.hasNext) {
        val curr = iter.next()
        if(curr.satisfied.get) {
          iter.remove()
          purged += 1
        }
      }

      purged
    }
  }

}
