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

import scala.collection._
import java.util.concurrent._
import java.util.concurrent.atomic._
import kafka.network._
import kafka.utils._
import kafka.metrics.KafkaMetricsGroup
import java.util
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
 *   watch(delayedRequest) 
 * we will add triggers for each of the given keys. It is up to the user to then call
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
abstract class RequestPurgatory[T <: DelayedRequest, R](brokerId: Int = 0, purgeInterval: Int = 10000)
        extends Logging with KafkaMetricsGroup {

  /* a list of requests watching each key */
  private val watchersForKey = new Pool[Any, Watchers](Some((key: Any) => new Watchers))

  private val requestCounter = new AtomicInteger(0)

  newGauge(
    "PurgatorySize",
    new Gauge[Int] {
      def getValue = watchersForKey.values.map(_.numRequests).sum + expiredRequestReaper.numRequests
    }
  )

  newGauge(
    "NumDelayedRequests",
    new Gauge[Int] {
      def getValue = expiredRequestReaper.unsatisfied.get()
    }
  )

  /* background thread expiring requests that have been waiting too long */
  private val expiredRequestReaper = new ExpiredRequestReaper
  private val expirationThread = Utils.newThread(name="request-expiration-task", runnable=expiredRequestReaper, daemon=false)
  expirationThread.start()

  /**
   * Add a new delayed request watching the contained keys
   */
  def watch(delayedRequest: T) {
    requestCounter.getAndIncrement()

    for(key <- delayedRequest.keys) {
      var lst = watchersFor(key)
      lst.add(delayedRequest)
    }
    expiredRequestReaper.enqueue(delayedRequest)
  }

  /**
   * Update any watchers and return a list of newly satisfied requests.
   */
  def update(key: Any, request: R): Seq[T] = {
    val w = watchersForKey.get(key)
    if(w == null)
      Seq.empty
    else
      w.collectSatisfiedRequests(request)
  }

  private def watchersFor(key: Any) = watchersForKey.getAndMaybePut(key)
  
  /**
   * Check if this request satisfied this delayed request
   */
  protected def checkSatisfied(request: R, delayed: T): Boolean

  /**
   * Handle an expired delayed request
   */
  protected def expire(delayed: T)

  /**
   * Shutdown the expirey thread
   */
  def shutdown() {
    expiredRequestReaper.shutdown()
  }

  /**
   * A linked list of DelayedRequests watching some key with some associated
   * bookkeeping logic.
   */
  private class Watchers {


    private val requests = new util.ArrayList[T]

    def numRequests = requests.size

    def add(t: T) {
      synchronized {
        requests.add(t)
      }
    }

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

    def collectSatisfiedRequests(request: R): Seq[T] = {
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
            val satisfied = curr synchronized checkSatisfied(request, curr)
            if(satisfied) {
              iter.remove()
              val updated = curr.satisfied.compareAndSet(false, true)
              if(updated == true) {
                response += curr
                expiredRequestReaper.satisfyRequest()
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

    private val delayed = new DelayQueue[T]
    private val running = new AtomicBoolean(true)
    private val shutdownLatch = new CountDownLatch(1)

    /* The count of elements in the delay queue that are unsatisfied */
    private [kafka] val unsatisfied = new AtomicInteger(0)

    def numRequests = delayed.size()

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
          if (requestCounter.get >= purgeInterval) { // see if we need to force a full purge
            requestCounter.set(0)
            val purged = purgeSatisfied()
            debug("Purged %d requests from delay queue.".format(purged))
            val numPurgedFromWatchers = watchersForKey.values.map(_.purgeSatisfied()).sum
            debug("Purged %d (watcher) requests.".format(numPurgedFromWatchers))
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
      delayed.add(t)
      unsatisfied.incrementAndGet()
    }

    /** Shutdown the expiry thread*/
    def shutdown() {
      debug("Shutting down.")
      running.set(false)
      shutdownLatch.await()
      debug("Shut down complete.")
    }

    /** Record the fact that we satisfied a request in the stats for the expiry queue */
    def satisfyRequest(): Unit = unsatisfied.getAndDecrement()

    /**
     * Get the next expired event
     */
    private def pollExpired(): T = {
      while(true) {
        val curr = delayed.poll(200L, TimeUnit.MILLISECONDS)
        if (curr == null)
          return null.asInstanceOf[T]
        val updated = curr.satisfied.compareAndSet(false, true)
        if(updated) {
          unsatisfied.getAndDecrement()
          return curr
        }
      }
      throw new RuntimeException("This should not happen")
    }

    /**
     * Delete all expired events from the delay queue
     */
    private def purgeSatisfied(): Int = {
      var purged = 0
      val iter = delayed.iterator()
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
