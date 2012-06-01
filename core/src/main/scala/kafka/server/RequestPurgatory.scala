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
import java.util.LinkedList
import java.util.concurrent._
import java.util.concurrent.atomic._
import kafka.network._
import kafka.utils._

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
abstract class RequestPurgatory[T <: DelayedRequest, R] {
  
  /* a list of requests watching each key */
  private val watchersForKey = new ConcurrentHashMap[Any, Watchers]
  
  /* background thread expiring requests that have been waiting too long */
  private val expiredRequestReaper = new ExpiredRequestReaper
  private val expirationThread = Utils.daemonThread("request-expiration-task", expiredRequestReaper)
  expirationThread.start()
  
  /**
   * Add a new delayed request watching the contained keys
   */
  def watch(delayedRequest: T) {
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
  
  private def watchersFor(key: Any): Watchers = {
    var lst = watchersForKey.get(key)
    if(lst == null) {
      watchersForKey.putIfAbsent(key, new Watchers)
      lst = watchersForKey.get(key)
    }
    lst
  }
  
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
   * A linked list of DelayedRequests watching some key with some associated bookeeping logic
   */
  private class Watchers {
    
    /* a few magic parameters to help do cleanup to avoid accumulating old watchers */
    private val CleanupThresholdSize = 100
    private val CleanupThresholdPrct = 0.5
    
    private val requests = new LinkedList[T]
    
    /* you can only change this if you have added something or marked something satisfied */
    var liveCount = 0.0
  
    def add(t: T) {
      synchronized {
        requests.add(t)
        liveCount += 1
        maybePurge()
      }
    }
    
    private def maybePurge() {
      if(requests.size > CleanupThresholdSize && liveCount / requests.size < CleanupThresholdPrct) {
        val iter = requests.iterator()
        while(iter.hasNext) {
          val curr = iter.next
          if(curr.satisfied.get())
            iter.remove()
        }
      }
    }
  
    def decLiveCount() {
      synchronized {
        liveCount -= 1
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
            if(checkSatisfied(request, curr)) {
              iter.remove()
              val updated = curr.satisfied.compareAndSet(false, true)
              if(updated == true) {
                response += curr
                liveCount -= 1
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
    
    /* a few magic parameters to help do cleanup to avoid accumulating old watchers */
    private val CleanupThresholdSize = 100
    private val CleanupThresholdPrct = 0.5
    
    private val delayed = new DelayQueue[T]
    private val running = new AtomicBoolean(true)
    private val shutdownLatch = new CountDownLatch(1)
    private val needsPurge = new AtomicBoolean(false)
    /* The count of elements in the delay queue that are unsatisfied */
    private val unsatisfied = new AtomicInteger(0)
    
    /** Main loop for the expiry thread */
    def run() {
      while(running.get) {
        try {
          val curr = pollExpired()
          expire(curr)
        } catch {
          case ie: InterruptedException => 
            if(needsPurge.getAndSet(false)) {
              val purged = purgeSatisfied()
              debug("Forced purge of " + purged + " requests from delay queue.")
            }
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
      if(unsatisfied.get > CleanupThresholdSize && unsatisfied.get / delayed.size.toDouble < CleanupThresholdPrct)
        forcePurge()
    }
    
    private def forcePurge() {
      needsPurge.set(true)
      expirationThread.interrupt()
    }
    
    /** Shutdown the expiry thread*/
    def shutdown() {
      debug("Shutting down request expiry thread")
      running.set(false)
      expirationThread.interrupt()
      shutdownLatch.await()
    }
    
    /** Record the fact that we satisfied a request in the stats for the expiry queue */
    def satisfyRequest(): Unit = unsatisfied.getAndDecrement()
    
    /**
     * Get the next expired event
     */
    private def pollExpired(): T = {
      while(true) {
        val curr = delayed.take()
        val updated = curr.satisfied.compareAndSet(false, true)
        if(updated) {
          unsatisfied.getAndDecrement()
          for(key <- curr.keys)
            watchersFor(key).decLiveCount()
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