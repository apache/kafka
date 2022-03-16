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
package kafka.metrics.clientmetrics

import kafka.Kafka.info
import kafka.metrics.clientmetrics.ClientMetricsCache.{DEFAULT_TTL_MS, cmCache}
import kafka.metrics.clientmetrics.ClientMetricsConfig.SubscriptionInfo
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.cache.LRUCache
import org.apache.log4j.helpers.LogLog.{debug, error}

import java.util.Calendar
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}

/**
 * Client Metrics Cache:
 *   Standard LRU Cache of the ClientInstanceState objects that are created during to the client's
 *   GetTelemetrySubscriptionRequest message.
 *
 *   Eviction Policy:
 *      1. Standard LRU eviction policy applies once cache size reaches its max size.
 *      2. In addition to the LRU eviction there is a GC for the elements that have stayed too long
 *      in the cache. There is a last accessed timestamp is set for every cached object which gets
 *      updated every time a cache object is accessed by GetTelemetrySubscriptionRequest or
 *      PushTelemetrySubscriptionRequest. During the GC, all the elements that are inactive beyond
 *      TTL time period would be cleaned up from the cache. GC operation is an asynchronous task
 *      triggered by ClientMetricManager in specific intervals governed by CM_CACHE_GC_INTERVAL.
 *
 *   Invalidation of the Cached objects:
 *      Since ClientInstanceState objects are created by compiling qualified client metric subscriptions
 *      they can go out of sync whenever client metric subscriptions changed like adding a new subscriptions or
 *      updating an existing subscription. So there is a need to invalidate the cached objects whenever
 *      client metric subscription is updated. Invalidation method iterates through all the matched client
 *      instances and applies the subscription changes by replacing it with a new ClientInstanceState object.
 *
 *   Locking:
 *      All the cache modifiers (add/delete/replace) are synchronized through standard scala object
 *      level synchronized method. For better concurrency there is no explicit locking applied on
 *      read/get operations.
 */
object  ClientMetricsCache {
  val DEFAULT_TTL_MS = 60 * 1000  // One minute
  val CM_CACHE_GC_INTERVAL_MS = 5 * 60 * 1000 // 5 minutes
  val CM_CACHE_MAX_SIZE = 16384 // Max cache size (16k active client connections per broker)
  val gcTs = Calendar.getInstance.getTime
  private val cmCache = new ClientMetricsCache(CM_CACHE_MAX_SIZE)

  def getInstance = cmCache

  /**
   * Launches the asynchronous task to clean the client metric subscriptions that are expired in the cache.
   */
  def runGCIfNeeded(forceGC: Boolean = false): Unit = {
    gcTs.synchronized {
      val timeElapsed = Calendar.getInstance.getTime.getTime - gcTs.getTime
      if (forceGC || cmCache.getSize > CM_CACHE_MAX_SIZE && timeElapsed > CM_CACHE_GC_INTERVAL_MS) {
        cmCache.cleanupExpiredEntries("GC").onComplete {
          case Success(value) => info(s"Client Metrics subscriptions cache cleaned up $value entries")
          case Failure(e) => error(s"Client Metrics subscription cache cleanup failed: ${e.getMessage}")
        }
      }
    }
  }
}

class ClientMetricsCache(maxSize: Int) {
  private val _cache = new LRUCache[Uuid, ClientMetricsCacheValue](maxSize)
  def getSize = _cache.size()
  def clear() = _cache.clear()
  def get(id: Uuid): CmClientInstanceState =  {
    val value = _cache.get(id)
    if (value != null) value.getClientInstance else null
  }

  /**
   * Iterates through all the elements of the cache and updates the client instance state objects that
   * matches the client subscription that is being updated.
   * @param oldSubscriptionInfo -- Subscription that has been deleted from the client metrics subscription
   * @param newSubscriptionInfo -- subscription that has been added to the client metrics subscription
   */
  def invalidate(oldSubscriptionInfo: SubscriptionInfo, newSubscriptionInfo: SubscriptionInfo) = {
    _cache.synchronized {
      update(oldSubscriptionInfo, newSubscriptionInfo)
    }
  }

  def add(instance: CmClientInstanceState)= {
    _cache.synchronized{
      _cache.put(instance.getId, new ClientMetricsCacheValue(instance))
    }
  }

  def cleanupExpiredEntries(reason: String): Future[Long] = Future {
    _cache.synchronized{
      val preCleanupSize = _cache.size()
      cmCache.cleanupTtlEntries()
      preCleanupSize - _cache.size()
    }
  }


  ///////// **** PRIVATE - METHODS **** /////////////
  private def update(oldSubscription: SubscriptionInfo, newSubscription: SubscriptionInfo) = {
    _cache.entrySet().forEach(element =>  {
      val clientInstance = element.getValue.getClientInstance
      val updatedMetricSubscriptions = clientInstance.getSubscriptions
      if (oldSubscription!= null && clientInstance.getClientInfo.isMatched(oldSubscription.getClientMatchingPatterns)) {
        updatedMetricSubscriptions.remove(oldSubscription)
      }
      if (newSubscription != null && clientInstance.getClientInfo.isMatched(newSubscription.getClientMatchingPatterns)){
        updatedMetricSubscriptions.add(newSubscription)
      }
      element.getValue.replace(CmClientInstanceState(clientInstance, updatedMetricSubscriptions))
    })
  }

  private def isExpired(element: CmClientInstanceState) = {
    val currentTs = Calendar.getInstance.getTime
    val delta = currentTs.getTime - element.getLastAccessTs.getTime
    delta > Math.max(3 * element.getPushIntervalMs, DEFAULT_TTL_MS)
  }

  private def cleanupTtlEntries() = {
    val iter = _cache.entrySet().iterator()
    while (iter.hasNext) {
      val element = iter.next().getValue.clientInstance
      if (isExpired(element)) {
        debug(s"Client subscription entry ${element} is expired removing it from the cache")
        iter.remove()
      }
    }
  }


  /**
   * Wrapper class to hold the CmClientInstance object and helps in preserving the LRU order.
   *
   * Background:
   * Whenever client metrics subscription is added/updated that results in updating the
   * affected client instance state objects to absorb the new changes, that's done by replacing
   * the old client instance with the new instance object as explained in invalidate method.
   * If we directly replace the cached object that would alter the LRU order, so having the wrapper
   * will allow us to replace the client instance object in the wrapper itself without changing
   * the LRU order.
   * @param instance
   */
  class ClientMetricsCacheValue(instance: CmClientInstanceState) {
    var clientInstance :CmClientInstanceState = instance
    def getClientInstance = clientInstance
    def replace(instance: CmClientInstanceState): Unit = {
      clientInstance = instance
    }
  }
}
