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

import com.typesafe.scalalogging.Logger
import kafka.utils.Logging
import org.apache.kafka.common.{TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.common.message.FetchResponseData
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.FetchMetadata.{FINAL_EPOCH, INITIAL_EPOCH, INVALID_SESSION_ID}
import org.apache.kafka.common.requests.{FetchRequest, FetchResponse, FetchMetadata => JFetchMetadata}
import org.apache.kafka.common.utils.{ImplicitLinkedHashCollection, Time, Utils}
import org.apache.kafka.server.metrics.KafkaMetricsGroup

import java.util
import java.util.Optional
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ThreadLocalRandom, TimeUnit}
import scala.collection.mutable
import scala.math.Ordered.orderingToOrdered

object FetchSession {
  type REQ_MAP = util.Map[TopicIdPartition, FetchRequest.PartitionData]
  type RESP_MAP = util.LinkedHashMap[TopicIdPartition, FetchResponseData.PartitionData]
  type CACHE_MAP = ImplicitLinkedHashCollection[CachedPartition]
  type RESP_MAP_ITER = util.Iterator[util.Map.Entry[TopicIdPartition, FetchResponseData.PartitionData]]
  type TOPIC_NAME_MAP = util.Map[Uuid, String]

  val NUM_INCREMENTAL_FETCH_SESSIONS = "NumIncrementalFetchSessions"
  val NUM_INCREMENTAL_FETCH_PARTITIONS_CACHED = "NumIncrementalFetchPartitionsCached"
  val INCREMENTAL_FETCH_SESSIONS_EVICTIONS_PER_SEC = "IncrementalFetchSessionEvictionsPerSec"
  val EVICTIONS = "evictions"

  def partitionsToLogString(partitions: util.Collection[TopicIdPartition], traceEnabled: Boolean): String = {
    if (traceEnabled) {
      "(" + String.join(", ", partitions.toString) + ")"
    } else {
      s"${partitions.size} partition(s)"
    }
  }
}

/**
  * A cached partition.
  *
  * The broker maintains a set of these objects for each incremental fetch session.
  * When an incremental fetch request is made, any partitions which are not explicitly
  * enumerated in the fetch request are loaded from the cache.  Similarly, when an
  * incremental fetch response is being prepared, any partitions that have not changed and
  * do not have errors are left out of the response.
  *
  * We store many of these objects, so it is important for them to be memory-efficient.
  * That is why we store topic and partition separately rather than storing a TopicPartition
  * object.  The TP object takes up more memory because it is a separate JVM object, and
  * because it stores the cached hash code in memory.
  *
  * Note that fetcherLogStartOffset is the LSO of the follower performing the fetch, whereas
  * localLogStartOffset is the log start offset of the partition on this broker.
  */
class CachedPartition(var topic: String,
                      val topicId: Uuid,
                      val partition: Int,
                      var maxBytes: Int,
                      var fetchOffset: Long,
                      var highWatermark: Long,
                      var leaderEpoch: Optional[Integer],
                      var fetcherLogStartOffset: Long,
                      var localLogStartOffset: Long,
                      var lastFetchedEpoch: Optional[Integer])
    extends ImplicitLinkedHashCollection.Element {

  private var cachedNext: Int = ImplicitLinkedHashCollection.INVALID_INDEX
  private var cachedPrev: Int = ImplicitLinkedHashCollection.INVALID_INDEX

  override def next: Int = cachedNext
  override def setNext(next: Int): Unit = this.cachedNext = next
  override def prev: Int = cachedPrev
  override def setPrev(prev: Int): Unit = this.cachedPrev = prev

  def this(topic: String, topicId: Uuid, partition: Int) =
    this(topic, topicId, partition, -1, -1, -1, Optional.empty(), -1, -1, Optional.empty[Integer])

  def this(part: TopicIdPartition) = {
    this(part.topic, part.topicId, part.partition)
  }

  def this(part: TopicIdPartition, reqData: FetchRequest.PartitionData) =
    this(part.topic, part.topicId, part.partition, reqData.maxBytes, reqData.fetchOffset, -1,
      reqData.currentLeaderEpoch, reqData.logStartOffset, -1, reqData.lastFetchedEpoch)

  def this(part: TopicIdPartition, reqData: FetchRequest.PartitionData,
           respData: FetchResponseData.PartitionData) =
    this(part.topic, part.topicId, part.partition, reqData.maxBytes, reqData.fetchOffset, respData.highWatermark,
      reqData.currentLeaderEpoch, reqData.logStartOffset, respData.logStartOffset, reqData.lastFetchedEpoch)

  def reqData = new FetchRequest.PartitionData(topicId, fetchOffset, fetcherLogStartOffset, maxBytes, leaderEpoch, lastFetchedEpoch)

  def updateRequestParams(reqData: FetchRequest.PartitionData): Unit = {
    // Update our cached request parameters.
    maxBytes = reqData.maxBytes
    fetchOffset = reqData.fetchOffset
    fetcherLogStartOffset = reqData.logStartOffset
    leaderEpoch = reqData.currentLeaderEpoch
    lastFetchedEpoch = reqData.lastFetchedEpoch
  }

  def maybeResolveUnknownName(topicNames: FetchSession.TOPIC_NAME_MAP): Unit = {
    if (this.topic == null) {
      this.topic = topicNames.get(this.topicId)
    }
  }

  /**
    * Determine whether or not the specified cached partition should be included in the FetchResponse we send back to
    * the fetcher and update it if requested.
    *
    * This function should be called while holding the appropriate session lock.
    *
    * @param respData partition data
    * @param updateResponseData if set to true, update this CachedPartition with new request and response data.
    * @return True if this partition should be included in the response; false if it can be omitted.
    */
  def maybeUpdateResponseData(respData: FetchResponseData.PartitionData, updateResponseData: Boolean): Boolean = {
    // Check the response data.
    var mustRespond = false
    if (FetchResponse.recordsSize(respData) > 0) {
      // Partitions with new data are always included in the response.
      mustRespond = true
    }
    if (highWatermark != respData.highWatermark) {
      mustRespond = true
      if (updateResponseData)
        highWatermark = respData.highWatermark
    }
    if (localLogStartOffset != respData.logStartOffset) {
      mustRespond = true
      if (updateResponseData)
        localLogStartOffset = respData.logStartOffset
    }
    if (FetchResponse.isPreferredReplica(respData)) {
      // If the broker computed a preferred read replica, we need to include it in the response
      mustRespond = true
    }
    if (respData.errorCode != Errors.NONE.code) {
      // Partitions with errors are always included in the response.
      // We also set the cached highWatermark to an invalid offset, -1.
      // This ensures that when the error goes away, we re-send the partition.
      if (updateResponseData)
        highWatermark = -1
      mustRespond = true
    }

    if (FetchResponse.isDivergingEpoch(respData)) {
      // Partitions with diverging epoch are always included in response to trigger truncation.
      mustRespond = true
    }
    mustRespond
  }

  /**
   * We have different equality checks depending on whether topic IDs are used.
   * This means we need a different hash function as well. We use name to calculate the hash if the ID is zero and unused.
   * Otherwise, we use the topic ID in the hash calculation.
   *
   * @return the hash code for the CachedPartition depending on what request version we are using.
   */
  override def hashCode: Int =
    if (topicId != Uuid.ZERO_UUID)
      (31 * partition) + topicId.hashCode
    else
      (31 * partition) + topic.hashCode

  /**
   * We have different equality checks depending on whether topic IDs are used.
   *
   * This is because when we use topic IDs, a partition with a given ID and an unknown name is the same as a partition with that
   * ID and a known name. This means we can only use topic ID and partition when determining equality.
   *
   * On the other hand, if we are using topic names, all IDs are zero. This means we can only use topic name and partition
   * when determining equality.
   */
  override def equals(that: Any): Boolean =
    that match {
      case that: CachedPartition =>
        this.eq(that) || (if (this.topicId != Uuid.ZERO_UUID)
          this.partition.equals(that.partition) && this.topicId.equals(that.topicId)
        else
          this.partition.equals(that.partition) && this.topic.equals(that.topic))
      case _ => false
    }

  override def toString: String = synchronized {
    "CachedPartition(topic=" + topic +
      ", topicId=" + topicId +
      ", partition=" + partition +
      ", maxBytes=" + maxBytes +
      ", fetchOffset=" + fetchOffset +
      ", highWatermark=" + highWatermark +
      ", fetcherLogStartOffset=" + fetcherLogStartOffset +
      ", localLogStartOffset=" + localLogStartOffset  +
        ")"
  }
}

/**
  * The fetch session.
  *
  * Each fetch session is protected by its own lock, which must be taken before mutable
  * fields are read or modified.  This includes modification of the session partition map.
  *
  * @param id                 The unique fetch session ID.
  * @param privileged         True if this session is privileged.  Sessions crated by followers
  *                           are privileged; session created by consumers are not.
  * @param partitionMap       The CachedPartitionMap.
  * @param usesTopicIds       True if this session is using topic IDs
  * @param creationMs         The time in milliseconds when this session was created.
  * @param lastUsedMs         The last used time in milliseconds.  This should only be updated by
  *                           FetchSessionCache#touch.
  * @param epoch              The fetch session sequence number.
  */
class FetchSession(val id: Int,
                   val privileged: Boolean,
                   val partitionMap: FetchSession.CACHE_MAP,
                   val usesTopicIds: Boolean,
                   val creationMs: Long,
                   var lastUsedMs: Long,
                   var epoch: Int) {
  // This is used by the FetchSessionCache to store the last known size of this session.
  // If this is -1, the Session is not in the cache.
  var cachedSize = -1

  def size: Int = synchronized {
    partitionMap.size
  }

  def isEmpty: Boolean = synchronized {
    partitionMap.isEmpty
  }

  def lastUsedKey: LastUsedKey = synchronized {
    LastUsedKey(lastUsedMs, id)
  }

  def evictableKey: EvictableKey = synchronized {
    EvictableKey(privileged, cachedSize, id)
  }

  def metadata: JFetchMetadata = synchronized { new JFetchMetadata(id, epoch) }

  def getFetchOffset(topicIdPartition: TopicIdPartition): Option[Long] = synchronized {
    Option(partitionMap.find(new CachedPartition(topicIdPartition))).map(_.fetchOffset)
  }

  private type TL = util.ArrayList[TopicIdPartition]

  // Update the cached partition data based on the request.
  def update(fetchData: FetchSession.REQ_MAP,
             toForget: util.List[TopicIdPartition],
             reqMetadata: JFetchMetadata): (TL, TL, TL) = synchronized {
    val added = new TL
    val updated = new TL
    val removed = new TL
    fetchData.forEach { (topicPart, reqData) =>
      val cachedPartitionKey = new CachedPartition(topicPart, reqData)
      val cachedPart = partitionMap.find(cachedPartitionKey)
      if (cachedPart == null) {
        partitionMap.mustAdd(cachedPartitionKey)
        added.add(topicPart)
      } else {
        cachedPart.updateRequestParams(reqData)
        updated.add(topicPart)
      }
    }
    toForget.forEach { p =>
      if (partitionMap.remove(new CachedPartition(p))) {
        removed.add(p)
      }
    }
    (added, updated, removed)
  }

  override def toString: String = synchronized {
    "FetchSession(id=" + id +
      ", privileged=" + privileged +
      ", partitionMap.size=" + partitionMap.size +
      ", usesTopicIds=" + usesTopicIds +
      ", creationMs=" + creationMs +
      ", lastUsedMs=" + lastUsedMs +
      ", epoch=" + epoch + ")"
  }
}

trait FetchContext extends Logging {
  /**
    * Get the fetch offset for a given partition.
    */
  def getFetchOffset(part: TopicIdPartition): Option[Long]

  /**
    * Apply a function to each partition in the fetch request.
    */
  def foreachPartition(fun: (TopicIdPartition, FetchRequest.PartitionData) => Unit): Unit

  /**
    * Get the response size to be used for quota computation. Since we are returning an empty response in case of
    * throttling, we are not supposed to update the context until we know that we are not going to throttle.
    */
  def getResponseSize(updates: FetchSession.RESP_MAP, versionId: Short): Int

  /**
    * Updates the fetch context with new partition information.  Generates response data.
    * The response data may require subsequent down-conversion.
    */
  def updateAndGenerateResponseData(updates: FetchSession.RESP_MAP): FetchResponse

  def partitionsToLogString(partitions: util.Collection[TopicIdPartition]): String =
    FetchSession.partitionsToLogString(partitions, isTraceEnabled)

  /**
    * Return an empty throttled response due to quota violation.
    */
  def getThrottledResponse(throttleTimeMs: Int): FetchResponse =
    FetchResponse.of(Errors.NONE, throttleTimeMs, INVALID_SESSION_ID, new FetchSession.RESP_MAP)
}

/**
  * The fetch context for a fetch request that had a session error.
  */
class SessionErrorContext(val error: Errors,
                          val reqMetadata: JFetchMetadata) extends FetchContext {
  override def getFetchOffset(part: TopicIdPartition): Option[Long] = None

  override def foreachPartition(fun: (TopicIdPartition, FetchRequest.PartitionData) => Unit): Unit = {}

  override def getResponseSize(updates: FetchSession.RESP_MAP, versionId: Short): Int = {
    FetchResponse.sizeOf(versionId, (new FetchSession.RESP_MAP).entrySet.iterator)
  }

  // Because of the fetch session error, we don't know what partitions were supposed to be in this request.
  override def updateAndGenerateResponseData(updates: FetchSession.RESP_MAP): FetchResponse = {
    debug(s"Session error fetch context returning $error")
    FetchResponse.of(error, 0, INVALID_SESSION_ID, new FetchSession.RESP_MAP)
  }
}

object SessionlessFetchContext {
  private final val logger = Logger(classOf[SessionlessFetchContext])
}

/**
  * The fetch context for a sessionless fetch request.
  *
  * @param fetchData          The partition data from the fetch request.
  */
class SessionlessFetchContext(val fetchData: util.Map[TopicIdPartition, FetchRequest.PartitionData]) extends FetchContext {

  override lazy val logger = SessionlessFetchContext.logger

  override def getFetchOffset(part: TopicIdPartition): Option[Long] =
    Option(fetchData.get(part)).map(_.fetchOffset)

  override def foreachPartition(fun: (TopicIdPartition, FetchRequest.PartitionData) => Unit): Unit = {
    fetchData.forEach((tp, data) => fun(tp, data))
  }

  override def getResponseSize(updates: FetchSession.RESP_MAP, versionId: Short): Int = {
    FetchResponse.sizeOf(versionId, updates.entrySet.iterator)
  }

  override def updateAndGenerateResponseData(updates: FetchSession.RESP_MAP): FetchResponse = {
    debug(s"Sessionless fetch context returning ${partitionsToLogString(updates.keySet)}")
    FetchResponse.of(Errors.NONE, 0, INVALID_SESSION_ID, updates)
  }
}

object FullFetchContext {
  private final val logger = Logger(classOf[FullFetchContext])
}

/**
  * The fetch context for a full fetch request.
  *
  * @param time               The clock to use.
  * @param cache              The fetch session cache.
  * @param reqMetadata        The request metadata.
  * @param fetchData          The partition data from the fetch request.
  * @param usesTopicIds       True if this session should use topic IDs.
  * @param isFromFollower     True if this fetch request came from a follower.
  */
class FullFetchContext(private val time: Time,
                       private val cache: FetchSessionCache,
                       private val reqMetadata: JFetchMetadata,
                       private val fetchData: util.Map[TopicIdPartition, FetchRequest.PartitionData],
                       private val usesTopicIds: Boolean,
                       private val isFromFollower: Boolean) extends FetchContext {

  def this(time: Time,
           cacheShard: FetchSessionCacheShard,
           reqMetadata: JFetchMetadata,
           fetchData: util.Map[TopicIdPartition, FetchRequest.PartitionData],
           usesTopicIds: Boolean,
           isFromFollower: Boolean
          ) = this(time, new FetchSessionCache(Seq(cacheShard)), reqMetadata, fetchData, usesTopicIds, isFromFollower)

  override lazy val logger = FullFetchContext.logger

  override def getFetchOffset(part: TopicIdPartition): Option[Long] =
    Option(fetchData.get(part)).map(_.fetchOffset)

  override def foreachPartition(fun: (TopicIdPartition, FetchRequest.PartitionData) => Unit): Unit = {
    fetchData.forEach((tp, data) => fun(tp, data))
  }

  override def getResponseSize(updates: FetchSession.RESP_MAP, versionId: Short): Int = {
    FetchResponse.sizeOf(versionId, updates.entrySet.iterator)
  }

  override def updateAndGenerateResponseData(updates: FetchSession.RESP_MAP): FetchResponse = {
    def createNewSession: FetchSession.CACHE_MAP = {
      val cachedPartitions = new FetchSession.CACHE_MAP(updates.size)
      updates.forEach { (part, respData) =>
        val reqData = fetchData.get(part)
        cachedPartitions.mustAdd(new CachedPartition(part, reqData, respData))
      }
      cachedPartitions
    }
    val cacheShard = cache.getNextCacheShard
    val responseSessionId = cacheShard.maybeCreateSession(time.milliseconds(), isFromFollower,
        updates.size, usesTopicIds, () => createNewSession)
    debug(s"Full fetch context with session id $responseSessionId returning " +
      s"${partitionsToLogString(updates.keySet)}")
    FetchResponse.of(Errors.NONE, 0, responseSessionId, updates)
  }
}

object IncrementalFetchContext {
  private val logger = Logger(classOf[IncrementalFetchContext])
}

/**
  * The fetch context for an incremental fetch request.
  *
  * @param time         The clock to use.
  * @param reqMetadata  The request metadata.
  * @param session      The incremental fetch request session.
  * @param topicNames   A mapping from topic ID to topic name used to resolve partitions already in the session.
  */
class IncrementalFetchContext(private val time: Time,
                              private val reqMetadata: JFetchMetadata,
                              private val session: FetchSession,
                              private val topicNames: FetchSession.TOPIC_NAME_MAP) extends FetchContext {

  override lazy val logger = IncrementalFetchContext.logger

  override def getFetchOffset(tp: TopicIdPartition): Option[Long] = session.getFetchOffset(tp)

  override def foreachPartition(fun: (TopicIdPartition, FetchRequest.PartitionData) => Unit): Unit = {
    // Take the session lock and iterate over all the cached partitions.
    session.synchronized {
      session.partitionMap.forEach { part =>
        // Try to resolve an unresolved partition if it does not yet have a name
        if (session.usesTopicIds)
          part.maybeResolveUnknownName(topicNames)
        fun(new TopicIdPartition(part.topicId, new TopicPartition(part.topic, part.partition)), part.reqData)
      }
    }
  }

  // Iterator that goes over the given partition map and selects partitions that need to be included in the response.
  // If updateFetchContextAndRemoveUnselected is set to true, the fetch context will be updated for the selected
  // partitions and also remove unselected ones as they are encountered.
  private class PartitionIterator(val iter: FetchSession.RESP_MAP_ITER,
                                  val updateFetchContextAndRemoveUnselected: Boolean)
    extends FetchSession.RESP_MAP_ITER {
    private var nextElement: util.Map.Entry[TopicIdPartition, FetchResponseData.PartitionData] = _

    override def hasNext: Boolean = {
      while ((nextElement == null) && iter.hasNext) {
        val element = iter.next()
        val topicPart = element.getKey
        val respData = element.getValue
        val cachedPart = session.partitionMap.find(new CachedPartition(topicPart))
        val mustRespond = cachedPart.maybeUpdateResponseData(respData, updateFetchContextAndRemoveUnselected)
        if (mustRespond) {
          nextElement = element
          if (updateFetchContextAndRemoveUnselected && FetchResponse.recordsSize(respData) > 0) {
            session.partitionMap.remove(cachedPart)
            session.partitionMap.mustAdd(cachedPart)
          }
        } else {
          if (updateFetchContextAndRemoveUnselected) {
            iter.remove()
          }
        }
      }
      nextElement != null
    }

    override def next(): util.Map.Entry[TopicIdPartition, FetchResponseData.PartitionData] = {
      if (!hasNext) throw new NoSuchElementException
      val element = nextElement
      nextElement = null
      element
    }

    override def remove(): Unit = throw new UnsupportedOperationException
  }

  override def getResponseSize(updates: FetchSession.RESP_MAP, versionId: Short): Int = {
    session.synchronized {
      val expectedEpoch = JFetchMetadata.nextEpoch(reqMetadata.epoch)
      if (session.epoch != expectedEpoch) {
        FetchResponse.sizeOf(versionId, (new FetchSession.RESP_MAP).entrySet.iterator)
      } else {
        // Pass the partition iterator which updates neither the fetch context nor the partition map.
        FetchResponse.sizeOf(versionId, new PartitionIterator(updates.entrySet.iterator, false))
      }
    }
  }

  override def updateAndGenerateResponseData(updates: FetchSession.RESP_MAP): FetchResponse = {
    session.synchronized {
      // Check to make sure that the session epoch didn't change in between
      // creating this fetch context and generating this response.
      val expectedEpoch = JFetchMetadata.nextEpoch(reqMetadata.epoch)
      if (session.epoch != expectedEpoch) {
        info(s"Incremental fetch session ${session.id} expected epoch $expectedEpoch, but " +
          s"got ${session.epoch}.  Possible duplicate request.")
        FetchResponse.of(Errors.INVALID_FETCH_SESSION_EPOCH, 0, session.id, new FetchSession.RESP_MAP)
      } else {
        // Iterate over the update list using PartitionIterator. This will prune updates which don't need to be sent
        val partitionIter = new PartitionIterator(updates.entrySet.iterator, true)
        while (partitionIter.hasNext) {
          partitionIter.next()
        }
        debug(s"Incremental fetch context with session id ${session.id} returning " +
          s"${partitionsToLogString(updates.keySet)}")
        FetchResponse.of(Errors.NONE, 0, session.id, updates)
      }
    }
  }

  override def getThrottledResponse(throttleTimeMs: Int): FetchResponse = {
    session.synchronized {
      // Check to make sure that the session epoch didn't change in between
      // creating this fetch context and generating this response.
      val expectedEpoch = JFetchMetadata.nextEpoch(reqMetadata.epoch)
      if (session.epoch != expectedEpoch) {
        info(s"Incremental fetch session ${session.id} expected epoch $expectedEpoch, but " +
          s"got ${session.epoch}.  Possible duplicate request.")
        FetchResponse.of(Errors.INVALID_FETCH_SESSION_EPOCH, throttleTimeMs, session.id, new FetchSession.RESP_MAP)
      } else {
        FetchResponse.of(Errors.NONE, throttleTimeMs, session.id, new FetchSession.RESP_MAP)
      }
    }
  }
}

case class LastUsedKey(lastUsedMs: Long, id: Int) extends Comparable[LastUsedKey] {
  override def compareTo(other: LastUsedKey): Int =
    (lastUsedMs, id) compare (other.lastUsedMs, other.id)
}

case class EvictableKey(privileged: Boolean, size: Int, id: Int) extends Comparable[EvictableKey] {
  override def compareTo(other: EvictableKey): Int =
    (privileged, size, id) compare (other.privileged, other.size, other.id)
}


/**
  * Caches fetch sessions.
  *
  * See tryEvict for an explanation of the cache eviction strategy.
  *
  * The FetchSessionCache is thread-safe because all of its methods are synchronized.
  * Note that individual fetch sessions have their own locks which are separate from the
  * FetchSessionCache lock.  In order to avoid deadlock, the FetchSessionCache lock
  * must never be acquired while an individual FetchSession lock is already held.
  *
  * @param maxEntries The maximum number of entries that can be in the cache.
  * @param evictionMs The minimum time that an entry must be unused in order to be evictable.
  * @param sessionIdRange The number of sessionIds each cache shard handles. For a given instance, Math.max(1, shardNum * sessionIdRange) <= sessionId < (shardNum + 1) * sessionIdRange always holds.
  * @param shardNum Identifier for this shard.
 */
class FetchSessionCacheShard(private val maxEntries: Int,
                             private val evictionMs: Long,
                             val sessionIdRange: Int = Int.MaxValue,
                             private val shardNum: Int = 0) extends Logging {

  this.logIdent = s"[Shard $shardNum] "

  private var numPartitions: Long = 0

  // A map of session ID to FetchSession.
  private val sessions = new mutable.HashMap[Int, FetchSession]

  // Maps last used times to sessions.
  private val lastUsed = new util.TreeMap[LastUsedKey, FetchSession]

  // A map containing sessions which can be evicted by both privileged and
  // unprivileged sessions.
  private val evictableByAll = new util.TreeMap[EvictableKey, FetchSession]

  // A map containing sessions which can be evicted by privileged sessions.
  private val evictableByPrivileged = new util.TreeMap[EvictableKey, FetchSession]

  // This metric is shared across all shards because newMeter returns an existing metric
  // if one exists with the same name. It's safe for concurrent use because Meter is thread-safe.
  private[server] val evictionsMeter = FetchSessionCache.metricsGroup.newMeter(FetchSession.INCREMENTAL_FETCH_SESSIONS_EVICTIONS_PER_SEC,
    FetchSession.EVICTIONS, TimeUnit.SECONDS)

  /**
    * Get a session by session ID.
    *
    * @param sessionId  The session ID.
    * @return           The session, or None if no such session was found.
    */
  def get(sessionId: Int): Option[FetchSession] = synchronized {
    sessions.get(sessionId)
  }

  /**
    * Get the number of entries currently in the fetch session cache.
    */
  def size: Int = synchronized {
    sessions.size
  }

  /**
    * Get the total number of cached partitions.
    */
  def totalPartitions: Long = synchronized {
    numPartitions
  }

  /**
    * Creates a new random session ID.  The new session ID will be positive and unique on this broker.
    *
    * @return   The new session ID.
    */
  def newSessionId(): Int = synchronized {
    var id = 0
    do {
      id = ThreadLocalRandom.current().nextInt(Math.max(1, shardNum * sessionIdRange), (shardNum + 1) * sessionIdRange)
    } while (sessions.contains(id) || id == INVALID_SESSION_ID)
    id
  }

  /**
    * Try to create a new session.
    *
    * @param now                The current time in milliseconds.
    * @param privileged         True if the new entry we are trying to create is privileged.
    * @param size               The number of cached partitions in the new entry we are trying to create.
    * @param usesTopicIds       True if this session should use topic IDs.
    * @param createPartitions   A callback function which creates the map of cached partitions and the mapping from
    *                           topic name to topic ID for the topics.
    * @return                   If we created a session, the ID; INVALID_SESSION_ID otherwise.
    */
  def maybeCreateSession(now: Long,
                         privileged: Boolean,
                         size: Int,
                         usesTopicIds: Boolean,
                         createPartitions: () => FetchSession.CACHE_MAP): Int =
  synchronized {
    // If there is room, create a new session entry.
    if ((sessions.size < maxEntries) ||
        tryEvict(privileged, EvictableKey(privileged, size, 0), now)) {
      val partitionMap = createPartitions()
      val session = new FetchSession(newSessionId(), privileged, partitionMap, usesTopicIds,
          now, now, JFetchMetadata.nextEpoch(INITIAL_EPOCH))
      debug(s"Created fetch session ${session.toString}")
      sessions.put(session.id, session)
      touch(session, now)
      session.id
    } else {
      debug(s"No fetch session created for privileged=$privileged, size=$size.")
      INVALID_SESSION_ID
    }
  }

  /**
    * Try to evict an entry from the session cache.
    *
    * A proposed new element A may evict an existing element B if:
    * 1. A is privileged and B is not, or
    * 2. B is considered "stale" because it has been inactive for a long time, or
    * 3. A contains more partitions than B, and B is not recently created.
    *
    * Prior to KAFKA-9401, the session cache was not sharded and we looked at all
    * entries while considering those eligible for eviction. Now eviction is done
    * by considering entries on a per-shard basis.
    *
    * @param privileged True if the new entry we would like to add is privileged.
    * @param key        The EvictableKey for the new entry we would like to add.
    * @param now        The current time in milliseconds.
    * @return           True if an entry was evicted; false otherwise.
    */
  private def tryEvict(privileged: Boolean, key: EvictableKey, now: Long): Boolean = synchronized {
    // Try to evict an entry which is stale.
    val lastUsedEntry = lastUsed.firstEntry
    if (lastUsedEntry == null) {
      trace("There are no cache entries to evict.")
      false
    } else if (now - lastUsedEntry.getKey.lastUsedMs > evictionMs) {
      val session = lastUsedEntry.getValue
      trace(s"Evicting stale FetchSession ${session.id}.")
      remove(session)
      evictionsMeter.mark()
      true
    } else {
      // If there are no stale entries, check the first evictable entry.
      // If it is less valuable than our proposed entry, evict it.
      val map = if (privileged) evictableByPrivileged else evictableByAll
      val evictableEntry = map.firstEntry
      if (evictableEntry == null) {
        trace("No evictable entries found.")
        false
      } else if (key.compareTo(evictableEntry.getKey) < 0) {
        trace(s"Can't evict ${evictableEntry.getKey} with ${key.toString}")
        false
      } else {
        trace(s"Evicting ${evictableEntry.getKey} with ${key.toString}.")
        remove(evictableEntry.getValue)
        evictionsMeter.mark()
        true
      }
    }
  }

  def remove(sessionId: Int): Option[FetchSession] = synchronized {
    get(sessionId) match {
      case None => None
      case Some(session) => remove(session)
    }
  }

  /**
    * Remove an entry from the session cache.
    *
    * @param session  The session.
    *
    * @return         The removed session, or None if there was no such session.
    */
  def remove(session: FetchSession): Option[FetchSession] = synchronized {
    val evictableKey = session.synchronized {
      lastUsed.remove(session.lastUsedKey)
      session.evictableKey
    }
    evictableByAll.remove(evictableKey)
    evictableByPrivileged.remove(evictableKey)
    val removeResult = sessions.remove(session.id)
    if (removeResult.isDefined) {
      numPartitions = numPartitions - session.cachedSize
    }
    removeResult
  }

  /**
    * Update a session's position in the lastUsed and evictable trees.
    *
    * @param session  The session.
    * @param now      The current time in milliseconds.
    */
  def touch(session: FetchSession, now: Long): Unit = synchronized {
    session.synchronized {
      // Update the lastUsed map.
      lastUsed.remove(session.lastUsedKey)
      session.lastUsedMs = now
      lastUsed.put(session.lastUsedKey, session)

      val oldSize = session.cachedSize
      if (oldSize != -1) {
        val oldEvictableKey = session.evictableKey
        evictableByPrivileged.remove(oldEvictableKey)
        evictableByAll.remove(oldEvictableKey)
        numPartitions = numPartitions - oldSize
      }
      session.cachedSize = session.size
      val newEvictableKey = session.evictableKey
      if ((!session.privileged) || (now - session.creationMs > evictionMs)) {
        evictableByPrivileged.put(newEvictableKey, session)
      }
      if (now - session.creationMs > evictionMs) {
        evictableByAll.put(newEvictableKey, session)
      }
      numPartitions = numPartitions + session.cachedSize
    }
  }
}
object FetchSessionCache {
  private[server] val metricsGroup = new KafkaMetricsGroup(classOf[FetchSessionCache])
  private[server] val counter = new AtomicInteger(0)
}

class FetchSessionCache(private val cacheShards: Seq[FetchSessionCacheShard]) {
  // Set up metrics.
  FetchSessionCache.metricsGroup.newGauge(FetchSession.NUM_INCREMENTAL_FETCH_SESSIONS, () => cacheShards.map(_.size).sum)
  FetchSessionCache.metricsGroup.newGauge(FetchSession.NUM_INCREMENTAL_FETCH_PARTITIONS_CACHED, () => cacheShards.map(_.totalPartitions).sum)

  def getCacheShard(sessionId: Int): FetchSessionCacheShard = {
    val shard = sessionId / cacheShards.head.sessionIdRange
    // This assumes that cacheShards is sorted by shardNum
    cacheShards(shard)
  }

  // Returns the shard in round-robin
  def getNextCacheShard: FetchSessionCacheShard = {
    val shardNum = Utils.toPositive(FetchSessionCache.counter.getAndIncrement()) % size
    cacheShards(shardNum)
  }

  def size: Int = {
    cacheShards.size
  }
}

class FetchManager(private val time: Time,
                   private val cache: FetchSessionCache) extends Logging {

  def this(time: Time, cacheShard: FetchSessionCacheShard) = this(time, new FetchSessionCache(Seq(cacheShard)))

  def newContext(reqVersion: Short,
                 reqMetadata: JFetchMetadata,
                 isFollower: Boolean,
                 fetchData: FetchSession.REQ_MAP,
                 toForget: util.List[TopicIdPartition],
                 topicNames: FetchSession.TOPIC_NAME_MAP): FetchContext = {
    val context = if (reqMetadata.isFull) {
      var removedFetchSessionStr = ""
      if (reqMetadata.sessionId != INVALID_SESSION_ID) {
        val cacheShard = cache.getCacheShard(reqMetadata.sessionId())
        // Any session specified in a FULL fetch request will be closed.
        if (cacheShard.remove(reqMetadata.sessionId).isDefined) {
          removedFetchSessionStr = s" Removed fetch session ${reqMetadata.sessionId}."
        }
      }
      var suffix = ""
      val context = if (reqMetadata.epoch == FINAL_EPOCH) {
        // If the epoch is FINAL_EPOCH, don't try to create a new session.
        suffix = " Will not try to create a new session."
        new SessionlessFetchContext(fetchData)
      } else {
        new FullFetchContext(time, cache, reqMetadata, fetchData, reqVersion >= 13, isFollower)
      }
      debug(s"Created a new full FetchContext with ${partitionsToLogString(fetchData.keySet)}."+
        s"$removedFetchSessionStr$suffix")
      context
    } else {
      val cacheShard = cache.getCacheShard(reqMetadata.sessionId())
      cacheShard.synchronized {
        cacheShard.get(reqMetadata.sessionId) match {
          case None => {
            debug(s"Session error for ${reqMetadata.sessionId}: no such session ID found.")
            new SessionErrorContext(Errors.FETCH_SESSION_ID_NOT_FOUND, reqMetadata)
          }
          case Some(session) => session.synchronized {
            if (session.epoch != reqMetadata.epoch) {
              debug(s"Session error for ${reqMetadata.sessionId}: expected epoch " +
                s"${session.epoch}, but got ${reqMetadata.epoch} instead.")
              new SessionErrorContext(Errors.INVALID_FETCH_SESSION_EPOCH, reqMetadata)
            } else if (session.usesTopicIds && reqVersion < 13 || !session.usesTopicIds && reqVersion >= 13)  {
              debug(s"Session error for ${reqMetadata.sessionId}: expected  " +
                s"${if (session.usesTopicIds) "to use topic IDs" else "to not use topic IDs"}" +
                s", but request version $reqVersion means that we can not.")
              new SessionErrorContext(Errors.FETCH_SESSION_TOPIC_ID_ERROR, reqMetadata)
            } else {
              val (added, updated, removed) = session.update(fetchData, toForget, reqMetadata)
              if (session.isEmpty) {
                debug(s"Created a new sessionless FetchContext and closing session id ${session.id}, " +
                  s"epoch ${session.epoch}: after removing ${partitionsToLogString(removed)}, " +
                  s"there are no more partitions left.")
                cacheShard.remove(session)
                new SessionlessFetchContext(fetchData)
              } else {
                cacheShard.touch(session, time.milliseconds())
                session.epoch = JFetchMetadata.nextEpoch(session.epoch)
                debug(s"Created a new incremental FetchContext for session id ${session.id}, " +
                  s"epoch ${session.epoch}: added ${partitionsToLogString(added)}, " +
                  s"updated ${partitionsToLogString(updated)}, " +
                  s"removed ${partitionsToLogString(removed)}")
                new IncrementalFetchContext(time, reqMetadata, session, topicNames)
              }
            }
          }
        }
      }
    }
    context
  }

  private def partitionsToLogString(partitions: util.Collection[TopicIdPartition]): String =
    FetchSession.partitionsToLogString(partitions, isTraceEnabled)
}
