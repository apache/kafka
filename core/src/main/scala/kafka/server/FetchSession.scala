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

import kafka.metrics.KafkaMetricsGroup
import kafka.utils.Logging
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.common.message.{FetchRequestData, FetchResponseData}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.FetchMetadata.{FINAL_EPOCH, INITIAL_EPOCH, INVALID_SESSION_ID}
import org.apache.kafka.common.requests.FetchRequest.FetchDataAndError
import org.apache.kafka.common.requests.{FetchRequest, FetchResponse, FetchMetadata => JFetchMetadata}
import org.apache.kafka.common.utils.{ImplicitLinkedHashCollection, Time, Utils}
import java.util
import java.util.{Collections, Optional}
import java.util.concurrent.{ThreadLocalRandom, TimeUnit}

import scala.collection.{mutable, _}
import scala.math.Ordered.orderingToOrdered

object FetchSession {
  type REQ_MAP = util.Map[TopicPartition, FetchRequest.PartitionData]
  type RESP_MAP = util.LinkedHashMap[TopicPartition, FetchResponseData.PartitionData]
  type CACHE_MAP = ImplicitLinkedHashCollection[CachedPartition]
  type UNRESOLVED_CACHE = util.HashSet[CachedUnresolvedPartition]
  type RESP_MAP_ITER = util.Iterator[util.Map.Entry[TopicPartition, FetchResponseData.PartitionData]]
  type TOPIC_ID_MAP = util.Map[String, Uuid]
  type UNRESOLVED_DATA = util.ArrayList[FetchResponseData.FetchableTopicResponse]

  val NUM_INCREMENTAL_FETCH_SESSISONS = "NumIncrementalFetchSessions"
  val NUM_INCREMENTAL_FETCH_PARTITIONS_CACHED = "NumIncrementalFetchPartitionsCached"
  val INCREMENTAL_FETCH_SESSIONS_EVICTIONS_PER_SEC = "IncrementalFetchSessionEvictionsPerSec"
  val EVICTIONS = "evictions"

  def partitionsToLogString(partitions: util.Collection[TopicPartition], traceEnabled: Boolean): String = {
    if (traceEnabled) {
      "(" + Utils.join(partitions, ", ") + ")"
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
class CachedPartition(val topic: String,
                      var topicId: Uuid,
                      val partition: Int,
                      var maxBytes: Int,
                      var fetchOffset: Long,
                      var highWatermark: Long,
                      var leaderEpoch: Optional[Integer],
                      var fetcherLogStartOffset: Long,
                      var localLogStartOffset: Long,
                      var lastFetchedEpoch: Optional[Integer])
    extends ImplicitLinkedHashCollection.Element {

  var cachedNext: Int = ImplicitLinkedHashCollection.INVALID_INDEX
  var cachedPrev: Int = ImplicitLinkedHashCollection.INVALID_INDEX

  override def next: Int = cachedNext
  override def setNext(next: Int): Unit = this.cachedNext = next
  override def prev: Int = cachedPrev
  override def setPrev(prev: Int): Unit = this.cachedPrev = prev

  def this(topic: String, partition: Int) =
    this(topic, Uuid.ZERO_UUID, partition, -1, -1, -1, Optional.empty(), -1, -1, Optional.empty[Integer])

  def this(part: TopicPartition) =
    this(part.topic, part.partition)

  def this(part: TopicPartition, reqData: FetchRequest.PartitionData) =
    this(part.topic, Uuid.ZERO_UUID, part.partition, reqData.maxBytes, reqData.fetchOffset, -1,
      reqData.currentLeaderEpoch, reqData.logStartOffset, -1, reqData.lastFetchedEpoch)

  def this(part: TopicPartition, id: Uuid, reqData: FetchRequest.PartitionData) =
    this(part.topic, id, part.partition, reqData.maxBytes, reqData.fetchOffset, -1,
      reqData.currentLeaderEpoch, reqData.logStartOffset, -1, reqData.lastFetchedEpoch)

  def this(part: TopicPartition, reqData: FetchRequest.PartitionData,
           respData: FetchResponseData.PartitionData) =
    this(part.topic, Uuid.ZERO_UUID, part.partition, reqData.maxBytes, reqData.fetchOffset, respData.highWatermark,
      reqData.currentLeaderEpoch, reqData.logStartOffset, respData.logStartOffset, reqData.lastFetchedEpoch)

  def this(part: TopicPartition, id: Uuid, reqData: FetchRequest.PartitionData,
           respData: FetchResponseData.PartitionData) =
    this(part.topic, id, part.partition, reqData.maxBytes, reqData.fetchOffset, respData.highWatermark,
      reqData.currentLeaderEpoch, reqData.logStartOffset, respData.logStartOffset, reqData.lastFetchedEpoch)

  def reqData = new FetchRequest.PartitionData(fetchOffset, fetcherLogStartOffset, maxBytes, leaderEpoch, lastFetchedEpoch)

  def updateRequestParams(reqData: FetchRequest.PartitionData): Unit = {
    // Update our cached request parameters.
    maxBytes = reqData.maxBytes
    fetchOffset = reqData.fetchOffset
    fetcherLogStartOffset = reqData.logStartOffset
    leaderEpoch = reqData.currentLeaderEpoch
    lastFetchedEpoch = reqData.lastFetchedEpoch
  }

  def addId(id: Uuid): Unit = {
    topicId = id
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

  override def hashCode: Int = (31 * partition) + topic.hashCode

  def canEqual(that: Any): Boolean = that.isInstanceOf[CachedPartition]

  override def equals(that: Any): Boolean =
    that match {
      case that: CachedPartition =>
        this.eq(that) ||
          (that.canEqual(this) &&
            this.partition.equals(that.partition) &&
            this.topic.equals(that.topic))
      case _ => false
    }

  override def toString: String = synchronized {
    "CachedPartition(topic=" + topic +
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
 * Very similar to CachedPartition above, CachedUnresolvedPartition is used for incremental fetch requests.
 * These objects store partitions that had topic IDs that could not be resolved by the broker.
 *
 * Upon each incremental request in the session, these partitions will be loaded. They can either be removed
 * through resolving the partition with the broker's topicNames map or by receiving an unresolved toForget ID.
 *
 * Since these partitions signify an error, they will always be returned in the response.
 */

class CachedUnresolvedPartition(val topicId: Uuid,
                                val partition: Int,
                                var maxBytes: Int,
                                var fetchOffset: Long,
                                var leaderEpoch: Optional[Integer],
                                var fetcherLogStartOffset: Long,
                                var lastFetchedEpoch: Optional[Integer])
  extends ImplicitLinkedHashCollection.Element {

  var cachedNext: Int = ImplicitLinkedHashCollection.INVALID_INDEX
  var cachedPrev: Int = ImplicitLinkedHashCollection.INVALID_INDEX

  override def next: Int = cachedNext
  override def setNext(next: Int): Unit = this.cachedNext = next
  override def prev: Int = cachedPrev
  override def setPrev(prev: Int): Unit = this.cachedPrev = prev

  def this(id: Uuid, partition: Int) =
    this(id, partition, -1, -1, Optional.empty(), -1, Optional.empty[Integer])

  def this(id: Uuid, partition: Int, reqData: FetchRequest.PartitionData) =
    this(id, partition, reqData.maxBytes, reqData.fetchOffset,
      reqData.currentLeaderEpoch, reqData.logStartOffset, reqData.lastFetchedEpoch)

  def reqData = new FetchRequest.PartitionData(fetchOffset, fetcherLogStartOffset, maxBytes, leaderEpoch, lastFetchedEpoch)

  override def hashCode: Int = (31 * partition) + topicId.hashCode

  def canEqual(that: Any) = that.isInstanceOf[CachedUnresolvedPartition]

  override def equals(that: Any): Boolean =
    that match {
      case that: CachedUnresolvedPartition =>
        this.eq(that) ||
          (that.canEqual(this) &&
            this.partition.equals(that.partition) &&
            this.topicId.equals(that.topicId))
      case _ => false
    }

  override def toString: String = synchronized {
    "CachedPartition(Id=" + topicId +
      ", partition=" + partition +
      ", maxBytes=" + maxBytes +
      ", fetchOffset=" + fetchOffset +
      ", fetcherLogStartOffset=" + fetcherLogStartOffset +
      ")"
  }
}

/**
  * The fetch session.
  *
  * Each fetch session is protected by its own lock, which must be taken before mutable
  * fields are read or modified.  This includes modification of the session partition map.
  *
  * For fetch versions 13 or greater, partitionMap will only contain partitions whose topic IDs were resolved on the
  * server. unresolvedPartitions will contain those partitions whose topic IDs could not be resolved. All partitions
  * in the request will be in one map or the other with is no overlap. Partitions in partitionMap can return records,
  * but partitions in unresolvedPartitions will always return an error. On subsequent incremental fetch requests,
  * unresolved partitions may be resolved if the server received metadata containing the topic ID. In this case,
  * the partition will be moved from unresolvedPartitions to partitionMap.
  *
  * @param id                     The unique fetch session ID.
  * @param privileged             True if this session is privileged.  Sessions crated by followers
  *                               are privileged; session created by consumers are not.
  * @param partitionMap           The CachedPartitionMap.
 *  @param unresolvedPartitions   The CachedUnresolvedPartitionMap containing all partitions with topic IDs that
 *                                were not found on the server.
  * @param creationMs             The time in milliseconds when this session was created.
  * @param lastUsedMs             The last used time in milliseconds.  This should only be updated by
  *                               FetchSessionCache#touch.
  * @param epoch                  The fetch session sequence number.
  */
class FetchSession(val id: Int,
                   val privileged: Boolean,
                   val partitionMap: FetchSession.CACHE_MAP,
                   val unresolvedPartitions: FetchSession.UNRESOLVED_CACHE,
                   val creationMs: Long,
                   var lastUsedMs: Long,
                   var epoch: Int) {
  // This is used by the FetchSessionCache to store the last known size of this session.
  // If this is -1, the Session is not in the cache.
  var cachedSize = -1

  def size: Int = synchronized {
    partitionMap.size + unresolvedPartitions.size
  }

  def isEmpty: Boolean = synchronized {
    partitionMap.isEmpty && unresolvedPartitions.isEmpty
  }

  def lastUsedKey: LastUsedKey = synchronized {
    LastUsedKey(lastUsedMs, id)
  }

  def evictableKey: EvictableKey = synchronized {
    EvictableKey(privileged, cachedSize, id)
  }

  def metadata: JFetchMetadata = synchronized { new JFetchMetadata(id, epoch) }

  def getFetchOffset(topicPartition: TopicPartition): Option[Long] = synchronized {
    Option(partitionMap.find(new CachedPartition(topicPartition))).map(_.fetchOffset)
  }

  type TL = util.ArrayList[TopicPartition]

  // Update the cached partition data based on the request.
  def update(version: Short,
             fetchDataAndError: FetchDataAndError,
             toForget: util.List[FetchRequestData.ForgottenTopic],
             reqMetadata: JFetchMetadata,
             topicIds: util.Map[String, Uuid],
             topicNames: util.Map[Uuid, String]): (TL, TL, TL) = synchronized {
    val added = new TL
    val updated = new TL
    val removed = new TL

    // Only make changes to unresolvedPartitions if we have a new request version.
    // If we receive an old request version, ignore all topic ID code, keep IDs that are there.
    if (version >= 13) {
      val unresolvedIterator = unresolvedPartitions.iterator()
      while (unresolvedIterator.hasNext()) {
        val partition = unresolvedIterator.next()

        if (topicNames.get(partition.topicId) != null) {
          // Try to resolve ID, if there is a name for the given ID, add or update a CachedPartition in partitionMap
          // and remove from unresolvedPartitions.
          val newTp = new TopicPartition(topicNames.get(partition.topicId), partition.partition)
          val newCp = new CachedPartition(newTp, partition.topicId, partition.reqData)
          val cachedPart = partitionMap.find(newCp)
          if (cachedPart == null) {
            partitionMap.mustAdd(newCp)
            added.add(newTp)
          } else {
            cachedPart.updateRequestParams(partition.reqData)
            updated.add(newTp)
          }
          unresolvedIterator.remove()
        }
      }

      // Add new unresolved IDs.
      fetchDataAndError.unresolvedPartitions.forEach { idAndData =>
        idAndData.partitionData.forEach { (part, reqData) =>
          unresolvedPartitions.add(new CachedUnresolvedPartition(idAndData.id, part, reqData))
        }
      }
    }

    fetchDataAndError.fetchData.forEach { (topicPart, reqData) =>
      val newCachedPart = new CachedPartition(topicPart, topicIds.getOrDefault(topicPart.topic(), Uuid.ZERO_UUID), reqData)
      val cachedPart = partitionMap.find(newCachedPart)
      if (cachedPart == null) {
        partitionMap.mustAdd(newCachedPart)
        added.add(topicPart)
      } else {
        cachedPart.updateRequestParams(reqData)
        updated.add(topicPart)
      }
    }

    toForget.forEach { topicToForget =>
      if (topicToForget.topic() == "") {
        topicToForget.partitions().forEach { partitionToForget =>
          unresolvedPartitions.remove(new CachedUnresolvedPartition(topicToForget.topicId(), partitionToForget))
        }
      } else {
        topicToForget.partitions().forEach { partitionToForget =>
          if (partitionMap.remove(new CachedPartition(topicToForget.topic(), partitionToForget)))
            removed.add(new TopicPartition(topicToForget.topic(), partitionToForget))
        }
      }
    }
    (added, updated, removed)
  }

  override def toString: String = synchronized {
    "FetchSession(id=" + id +
      ", privileged=" + privileged +
      ", partitionMap.size=" + partitionMap.size +
      ", creationMs=" + creationMs +
      ", lastUsedMs=" + lastUsedMs +
      ", epoch=" + epoch + ")"
  }
}

trait FetchContext extends Logging {
  /**
    * Get the fetch offset for a given partition.
    */
  def getFetchOffset(part: TopicPartition): Option[Long]

  /**
    * Apply a function to each resolved partition in the fetch request.
    */
  def foreachResolvedPartition(fun: (TopicPartition, FetchRequest.PartitionData) => Unit): Unit

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

  def partitionsToLogString(partitions: util.Collection[TopicPartition]): String =
    FetchSession.partitionsToLogString(partitions, isTraceEnabled)

  /**
    * Return an empty throttled response due to quota violation.
    */
  def getThrottledResponse(throttleTimeMs: Int): FetchResponse =
    FetchResponse.of(Errors.NONE, throttleTimeMs, INVALID_SESSION_ID, new FetchSession.RESP_MAP)

  /**
   * Return unresolved partition data in the form of a list of FetchResponseData.FetchableTopicResponse
   */
  def getUnresolvedTopicData(): util.List[FetchResponseData.FetchableTopicResponse]
}

/**
  * The fetch context for a fetch request that had a session error.
  */
class SessionErrorContext(val error: Errors,
                          val reqMetadata: JFetchMetadata) extends FetchContext {
  override def getFetchOffset(part: TopicPartition): Option[Long] = None

  override def foreachResolvedPartition(fun: (TopicPartition, FetchRequest.PartitionData) => Unit): Unit = {}

  override def getResponseSize(updates: FetchSession.RESP_MAP, versionId: Short): Int = {
    FetchResponse.sizeOf(versionId, (new FetchSession.RESP_MAP).entrySet.iterator, Collections.emptyList(), Collections.emptyMap())
  }

  // Because of the fetch session error, we don't know what partitions were supposed to be in this request.
  override def updateAndGenerateResponseData(updates: FetchSession.RESP_MAP): FetchResponse = {
    debug(s"Session error fetch context returning $error")
    FetchResponse.of(error, 0, INVALID_SESSION_ID, new FetchSession.RESP_MAP)
  }

  override def getUnresolvedTopicData(): util.List[FetchResponseData.FetchableTopicResponse] = {
    new FetchSession.UNRESOLVED_DATA
  }

}

/**
  * The fetch context for a sessionless fetch request.
  *
  * @param fetchDataAndError          The partition data and ID errors from the fetch request.
  * @param topicIds                   The map from topic name to topic IDs
  */
class SessionlessFetchContext(val fetchDataAndError: FetchRequest.FetchDataAndError,
                              val topicIds: util.Map[String, Uuid]) extends FetchContext {
  val unresolvedTopicData = generateUnresolvedTopicData(topicIds)
  override def getFetchOffset(part: TopicPartition): Option[Long] =
    Option(fetchDataAndError.fetchData.get(part)).map(_.fetchOffset)

  override def foreachResolvedPartition(fun: (TopicPartition, FetchRequest.PartitionData) => Unit): Unit = {
    fetchDataAndError.fetchData.forEach(fun(_, _))
  }

  private def generateUnresolvedTopicData(topicIds: util.Map[String, Uuid]): util.List[FetchResponseData.FetchableTopicResponse] = {
    val topicResponses = new util.ArrayList[FetchResponseData.FetchableTopicResponse]()
    val error = if (topicIds.isEmpty) Errors.UNSUPPORTED_VERSION else Errors.UNKNOWN_TOPIC_ID

    def buildPartitionData(partition: Int): FetchResponseData.PartitionData = {
      new FetchResponseData.PartitionData()
        .setPartitionIndex(partition)
        .setErrorCode(error.code)
        .setHighWatermark(FetchResponse.INVALID_HIGH_WATERMARK)
        .setLastStableOffset(FetchResponse.INVALID_LAST_STABLE_OFFSET)
        .setLogStartOffset(FetchResponse.INVALID_LOG_START_OFFSET)
        .setAbortedTransactions(null)
        .setPreferredReadReplica(FetchResponse.INVALID_PREFERRED_REPLICA_ID)
    }

    fetchDataAndError.unresolvedPartitions.forEach(unresolvedTopic => {
      val partitionResponses = new util.ArrayList[FetchResponseData.PartitionData]
      unresolvedTopic.partitionData().forEach((partitionIdx, _) => partitionResponses.add(buildPartitionData(partitionIdx)))
      topicResponses.add(new FetchResponseData.FetchableTopicResponse()
        .setTopicId(unresolvedTopic.id())
        .setPartitions(partitionResponses)
      )
    })
    topicResponses
  }

  override def getResponseSize(updates: FetchSession.RESP_MAP, versionId: Short): Int = {
    FetchResponse.sizeOf(versionId, updates.entrySet.iterator, unresolvedTopicData, topicIds)
  }

  override def updateAndGenerateResponseData(updates: FetchSession.RESP_MAP): FetchResponse = {
    debug(s"Sessionless fetch context returning ${partitionsToLogString(updates.keySet)}")
    FetchResponse.prepareResponse(Errors.NONE, updates, unresolvedTopicData, topicIds, 0, INVALID_SESSION_ID)
  }

  override def getUnresolvedTopicData(): util.List[FetchResponseData.FetchableTopicResponse] = {
    unresolvedTopicData
  }
}

/**
  * The fetch context for a full fetch request.
  *
  * @param time               The clock to use.
  * @param cache              The fetch session cache.
  * @param reqMetadata        The request metadata.
  * @param fetchDataAndError  The partition data and topic ID errors from the fetch request.
  * @param topicIds           The map from topic name to topic IDs
  * @param isFromFollower     True if this fetch request came from a follower.
  */
class FullFetchContext(private val time: Time,
                       private val cache: FetchSessionCache,
                       private val reqMetadata: JFetchMetadata,
                       private val fetchDataAndError: FetchDataAndError,
                       private val topicIds: util.Map[String, Uuid],
                       private val isFromFollower: Boolean) extends FetchContext {
  val unresolvedTopicData = generateUnresolvedTopicData(topicIds)
  override def getFetchOffset(part: TopicPartition): Option[Long] =
    Option(fetchDataAndError.fetchData.get(part)).map(_.fetchOffset)

  override def foreachResolvedPartition(fun: (TopicPartition, FetchRequest.PartitionData) => Unit): Unit = {
    fetchDataAndError.fetchData.forEach(fun(_, _))
  }

  private def generateUnresolvedTopicData(topicIds: util.Map[String, Uuid]): util.List[FetchResponseData.FetchableTopicResponse] = {
    val topicResponses = new util.ArrayList[FetchResponseData.FetchableTopicResponse]()
    val error = if (topicIds.isEmpty) Errors.UNSUPPORTED_VERSION else Errors.UNKNOWN_TOPIC_ID

    def buildPartitionData(partition: Int): FetchResponseData.PartitionData = {
      new FetchResponseData.PartitionData()
        .setPartitionIndex(partition)
        .setErrorCode(error.code)
        .setHighWatermark(FetchResponse.INVALID_HIGH_WATERMARK)
        .setLastStableOffset(FetchResponse.INVALID_LAST_STABLE_OFFSET)
        .setLogStartOffset(FetchResponse.INVALID_LOG_START_OFFSET)
        .setAbortedTransactions(null)
        .setPreferredReadReplica(FetchResponse.INVALID_PREFERRED_REPLICA_ID)
    }

    fetchDataAndError.unresolvedPartitions.forEach(unresolvedTopic => {
      val partitionResponses = new util.ArrayList[FetchResponseData.PartitionData]
      unresolvedTopic.partitionData().forEach((partitionIdx, _) => partitionResponses.add(buildPartitionData(partitionIdx)))
      topicResponses.add(new FetchResponseData.FetchableTopicResponse()
        .setTopicId(unresolvedTopic.id())
        .setPartitions(partitionResponses)
      )
    })
    topicResponses
  }

  override def getResponseSize(updates: FetchSession.RESP_MAP, versionId: Short): Int = {
    FetchResponse.sizeOf(versionId, updates.entrySet.iterator, unresolvedTopicData, topicIds)
  }

  override def updateAndGenerateResponseData(updates: FetchSession.RESP_MAP): FetchResponse = {
    def generateResolvedPartitions: FetchSession.CACHE_MAP = {
      val cachedPartitions = new FetchSession.CACHE_MAP(updates.size)
      updates.forEach { (part, respData) =>
        val reqData = fetchDataAndError.fetchData.get(part)
        cachedPartitions.mustAdd(new CachedPartition(part, topicIds.getOrDefault(part.topic(), Uuid.ZERO_UUID), reqData, respData))
      }
      cachedPartitions
    }
    def generateUnresolvedPartitions: FetchSession.UNRESOLVED_CACHE = {
      val unresolvedPartitions = new FetchSession.UNRESOLVED_CACHE()
      fetchDataAndError.unresolvedPartitions.forEach { idAndData =>
        idAndData.partitionData.forEach { (part, reqData) =>
          unresolvedPartitions.add(new CachedUnresolvedPartition(idAndData.id, part, reqData))
        }
      }
      unresolvedPartitions
    }
    val responseSessionId = cache.maybeCreateSession(time.milliseconds(), isFromFollower,
        updates.size + fetchDataAndError.unresolvedPartitions().size(), () => generateResolvedPartitions, () => generateUnresolvedPartitions)
    debug(s"Full fetch context with session id $responseSessionId returning " +
      s"${partitionsToLogString(updates.keySet)}")
    FetchResponse.prepareResponse(Errors.NONE, updates, unresolvedTopicData, topicIds, 0, responseSessionId)
  }

  override def getUnresolvedTopicData(): util.List[FetchResponseData.FetchableTopicResponse] = {
    unresolvedTopicData
  }
}

/**
  * The fetch context for an incremental fetch request.
  *
  * @param time         The clock to use.
  * @param topicIds     The map from topic name to topic IDs
  * @param reqMetadata  The request metadata.
  * @param session      The incremental fetch request session.
  */
class IncrementalFetchContext(private val time: Time,
                              private val topicIds: util.Map[String, Uuid],
                              private val reqMetadata: JFetchMetadata,
                              private val session: FetchSession) extends FetchContext {
  private val unresolvedTopicData = generateUnresolvedTopicData(topicIds)
  override def getFetchOffset(tp: TopicPartition): Option[Long] = session.getFetchOffset(tp)

  override def foreachResolvedPartition(fun: (TopicPartition, FetchRequest.PartitionData) => Unit): Unit = {
    // Take the session lock and iterate over all the resolved cached partitions.
    session.synchronized {
      session.partitionMap.forEach { part =>
        fun(new TopicPartition(part.topic, part.partition), part.reqData)
      }
    }
  }

  private def generateUnresolvedTopicData(topicIds: util.Map[String, Uuid]): util.List[FetchResponseData.FetchableTopicResponse] = {
    val topicResponses = new util.ArrayList[FetchResponseData.FetchableTopicResponse]()
    val error = if (topicIds.isEmpty) Errors.UNSUPPORTED_VERSION else Errors.UNKNOWN_TOPIC_ID

    session.unresolvedPartitions.forEach(unresolvedPartition => {

      def buildPartitionData(): FetchResponseData.PartitionData = {
        new FetchResponseData.PartitionData()
          .setPartitionIndex(unresolvedPartition.partition)
          .setErrorCode(error.code)
          .setHighWatermark(FetchResponse.INVALID_HIGH_WATERMARK)
          .setLastStableOffset(FetchResponse.INVALID_LAST_STABLE_OFFSET)
          .setLogStartOffset(FetchResponse.INVALID_LOG_START_OFFSET)
          .setAbortedTransactions(null)
          .setPreferredReadReplica(FetchResponse.INVALID_PREFERRED_REPLICA_ID)
      }

      val prevTopic = if (topicResponses.isEmpty) null else topicResponses.get(topicResponses.size - 1)
      if (prevTopic != null && prevTopic.topicId == unresolvedPartition.topicId)
        prevTopic.partitions.add(buildPartitionData())
      else {
        val partitionResponses = new util.ArrayList[FetchResponseData.PartitionData]
        partitionResponses.add(buildPartitionData())
        topicResponses.add(new FetchResponseData.FetchableTopicResponse()
          .setTopicId(unresolvedPartition.topicId)
          .setPartitions(partitionResponses))
      }
    })
    topicResponses
  }

  // Iterator that goes over the given partition map and selects partitions that need to be included in the response.
  // If updateFetchContextAndRemoveUnselected is set to true, the fetch context will be updated for the selected
  // partitions and also remove unselected ones as they are encountered.
  private class PartitionIterator(val iter: FetchSession.RESP_MAP_ITER,
                                  val updateFetchContextAndRemoveUnselected: Boolean)
    extends FetchSession.RESP_MAP_ITER {
    var nextElement: util.Map.Entry[TopicPartition, FetchResponseData.PartitionData] = null

    override def hasNext: Boolean = {
      while ((nextElement == null) && iter.hasNext) {
        val element = iter.next()
        val topicPart = element.getKey
        val respData = element.getValue
        val cachedPart = session.partitionMap.find(new CachedPartition(topicPart))

        // If the current ID in a cached partition is Uuid.ZERO_UUID, and we have a valid
        // ID in topic IDs, simply add the ID. If there is not a valid ID, keep as Uuid.ZERO_UUID.
        if (cachedPart.topicId == Uuid.ZERO_UUID)
          cachedPart.addId(topicIds.getOrDefault(topicPart.topic, Uuid.ZERO_UUID))

        // If we have an situation where there is a valid ID on the partition, but it does not match
        // the ID in topic IDs (likely due to topic deletion and re-creation) or there is no valid topic
        // ID on the broker (topic deleted or broker received a metadataResponse without IDs),
        // remove the cached partition from partitionMap and from the response.
        if (cachedPart.topicId != topicIds.getOrDefault(topicPart.topic, Uuid.ZERO_UUID)) {
          nextElement = element
          session.partitionMap.remove(cachedPart)
          iter.remove()
        } else {
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
      }
      nextElement != null
    }

    override def next(): util.Map.Entry[TopicPartition, FetchResponseData.PartitionData] = {
      if (!hasNext) throw new NoSuchElementException
      val element = nextElement
      nextElement = null
      element
    }

    override def remove() = throw new UnsupportedOperationException
  }

  override def getResponseSize(updates: FetchSession.RESP_MAP, versionId: Short): Int = {
    session.synchronized {
      val expectedEpoch = JFetchMetadata.nextEpoch(reqMetadata.epoch)
      if (session.epoch != expectedEpoch) {
        FetchResponse.sizeOf(versionId, (new FetchSession.RESP_MAP).entrySet.iterator, Collections.emptyList(), Collections.emptyMap())
      } else {
        // Pass the partition iterator which updates neither the fetch context nor the partition map.
        FetchResponse.sizeOf(versionId, new PartitionIterator(updates.entrySet.iterator, false), unresolvedTopicData, topicIds)
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
        FetchResponse.prepareResponse(Errors.NONE, updates, unresolvedTopicData, topicIds, 0, session.id)
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
        FetchResponse.prepareResponse(Errors.NONE, new FetchSession.RESP_MAP, Collections.emptyList(), Collections.emptyMap(),
          throttleTimeMs, session.id)
      }
    }
  }

  override def getUnresolvedTopicData(): util.List[FetchResponseData.FetchableTopicResponse] = {
    unresolvedTopicData
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
  */
class FetchSessionCache(private val maxEntries: Int,
                        private val evictionMs: Long) extends Logging with KafkaMetricsGroup {
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

  // Set up metrics.
  removeMetric(FetchSession.NUM_INCREMENTAL_FETCH_SESSISONS)
  newGauge(FetchSession.NUM_INCREMENTAL_FETCH_SESSISONS, () => FetchSessionCache.this.size)
  removeMetric(FetchSession.NUM_INCREMENTAL_FETCH_PARTITIONS_CACHED)
  newGauge(FetchSession.NUM_INCREMENTAL_FETCH_PARTITIONS_CACHED, () => FetchSessionCache.this.totalPartitions)
  removeMetric(FetchSession.INCREMENTAL_FETCH_SESSIONS_EVICTIONS_PER_SEC)
  private[server] val evictionsMeter = newMeter(FetchSession.INCREMENTAL_FETCH_SESSIONS_EVICTIONS_PER_SEC,
    FetchSession.EVICTIONS, TimeUnit.SECONDS, Map.empty)

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
      id = ThreadLocalRandom.current().nextInt(1, Int.MaxValue)
    } while (sessions.contains(id) || id == INVALID_SESSION_ID)
    id
  }

  /**
    * Try to create a new session.
    *
    * @param now                The current time in milliseconds.
    * @param privileged         True if the new entry we are trying to create is privileged.
    * @param size               The number of cached partitions in the new entry we are trying to create.
    * @param createPartitions   A callback function which creates the map of cached partitions.
    * @return                   If we created a session, the ID; INVALID_SESSION_ID otherwise.
    */
  def maybeCreateSession(now: Long,
                         privileged: Boolean,
                         size: Int,
                         createPartitions: () => FetchSession.CACHE_MAP,
                         createUnresolvedPartitions: () => FetchSession.UNRESOLVED_CACHE): Int =
  synchronized {
    // If there is room, create a new session entry.
    if ((sessions.size < maxEntries) ||
        tryEvict(privileged, EvictableKey(privileged, size, 0), now)) {
      val partitionMap = createPartitions()
      val unresolvedPartitions = createUnresolvedPartitions()
      val session = new FetchSession(newSessionId(), privileged, partitionMap, unresolvedPartitions,
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
    * @param privileged True if the new entry we would like to add is privileged.
    * @param key        The EvictableKey for the new entry we would like to add.
    * @param now        The current time in milliseconds.
    * @return           True if an entry was evicted; false otherwise.
    */
  def tryEvict(privileged: Boolean, key: EvictableKey, now: Long): Boolean = synchronized {
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

class FetchManager(private val time: Time,
                   private val cache: FetchSessionCache) extends Logging {
  def newContext(reqVersion: Short,
                 reqMetadata: JFetchMetadata,
                 isFollower: Boolean,
                 fetchDataAndError: FetchDataAndError,
                 toForget: util.List[FetchRequestData.ForgottenTopic],
                 topicNames: util.Map[Uuid, String],
                 topicIds: util.Map[String, Uuid]): FetchContext = {
    val context = if (reqMetadata.isFull) {
      var removedFetchSessionStr = ""
      if (reqMetadata.sessionId != INVALID_SESSION_ID) {
        // Any session specified in a FULL fetch request will be closed.
        if (cache.remove(reqMetadata.sessionId).isDefined) {
          removedFetchSessionStr = s" Removed fetch session ${reqMetadata.sessionId}."
        }
      }
      var suffix = ""
      val context = if (reqMetadata.epoch == FINAL_EPOCH) {
        // If the epoch is FINAL_EPOCH, don't try to create a new session.
        suffix = " Will not try to create a new session."
        new SessionlessFetchContext(fetchDataAndError, topicIds)
      } else {
        new FullFetchContext(time, cache, reqMetadata, fetchDataAndError, topicIds, isFollower)
      }
      debug(s"Created a new full FetchContext with ${partitionsToLogString(fetchDataAndError.fetchData.keySet)}."+
        s"${removedFetchSessionStr}${suffix}")
      context
    } else {
      cache.synchronized {
        cache.get(reqMetadata.sessionId) match {
          case None => {
            debug(s"Session error for ${reqMetadata.sessionId}: no such session ID found.")
            new SessionErrorContext(Errors.FETCH_SESSION_ID_NOT_FOUND, reqMetadata)
          }
          case Some(session) => session.synchronized {
            if (session.epoch != reqMetadata.epoch) {
              debug(s"Session error for ${reqMetadata.sessionId}: expected epoch " +
                s"${session.epoch}, but got ${reqMetadata.epoch} instead.");
              new SessionErrorContext(Errors.INVALID_FETCH_SESSION_EPOCH, reqMetadata)
            } else {
              val (added, updated, removed) = session.update(reqVersion, fetchDataAndError, toForget, reqMetadata, topicIds, topicNames)
              if (session.isEmpty) {
                debug(s"Created a new sessionless FetchContext and closing session id ${session.id}, " +
                  s"epoch ${session.epoch}: after removing ${partitionsToLogString(removed)}, " +
                  s"there are no more partitions left.")
                cache.remove(session)
                new SessionlessFetchContext(fetchDataAndError, topicIds)
              } else {
                cache.touch(session, time.milliseconds())
                session.epoch = JFetchMetadata.nextEpoch(session.epoch)
                debug(s"Created a new incremental FetchContext for session id ${session.id}, " +
                  s"epoch ${session.epoch}: added ${partitionsToLogString(added)}, " +
                  s"updated ${partitionsToLogString(updated)}, " +
                  s"removed ${partitionsToLogString(removed)}")
                new IncrementalFetchContext(time, topicIds, reqMetadata, session)
              }
            }
          }
        }
      }
    }
    context
  }

  def partitionsToLogString(partitions: util.Collection[TopicPartition]): String =
    FetchSession.partitionsToLogString(partitions, isTraceEnabled)
}
