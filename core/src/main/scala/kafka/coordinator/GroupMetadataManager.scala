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

package kafka.coordinator

import java.util.concurrent.locks.ReentrantReadWriteLock
import kafka.utils.CoreUtils._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.protocol.types.{ArrayOf, Field, Schema, Struct}
import org.apache.kafka.common.protocol.types.Type.STRING
import org.apache.kafka.common.protocol.types.Type.INT32
import org.apache.kafka.common.protocol.types.Type.INT64
import org.apache.kafka.common.protocol.types.Type.BYTES
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.requests.OffsetFetchResponse
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.common.utils.Time
import org.apache.kafka.clients.consumer.ConsumerRecord
import kafka.utils._
import kafka.common._
import kafka.message._
import kafka.log.FileMessageSet
import kafka.metrics.KafkaMetricsGroup
import kafka.common.TopicAndPartition
import kafka.common.MessageFormatter
import kafka.server.ReplicaManager
import scala.collection._
import java.io.PrintStream
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.TimeUnit
import com.yammer.metrics.core.Gauge
import org.apache.kafka.common.internals.TopicConstants

case class DelayedStore(messageSet: Map[TopicPartition, MessageSet],
                        callback: Map[TopicPartition, PartitionResponse] => Unit)

class GroupMetadataManager(val brokerId: Int,
                           val config: OffsetConfig,
                           replicaManager: ReplicaManager,
                           zkUtils: ZkUtils,
                           time: Time) extends Logging with KafkaMetricsGroup {

  /* offsets cache */
  private val offsetsCache = new Pool[GroupTopicPartition, OffsetAndMetadata]

  /* group metadata cache */
  private val groupsCache = new Pool[String, GroupMetadata]

  /* partitions of consumer groups that are being loaded, its lock should be always called BEFORE offsetExpireLock and the group lock if needed */
  private val loadingPartitions: mutable.Set[Int] = mutable.Set()

  /* partitions of consumer groups that are assigned, using the same loading partition lock */
  private val ownedPartitions: mutable.Set[Int] = mutable.Set()

  /* lock for expiring stale offsets, it should be always called BEFORE the group lock if needed */
  private val offsetExpireLock = new ReentrantReadWriteLock()

  /* shutting down flag */
  private val shuttingDown = new AtomicBoolean(false)

  /* number of partitions for the consumer metadata topic */
  private val groupMetadataTopicPartitionCount = getOffsetsTopicPartitionCount

  /* Single-thread scheduler to handling offset/group metadata cache loading and unloading */
  private val scheduler = new KafkaScheduler(threads = 1, threadNamePrefix = "group-metadata-manager-")

  this.logIdent = "[Group Metadata Manager on Broker " + brokerId + "]: "

  scheduler.startup()
  scheduler.schedule(name = "delete-expired-consumer-offsets",
    fun = deleteExpiredOffsets,
    period = config.offsetsRetentionCheckIntervalMs,
    unit = TimeUnit.MILLISECONDS)

  newGauge("NumOffsets",
    new Gauge[Int] {
      def value = offsetsCache.size
    }
  )

  newGauge("NumGroups",
    new Gauge[Int] {
      def value = groupsCache.size
    }
  )

  def currentGroups(): Iterable[GroupMetadata] = groupsCache.values

  def partitionFor(groupId: String): Int = Utils.abs(groupId.hashCode) % groupMetadataTopicPartitionCount

  def isGroupLocal(groupId: String): Boolean = loadingPartitions synchronized ownedPartitions.contains(partitionFor(groupId))

  def isGroupLoading(groupId: String): Boolean = loadingPartitions synchronized loadingPartitions.contains(partitionFor(groupId))

  def isLoading(): Boolean = loadingPartitions synchronized !loadingPartitions.isEmpty

  /**
   * Get the group associated with the given groupId, or null if not found
   */
  def getGroup(groupId: String): GroupMetadata = {
      groupsCache.get(groupId)
  }

  /**
   * Add a group or get the group associated with the given groupId if it already exists
   */
  def addGroup(group: GroupMetadata): GroupMetadata = {
    val currentGroup = groupsCache.putIfNotExists(group.groupId, group)
    if (currentGroup != null) {
      currentGroup
    } else {
      group
    }
  }

  /**
   * Remove all metadata associated with the group
   * @param group
   */
  def removeGroup(group: GroupMetadata) {
    // guard this removal in case of concurrent access (e.g. if a delayed join completes with no members
    // while the group is being removed due to coordinator emigration)
    if (groupsCache.remove(group.groupId, group)) {
      // Append the tombstone messages to the partition. It is okay if the replicas don't receive these (say,
      // if we crash or leaders move) since the new leaders will still expire the consumers with heartbeat and
      // retry removing this group.
      val groupPartition = partitionFor(group.groupId)
      val (magicValue, timestamp) = getMessageFormatVersionAndTimestamp(groupPartition)
      val tombstone = new Message(bytes = null, key = GroupMetadataManager.groupMetadataKey(group.groupId),
        timestamp = timestamp, magicValue = magicValue)

      val partitionOpt = replicaManager.getPartition(TopicConstants.GROUP_METADATA_TOPIC_NAME, groupPartition)
      partitionOpt.foreach { partition =>
        val appendPartition = TopicAndPartition(TopicConstants.GROUP_METADATA_TOPIC_NAME, groupPartition)

        trace("Marking group %s as deleted.".format(group.groupId))

        try {
          // do not need to require acks since even if the tombstone is lost,
          // it will be appended again by the new leader
          // TODO KAFKA-2720: periodic purging instead of immediate removal of groups
          partition.appendMessagesToLeader(new ByteBufferMessageSet(config.offsetsTopicCompressionCodec, tombstone))
        } catch {
          case t: Throwable =>
            error("Failed to mark group %s as deleted in %s.".format(group.groupId, appendPartition), t)
          // ignore and continue
        }
      }
    }
  }

  def prepareStoreGroup(group: GroupMetadata,
                        groupAssignment: Map[String, Array[Byte]],
                        responseCallback: Short => Unit): DelayedStore = {
    val (magicValue, timestamp) = getMessageFormatVersionAndTimestamp(partitionFor(group.groupId))
    val message = new Message(
      key = GroupMetadataManager.groupMetadataKey(group.groupId),
      bytes = GroupMetadataManager.groupMetadataValue(group, groupAssignment),
      timestamp = timestamp,
      magicValue = magicValue)

    val groupMetadataPartition = new TopicPartition(TopicConstants.GROUP_METADATA_TOPIC_NAME, partitionFor(group.groupId))

    val groupMetadataMessageSet = Map(groupMetadataPartition ->
      new ByteBufferMessageSet(config.offsetsTopicCompressionCodec, message))

    val generationId = group.generationId

    // set the callback function to insert the created group into cache after log append completed
    def putCacheCallback(responseStatus: Map[TopicPartition, PartitionResponse]) {
      // the append response should only contain the topics partition
      if (responseStatus.size != 1 || ! responseStatus.contains(groupMetadataPartition))
        throw new IllegalStateException("Append status %s should only have one partition %s"
          .format(responseStatus, groupMetadataPartition))

      // construct the error status in the propagated assignment response
      // in the cache
      val status = responseStatus(groupMetadataPartition)

      var responseCode = Errors.NONE.code
      if (status.errorCode != Errors.NONE.code) {
        debug("Metadata from group %s with generation %d failed when appending to log due to %s"
          .format(group.groupId, generationId, Errors.forCode(status.errorCode).exceptionName))

        // transform the log append error code to the corresponding the commit status error code
        responseCode = if (status.errorCode == Errors.UNKNOWN_TOPIC_OR_PARTITION.code) {
          Errors.GROUP_COORDINATOR_NOT_AVAILABLE.code
        } else if (status.errorCode == Errors.NOT_LEADER_FOR_PARTITION.code) {
          Errors.NOT_COORDINATOR_FOR_GROUP.code
        } else if (status.errorCode == Errors.REQUEST_TIMED_OUT.code) {
          Errors.REBALANCE_IN_PROGRESS.code
        } else if (status.errorCode == Errors.MESSAGE_TOO_LARGE.code
          || status.errorCode == Errors.RECORD_LIST_TOO_LARGE.code
          || status.errorCode == Errors.INVALID_FETCH_SIZE.code) {

          error("Appending metadata message for group %s generation %d failed due to %s, returning UNKNOWN error code to the client"
            .format(group.groupId, generationId, Errors.forCode(status.errorCode).exceptionName))

          Errors.UNKNOWN.code
        } else {
          error("Appending metadata message for group %s generation %d failed due to unexpected error: %s"
            .format(group.groupId, generationId, status.errorCode))

          status.errorCode
        }
      }

      responseCallback(responseCode)
    }

    DelayedStore(groupMetadataMessageSet, putCacheCallback)
  }

  def store(delayedAppend: DelayedStore) {
    // call replica manager to append the group message
    replicaManager.appendMessages(
      config.offsetCommitTimeoutMs.toLong,
      config.offsetCommitRequiredAcks,
      true, // allow appending to internal offset topic
      delayedAppend.messageSet,
      delayedAppend.callback)
  }

  /**
   * Store offsets by appending it to the replicated log and then inserting to cache
   */
  def prepareStoreOffsets(groupId: String,
                          consumerId: String,
                          generationId: Int,
                          offsetMetadata: immutable.Map[TopicPartition, OffsetAndMetadata],
                          responseCallback: immutable.Map[TopicPartition, Short] => Unit): DelayedStore = {
    // first filter out partitions with offset metadata size exceeding limit
    val filteredOffsetMetadata = offsetMetadata.filter { case (topicPartition, offsetAndMetadata) =>
      validateOffsetMetadataLength(offsetAndMetadata.metadata)
    }

    // construct the message set to append
    val messages = filteredOffsetMetadata.map { case (topicAndPartition, offsetAndMetadata) =>
      val (magicValue, timestamp) = getMessageFormatVersionAndTimestamp(partitionFor(groupId))
      new Message(
        key = GroupMetadataManager.offsetCommitKey(groupId, topicAndPartition.topic, topicAndPartition.partition),
        bytes = GroupMetadataManager.offsetCommitValue(offsetAndMetadata),
        timestamp = timestamp,
        magicValue = magicValue
      )
    }.toSeq

    val offsetTopicPartition = new TopicPartition(TopicConstants.GROUP_METADATA_TOPIC_NAME, partitionFor(groupId))

    val offsetsAndMetadataMessageSet = Map(offsetTopicPartition ->
      new ByteBufferMessageSet(config.offsetsTopicCompressionCodec, messages:_*))

    // set the callback function to insert offsets into cache after log append completed
    def putCacheCallback(responseStatus: Map[TopicPartition, PartitionResponse]) {
      // the append response should only contain the topics partition
      if (responseStatus.size != 1 || ! responseStatus.contains(offsetTopicPartition))
        throw new IllegalStateException("Append status %s should only have one partition %s"
          .format(responseStatus, offsetTopicPartition))

      // construct the commit response status and insert
      // the offset and metadata to cache if the append status has no error
      val status = responseStatus(offsetTopicPartition)

      val responseCode =
        if (status.errorCode == Errors.NONE.code) {
          filteredOffsetMetadata.foreach { case (topicAndPartition, offsetAndMetadata) =>
            putOffset(GroupTopicPartition(groupId, topicAndPartition), offsetAndMetadata)
          }
          Errors.NONE.code
        } else {
          debug("Offset commit %s from group %s consumer %s with generation %d failed when appending to log due to %s"
            .format(filteredOffsetMetadata, groupId, consumerId, generationId, Errors.forCode(status.errorCode).exceptionName))

          // transform the log append error code to the corresponding the commit status error code
          if (status.errorCode == Errors.UNKNOWN_TOPIC_OR_PARTITION.code)
            Errors.GROUP_COORDINATOR_NOT_AVAILABLE.code
          else if (status.errorCode == Errors.NOT_LEADER_FOR_PARTITION.code)
            Errors.NOT_COORDINATOR_FOR_GROUP.code
          else if (status.errorCode == Errors.MESSAGE_TOO_LARGE.code
            || status.errorCode == Errors.RECORD_LIST_TOO_LARGE.code
            || status.errorCode == Errors.INVALID_FETCH_SIZE.code)
            Errors.INVALID_COMMIT_OFFSET_SIZE.code
          else
            status.errorCode
        }


      // compute the final error codes for the commit response
      val commitStatus = offsetMetadata.map { case (topicAndPartition, offsetAndMetadata) =>
        if (validateOffsetMetadataLength(offsetAndMetadata.metadata))
          (topicAndPartition, responseCode)
        else
          (topicAndPartition, Errors.OFFSET_METADATA_TOO_LARGE.code)
      }

      // finally trigger the callback logic passed from the API layer
      responseCallback(commitStatus)
    }

    DelayedStore(offsetsAndMetadataMessageSet, putCacheCallback)
  }

  /**
   * The most important guarantee that this API provides is that it should never return a stale offset. i.e., it either
   * returns the current offset or it begins to sync the cache from the log (and returns an error code).
   */
  def getOffsets(group: String, topicPartitions: Seq[TopicPartition]): Map[TopicPartition, OffsetFetchResponse.PartitionData] = {
    trace("Getting offsets %s for group %s.".format(topicPartitions, group))

    if (isGroupLocal(group)) {
      if (topicPartitions.isEmpty) {
        // Return offsets for all partitions owned by this consumer group. (this only applies to consumers that commit offsets to Kafka.)
        offsetsCache.filter(_._1.group == group).map { case(groupTopicPartition, offsetAndMetadata) =>
          (groupTopicPartition.topicPartition, new OffsetFetchResponse.PartitionData(offsetAndMetadata.offset, offsetAndMetadata.metadata, Errors.NONE.code))
        }.toMap
      } else {
        topicPartitions.map { topicPartition =>
          val groupTopicPartition = GroupTopicPartition(group, topicPartition)
          (groupTopicPartition.topicPartition, getOffset(groupTopicPartition))
        }.toMap
      }
    } else {
      debug("Could not fetch offsets for group %s (not offset coordinator).".format(group))
      topicPartitions.map { topicPartition =>
        val groupTopicPartition = GroupTopicPartition(group, topicPartition)
        (groupTopicPartition.topicPartition, new OffsetFetchResponse.PartitionData(OffsetFetchResponse.INVALID_OFFSET, "", Errors.NOT_COORDINATOR_FOR_GROUP.code))
      }.toMap
    }
  }

  /**
   * Asynchronously read the partition from the offsets topic and populate the cache
   */
  def loadGroupsForPartition(offsetsPartition: Int,
                             onGroupLoaded: GroupMetadata => Unit) {
    val topicPartition = TopicAndPartition(TopicConstants.GROUP_METADATA_TOPIC_NAME, offsetsPartition)
    scheduler.schedule(topicPartition.toString, loadGroupsAndOffsets)

    def loadGroupsAndOffsets() {
      info("Loading offsets and group metadata from " + topicPartition)

      loadingPartitions synchronized {
        if (loadingPartitions.contains(offsetsPartition)) {
          info("Offset load from %s already in progress.".format(topicPartition))
          return
        } else {
          loadingPartitions.add(offsetsPartition)
        }
      }

      val startMs = time.milliseconds()
      try {
        replicaManager.logManager.getLog(topicPartition) match {
          case Some(log) =>
            var currOffset = log.logSegments.head.baseOffset
            val buffer = ByteBuffer.allocate(config.loadBufferSize)
            // loop breaks if leader changes at any time during the load, since getHighWatermark is -1
            inWriteLock(offsetExpireLock) {
              val loadedGroups = mutable.Map[String, GroupMetadata]()
              val removedGroups = mutable.Set[String]()

              while (currOffset < getHighWatermark(offsetsPartition) && !shuttingDown.get()) {
                buffer.clear()
                val messages = log.read(currOffset, config.loadBufferSize).messageSet.asInstanceOf[FileMessageSet]
                messages.readInto(buffer, 0)
                val messageSet = new ByteBufferMessageSet(buffer)
                messageSet.foreach { msgAndOffset =>
                  require(msgAndOffset.message.key != null, "Offset entry key should not be null")
                  val baseKey = GroupMetadataManager.readMessageKey(msgAndOffset.message.key)

                  if (baseKey.isInstanceOf[OffsetKey]) {
                    // load offset
                    val key = baseKey.key.asInstanceOf[GroupTopicPartition]
                    if (msgAndOffset.message.payload == null) {
                      if (offsetsCache.remove(key) != null)
                        trace("Removed offset for %s due to tombstone entry.".format(key))
                      else
                        trace("Ignoring redundant tombstone for %s.".format(key))
                    } else {
                      // special handling for version 0:
                      // set the expiration time stamp as commit time stamp + server default retention time
                      val value = GroupMetadataManager.readOffsetMessageValue(msgAndOffset.message.payload)
                      putOffset(key, value.copy (
                        expireTimestamp = {
                          if (value.expireTimestamp == org.apache.kafka.common.requests.OffsetCommitRequest.DEFAULT_TIMESTAMP)
                            value.commitTimestamp + config.offsetsRetentionMs
                          else
                            value.expireTimestamp
                        }
                      ))
                      trace("Loaded offset %s for %s.".format(value, key))
                    }
                  } else {
                    // load group metadata
                    val groupId = baseKey.key.asInstanceOf[String]
                    val groupMetadata = GroupMetadataManager.readGroupMessageValue(groupId, msgAndOffset.message.payload)
                    if (groupMetadata != null) {
                      trace(s"Loaded group metadata for group ${groupMetadata.groupId} with generation ${groupMetadata.generationId}")
                      removedGroups.remove(groupId)
                      loadedGroups.put(groupId, groupMetadata)
                    } else {
                      loadedGroups.remove(groupId)
                      removedGroups.add(groupId)
                    }
                  }

                  currOffset = msgAndOffset.nextOffset
                }
              }

              loadedGroups.values.foreach { group =>
                val currentGroup = addGroup(group)
                if (group != currentGroup)
                  debug(s"Attempt to load group ${group.groupId} from log with generation ${group.generationId} failed " +
                    s"because there is already a cached group with generation ${currentGroup.generationId}")
                else
                  onGroupLoaded(group)
              }

              removedGroups.foreach { groupId =>
                val group = groupsCache.get(groupId)
                if (group != null)
                  throw new IllegalStateException(s"Unexpected unload of acitve group ${group.groupId} while " +
                    s"loading partition ${topicPartition}")
              }
            }

            if (!shuttingDown.get())
              info("Finished loading offsets from %s in %d milliseconds."
                .format(topicPartition, time.milliseconds() - startMs))
          case None =>
            warn("No log found for " + topicPartition)
        }
      }
      catch {
        case t: Throwable =>
          error("Error in loading offsets from " + topicPartition, t)
      }
      finally {
        loadingPartitions synchronized {
          ownedPartitions.add(offsetsPartition)
          loadingPartitions.remove(offsetsPartition)
        }
      }
    }
  }

  /**
   * When this broker becomes a follower for an offsets topic partition clear out the cache for groups that belong to
   * that partition.
   * @param offsetsPartition Groups belonging to this partition of the offsets topic will be deleted from the cache.
   */
  def removeGroupsForPartition(offsetsPartition: Int,
                               onGroupUnloaded: GroupMetadata => Unit) {
    val topicPartition = TopicAndPartition(TopicConstants.GROUP_METADATA_TOPIC_NAME, offsetsPartition)
    scheduler.schedule(topicPartition.toString, removeGroupsAndOffsets)

    def removeGroupsAndOffsets() {
      var numOffsetsRemoved = 0
      var numGroupsRemoved = 0

      loadingPartitions synchronized {
        // we need to guard the group removal in cache in the loading partition lock
        // to prevent coordinator's check-and-get-group race condition
        ownedPartitions.remove(offsetsPartition)

        // clear the offsets for this partition in the cache

        /**
         * NOTE: we need to put this in the loading partition lock as well to prevent race condition of the leader-is-local check
         * in getOffsets to protects against fetching from an empty/cleared offset cache (i.e., cleared due to a leader->follower
         * transition right after the check and clear the cache), causing offset fetch return empty offsets with NONE error code
         */
        offsetsCache.keys.foreach { key =>
          if (partitionFor(key.group) == offsetsPartition) {
            offsetsCache.remove(key)
            numOffsetsRemoved += 1
          }
        }

        // clear the groups for this partition in the cache
        for (group <- groupsCache.values) {
          if (partitionFor(group.groupId) == offsetsPartition) {
            onGroupUnloaded(group)
            groupsCache.remove(group.groupId, group)
            numGroupsRemoved += 1
          }
        }
      }

      if (numOffsetsRemoved > 0) info("Removed %d cached offsets for %s on follower transition."
        .format(numOffsetsRemoved, TopicAndPartition(TopicConstants.GROUP_METADATA_TOPIC_NAME, offsetsPartition)))

      if (numGroupsRemoved > 0) info("Removed %d cached groups for %s on follower transition."
        .format(numGroupsRemoved, TopicAndPartition(TopicConstants.GROUP_METADATA_TOPIC_NAME, offsetsPartition)))
    }
  }

  /**
   * Fetch the current offset for the given group/topic/partition from the underlying offsets storage.
   *
   * @param key The requested group-topic-partition
   * @return If the key is present, return the offset and metadata; otherwise return None
   */
  private def getOffset(key: GroupTopicPartition): OffsetFetchResponse.PartitionData = {
    val offsetAndMetadata = offsetsCache.get(key)
    if (offsetAndMetadata == null)
      new OffsetFetchResponse.PartitionData(OffsetFetchResponse.INVALID_OFFSET, "", Errors.NONE.code)
    else
      new OffsetFetchResponse.PartitionData(offsetAndMetadata.offset, offsetAndMetadata.metadata, Errors.NONE.code)
  }

  /**
   * Put the (already committed) offset for the given group/topic/partition into the cache.
   *
   * @param key The group-topic-partition
   * @param offsetAndMetadata The offset/metadata to be stored
   */
  private def putOffset(key: GroupTopicPartition, offsetAndMetadata: OffsetAndMetadata) {
    offsetsCache.put(key, offsetAndMetadata)
  }

  private def deleteExpiredOffsets() {
    debug("Collecting expired offsets.")
    val startMs = time.milliseconds()

    val numExpiredOffsetsRemoved = inWriteLock(offsetExpireLock) {
      val expiredOffsets = offsetsCache.filter { case (groupTopicPartition, offsetAndMetadata) =>
        offsetAndMetadata.expireTimestamp < startMs
      }

      debug("Found %d expired offsets.".format(expiredOffsets.size))

      // delete the expired offsets from the table and generate tombstone messages to remove them from the log
      val tombstonesForPartition = expiredOffsets.map { case (groupTopicAndPartition, offsetAndMetadata) =>
        val offsetsPartition = partitionFor(groupTopicAndPartition.group)
        trace("Removing expired offset and metadata for %s: %s".format(groupTopicAndPartition, offsetAndMetadata))

        offsetsCache.remove(groupTopicAndPartition)

        val commitKey = GroupMetadataManager.offsetCommitKey(groupTopicAndPartition.group,
          groupTopicAndPartition.topicPartition.topic, groupTopicAndPartition.topicPartition.partition)

        val (magicValue, timestamp) = getMessageFormatVersionAndTimestamp(offsetsPartition)
        (offsetsPartition, new Message(bytes = null, key = commitKey, timestamp = timestamp, magicValue = magicValue))
      }.groupBy { case (partition, tombstone) => partition }

      // Append the tombstone messages to the offset partitions. It is okay if the replicas don't receive these (say,
      // if we crash or leaders move) since the new leaders will get rid of expired offsets during their own purge cycles.
      tombstonesForPartition.flatMap { case (offsetsPartition, tombstones) =>
        val partitionOpt = replicaManager.getPartition(TopicConstants.GROUP_METADATA_TOPIC_NAME, offsetsPartition)
        partitionOpt.map { partition =>
          val appendPartition = TopicAndPartition(TopicConstants.GROUP_METADATA_TOPIC_NAME, offsetsPartition)
          val messages = tombstones.map(_._2).toSeq

          trace("Marked %d offsets in %s for deletion.".format(messages.size, appendPartition))

          try {
            // do not need to require acks since even if the tombstone is lost,
            // it will be appended again in the next purge cycle
            partition.appendMessagesToLeader(new ByteBufferMessageSet(config.offsetsTopicCompressionCodec, messages: _*))
            tombstones.size
          }
          catch {
            case t: Throwable =>
              error("Failed to mark %d expired offsets for deletion in %s.".format(messages.size, appendPartition), t)
              // ignore and continue
              0
          }
        }
      }.sum
    }

    info("Removed %d expired offsets in %d milliseconds.".format(numExpiredOffsetsRemoved, time.milliseconds() - startMs))
  }

  private def getHighWatermark(partitionId: Int): Long = {
    val partitionOpt = replicaManager.getPartition(TopicConstants.GROUP_METADATA_TOPIC_NAME, partitionId)

    val hw = partitionOpt.map { partition =>
      partition.leaderReplicaIfLocal().map(_.highWatermark.messageOffset).getOrElse(-1L)
    }.getOrElse(-1L)

    hw
  }

  /*
   * Check if the offset metadata length is valid
   */
  private def validateOffsetMetadataLength(metadata: String) : Boolean = {
    metadata == null || metadata.length() <= config.maxMetadataSize
  }

  def shutdown() {
    shuttingDown.set(true)
    scheduler.shutdown()

    // TODO: clear the caches
  }

  /**
   * Gets the partition count of the offsets topic from ZooKeeper.
   * If the topic does not exist, the configured partition count is returned.
   */
  private def getOffsetsTopicPartitionCount = {
    val topic = TopicConstants.GROUP_METADATA_TOPIC_NAME
    val topicData = zkUtils.getPartitionAssignmentForTopics(Seq(topic))
    if (topicData(topic).nonEmpty)
      topicData(topic).size
    else
      config.offsetsTopicNumPartitions
  }

  private def getMessageFormatVersionAndTimestamp(partition: Int): (Byte, Long) = {
    val groupMetadataTopicAndPartition = new TopicAndPartition(TopicConstants.GROUP_METADATA_TOPIC_NAME, partition)
    val messageFormatVersion = replicaManager.getMessageFormatVersion(groupMetadataTopicAndPartition).getOrElse {
      throw new IllegalArgumentException(s"Message format version for partition $groupMetadataTopicPartitionCount not found")
    }
    val timestamp = if (messageFormatVersion == Message.MagicValue_V0) Message.NoTimestamp else time.milliseconds()
    (messageFormatVersion, timestamp)
  }

  /**
   * Add the partition into the owned list
   *
   * NOTE: this is for test only
   */
  def addPartitionOwnership(partition: Int) {
    loadingPartitions synchronized {
      ownedPartitions.add(partition)
    }
  }
}

/**
 * Messages stored for the group topic has versions for both the key and value fields. Key
 * version is used to indicate the type of the message (also to differentiate different types
 * of messages from being compacted together if they have the same field values); and value
 * version is used to evolve the messages within their data types:
 *
 * key version 0:       group consumption offset
 *    -> value version 0:       [offset, metadata, timestamp]
 *
 * key version 1:       group consumption offset
 *    -> value version 1:       [offset, metadata, commit_timestamp, expire_timestamp]
 *
 * key version 2:       group metadata
 *     -> value version 0:       [protocol_type, generation, protocol, leader, members]
 */
object GroupMetadataManager {

  private val CURRENT_OFFSET_KEY_SCHEMA_VERSION = 1.toShort
  private val CURRENT_GROUP_KEY_SCHEMA_VERSION = 2.toShort

  private val OFFSET_COMMIT_KEY_SCHEMA = new Schema(new Field("group", STRING),
    new Field("topic", STRING),
    new Field("partition", INT32))
  private val OFFSET_KEY_GROUP_FIELD = OFFSET_COMMIT_KEY_SCHEMA.get("group")
  private val OFFSET_KEY_TOPIC_FIELD = OFFSET_COMMIT_KEY_SCHEMA.get("topic")
  private val OFFSET_KEY_PARTITION_FIELD = OFFSET_COMMIT_KEY_SCHEMA.get("partition")

  private val OFFSET_COMMIT_VALUE_SCHEMA_V0 = new Schema(new Field("offset", INT64),
    new Field("metadata", STRING, "Associated metadata.", ""),
    new Field("timestamp", INT64))
  private val OFFSET_VALUE_OFFSET_FIELD_V0 = OFFSET_COMMIT_VALUE_SCHEMA_V0.get("offset")
  private val OFFSET_VALUE_METADATA_FIELD_V0 = OFFSET_COMMIT_VALUE_SCHEMA_V0.get("metadata")
  private val OFFSET_VALUE_TIMESTAMP_FIELD_V0 = OFFSET_COMMIT_VALUE_SCHEMA_V0.get("timestamp")

  private val OFFSET_COMMIT_VALUE_SCHEMA_V1 = new Schema(new Field("offset", INT64),
    new Field("metadata", STRING, "Associated metadata.", ""),
    new Field("commit_timestamp", INT64),
    new Field("expire_timestamp", INT64))
  private val OFFSET_VALUE_OFFSET_FIELD_V1 = OFFSET_COMMIT_VALUE_SCHEMA_V1.get("offset")
  private val OFFSET_VALUE_METADATA_FIELD_V1 = OFFSET_COMMIT_VALUE_SCHEMA_V1.get("metadata")
  private val OFFSET_VALUE_COMMIT_TIMESTAMP_FIELD_V1 = OFFSET_COMMIT_VALUE_SCHEMA_V1.get("commit_timestamp")
  private val OFFSET_VALUE_EXPIRE_TIMESTAMP_FIELD_V1 = OFFSET_COMMIT_VALUE_SCHEMA_V1.get("expire_timestamp")

  private val GROUP_METADATA_KEY_SCHEMA = new Schema(new Field("group", STRING))
  private val GROUP_KEY_GROUP_FIELD = GROUP_METADATA_KEY_SCHEMA.get("group")

  private val MEMBER_METADATA_V0 = new Schema(new Field("member_id", STRING),
    new Field("client_id", STRING),
    new Field("client_host", STRING),
    new Field("session_timeout", INT32),
    new Field("subscription", BYTES),
    new Field("assignment", BYTES))
  private val MEMBER_METADATA_MEMBER_ID_V0 = MEMBER_METADATA_V0.get("member_id")
  private val MEMBER_METADATA_CLIENT_ID_V0 = MEMBER_METADATA_V0.get("client_id")
  private val MEMBER_METADATA_CLIENT_HOST_V0 = MEMBER_METADATA_V0.get("client_host")
  private val MEMBER_METADATA_SESSION_TIMEOUT_V0 = MEMBER_METADATA_V0.get("session_timeout")
  private val MEMBER_METADATA_SUBSCRIPTION_V0 = MEMBER_METADATA_V0.get("subscription")
  private val MEMBER_METADATA_ASSIGNMENT_V0 = MEMBER_METADATA_V0.get("assignment")


  private val GROUP_METADATA_VALUE_SCHEMA_V0 = new Schema(new Field("protocol_type", STRING),
    new Field("generation", INT32),
    new Field("protocol", STRING),
    new Field("leader", STRING),
    new Field("members", new ArrayOf(MEMBER_METADATA_V0)))
  private val GROUP_METADATA_PROTOCOL_TYPE_V0 = GROUP_METADATA_VALUE_SCHEMA_V0.get("protocol_type")
  private val GROUP_METADATA_GENERATION_V0 = GROUP_METADATA_VALUE_SCHEMA_V0.get("generation")
  private val GROUP_METADATA_PROTOCOL_V0 = GROUP_METADATA_VALUE_SCHEMA_V0.get("protocol")
  private val GROUP_METADATA_LEADER_V0 = GROUP_METADATA_VALUE_SCHEMA_V0.get("leader")
  private val GROUP_METADATA_MEMBERS_V0 = GROUP_METADATA_VALUE_SCHEMA_V0.get("members")

  // map of versions to key schemas as data types
  private val MESSAGE_TYPE_SCHEMAS = Map(
    0 -> OFFSET_COMMIT_KEY_SCHEMA,
    1 -> OFFSET_COMMIT_KEY_SCHEMA,
    2 -> GROUP_METADATA_KEY_SCHEMA)

  // map of version of offset value schemas
  private val OFFSET_VALUE_SCHEMAS = Map(
    0 -> OFFSET_COMMIT_VALUE_SCHEMA_V0,
    1 -> OFFSET_COMMIT_VALUE_SCHEMA_V1)
  private val CURRENT_OFFSET_VALUE_SCHEMA_VERSION = 1.toShort

  // map of version of group metadata value schemas
  private val GROUP_VALUE_SCHEMAS = Map(0 -> GROUP_METADATA_VALUE_SCHEMA_V0)
  private val CURRENT_GROUP_VALUE_SCHEMA_VERSION = 0.toShort

  private val CURRENT_OFFSET_KEY_SCHEMA = schemaForKey(CURRENT_OFFSET_KEY_SCHEMA_VERSION)
  private val CURRENT_GROUP_KEY_SCHEMA = schemaForKey(CURRENT_GROUP_KEY_SCHEMA_VERSION)

  private val CURRENT_OFFSET_VALUE_SCHEMA = schemaForOffset(CURRENT_OFFSET_VALUE_SCHEMA_VERSION)
  private val CURRENT_GROUP_VALUE_SCHEMA = schemaForGroup(CURRENT_GROUP_VALUE_SCHEMA_VERSION)

  private def schemaForKey(version: Int) = {
    val schemaOpt = MESSAGE_TYPE_SCHEMAS.get(version)
    schemaOpt match {
      case Some(schema) => schema
      case _ => throw new KafkaException("Unknown offset schema version " + version)
    }
  }

  private def schemaForOffset(version: Int) = {
    val schemaOpt = OFFSET_VALUE_SCHEMAS.get(version)
    schemaOpt match {
      case Some(schema) => schema
      case _ => throw new KafkaException("Unknown offset schema version " + version)
    }
  }

  private def schemaForGroup(version: Int) = {
    val schemaOpt = GROUP_VALUE_SCHEMAS.get(version)
    schemaOpt match {
      case Some(schema) => schema
      case _ => throw new KafkaException("Unknown group metadata version " + version)
    }
  }

  /**
   * Generates the key for offset commit message for given (group, topic, partition)
   *
   * @return key for offset commit message
   */
  private def offsetCommitKey(group: String, topic: String, partition: Int, versionId: Short = 0): Array[Byte] = {
    val key = new Struct(CURRENT_OFFSET_KEY_SCHEMA)
    key.set(OFFSET_KEY_GROUP_FIELD, group)
    key.set(OFFSET_KEY_TOPIC_FIELD, topic)
    key.set(OFFSET_KEY_PARTITION_FIELD, partition)

    val byteBuffer = ByteBuffer.allocate(2 /* version */ + key.sizeOf)
    byteBuffer.putShort(CURRENT_OFFSET_KEY_SCHEMA_VERSION)
    key.writeTo(byteBuffer)
    byteBuffer.array()
  }

  /**
   * Generates the key for group metadata message for given group
   *
   * @return key bytes for group metadata message
   */
  private def groupMetadataKey(group: String): Array[Byte] = {
    val key = new Struct(CURRENT_GROUP_KEY_SCHEMA)
    key.set(GROUP_KEY_GROUP_FIELD, group)

    val byteBuffer = ByteBuffer.allocate(2 /* version */ + key.sizeOf)
    byteBuffer.putShort(CURRENT_GROUP_KEY_SCHEMA_VERSION)
    key.writeTo(byteBuffer)
    byteBuffer.array()
  }

  /**
   * Generates the payload for offset commit message from given offset and metadata
   *
   * @param offsetAndMetadata consumer's current offset and metadata
   * @return payload for offset commit message
   */
  private def offsetCommitValue(offsetAndMetadata: OffsetAndMetadata): Array[Byte] = {
    // generate commit value with schema version 1
    val value = new Struct(CURRENT_OFFSET_VALUE_SCHEMA)
    value.set(OFFSET_VALUE_OFFSET_FIELD_V1, offsetAndMetadata.offset)
    value.set(OFFSET_VALUE_METADATA_FIELD_V1, offsetAndMetadata.metadata)
    value.set(OFFSET_VALUE_COMMIT_TIMESTAMP_FIELD_V1, offsetAndMetadata.commitTimestamp)
    value.set(OFFSET_VALUE_EXPIRE_TIMESTAMP_FIELD_V1, offsetAndMetadata.expireTimestamp)
    val byteBuffer = ByteBuffer.allocate(2 /* version */ + value.sizeOf)
    byteBuffer.putShort(CURRENT_OFFSET_VALUE_SCHEMA_VERSION)
    value.writeTo(byteBuffer)
    byteBuffer.array()
  }

  /**
   * Generates the payload for group metadata message from given offset and metadata
   * assuming the generation id, selected protocol, leader and member assignment are all available
   *
   * @param groupMetadata
   * @return payload for offset commit message
   */
  private def groupMetadataValue(groupMetadata: GroupMetadata, assignment: Map[String, Array[Byte]]): Array[Byte] = {
    // generate commit value with schema version 1
    val value = new Struct(CURRENT_GROUP_VALUE_SCHEMA)
    value.set(GROUP_METADATA_PROTOCOL_TYPE_V0, groupMetadata.protocolType)
    value.set(GROUP_METADATA_GENERATION_V0, groupMetadata.generationId)
    value.set(GROUP_METADATA_PROTOCOL_V0, groupMetadata.protocol)
    value.set(GROUP_METADATA_LEADER_V0, groupMetadata.leaderId)

    val memberArray = groupMetadata.allMemberMetadata.map {
      case memberMetadata =>
        val memberStruct = value.instance(GROUP_METADATA_MEMBERS_V0)
        memberStruct.set(MEMBER_METADATA_MEMBER_ID_V0, memberMetadata.memberId)
        memberStruct.set(MEMBER_METADATA_CLIENT_ID_V0, memberMetadata.clientId)
        memberStruct.set(MEMBER_METADATA_CLIENT_HOST_V0, memberMetadata.clientHost)
        memberStruct.set(MEMBER_METADATA_SESSION_TIMEOUT_V0, memberMetadata.sessionTimeoutMs)

        val metadata = memberMetadata.metadata(groupMetadata.protocol)
        memberStruct.set(MEMBER_METADATA_SUBSCRIPTION_V0, ByteBuffer.wrap(metadata))

        val memberAssignment = assignment(memberMetadata.memberId)
        assert(memberAssignment != null)

        memberStruct.set(MEMBER_METADATA_ASSIGNMENT_V0, ByteBuffer.wrap(memberAssignment))

        memberStruct
    }

    value.set(GROUP_METADATA_MEMBERS_V0, memberArray.toArray)

    val byteBuffer = ByteBuffer.allocate(2 /* version */ + value.sizeOf)
    byteBuffer.putShort(CURRENT_GROUP_VALUE_SCHEMA_VERSION)
    value.writeTo(byteBuffer)
    byteBuffer.array()
  }

  /**
   * Decodes the offset messages' key
   *
   * @param buffer input byte-buffer
   * @return an GroupTopicPartition object
   */
  def readMessageKey(buffer: ByteBuffer): BaseKey = {
    val version = buffer.getShort
    val keySchema = schemaForKey(version)
    val key = keySchema.read(buffer)

    if (version <= CURRENT_OFFSET_KEY_SCHEMA_VERSION) {
      // version 0 and 1 refer to offset
      val group = key.get(OFFSET_KEY_GROUP_FIELD).asInstanceOf[String]
      val topic = key.get(OFFSET_KEY_TOPIC_FIELD).asInstanceOf[String]
      val partition = key.get(OFFSET_KEY_PARTITION_FIELD).asInstanceOf[Int]

      OffsetKey(version, GroupTopicPartition(group, new TopicPartition(topic, partition)))

    } else if (version == CURRENT_GROUP_KEY_SCHEMA_VERSION) {
      // version 2 refers to offset
      val group = key.get(GROUP_KEY_GROUP_FIELD).asInstanceOf[String]

      GroupMetadataKey(version, group)
    } else {
      throw new IllegalStateException("Unknown version " + version + " for group metadata message")
    }
  }

  /**
   * Decodes the offset messages' payload and retrieves offset and metadata from it
   *
   * @param buffer input byte-buffer
   * @return an offset-metadata object from the message
   */
  def readOffsetMessageValue(buffer: ByteBuffer): OffsetAndMetadata = {
    if(buffer == null) { // tombstone
      null
    } else {
      val version = buffer.getShort
      val valueSchema = schemaForOffset(version)
      val value = valueSchema.read(buffer)

      if (version == 0) {
        val offset = value.get(OFFSET_VALUE_OFFSET_FIELD_V0).asInstanceOf[Long]
        val metadata = value.get(OFFSET_VALUE_METADATA_FIELD_V0).asInstanceOf[String]
        val timestamp = value.get(OFFSET_VALUE_TIMESTAMP_FIELD_V0).asInstanceOf[Long]

        OffsetAndMetadata(offset, metadata, timestamp)
      } else if (version == 1) {
        val offset = value.get(OFFSET_VALUE_OFFSET_FIELD_V1).asInstanceOf[Long]
        val metadata = value.get(OFFSET_VALUE_METADATA_FIELD_V1).asInstanceOf[String]
        val commitTimestamp = value.get(OFFSET_VALUE_COMMIT_TIMESTAMP_FIELD_V1).asInstanceOf[Long]
        val expireTimestamp = value.get(OFFSET_VALUE_EXPIRE_TIMESTAMP_FIELD_V1).asInstanceOf[Long]

        OffsetAndMetadata(offset, metadata, commitTimestamp, expireTimestamp)
      } else {
        throw new IllegalStateException("Unknown offset message version")
      }
    }
  }

  /**
   * Decodes the group metadata messages' payload and retrieves its member metadatafrom it
   *
   * @param buffer input byte-buffer
   * @return a group metadata object from the message
   */
  def readGroupMessageValue(groupId: String, buffer: ByteBuffer): GroupMetadata = {
    if(buffer == null) { // tombstone
      null
    } else {
      val version = buffer.getShort
      val valueSchema = schemaForGroup(version)
      val value = valueSchema.read(buffer)

      if (version == 0) {
        val protocolType = value.get(GROUP_METADATA_PROTOCOL_TYPE_V0).asInstanceOf[String]

        val group = new GroupMetadata(groupId, protocolType)

        group.generationId = value.get(GROUP_METADATA_GENERATION_V0).asInstanceOf[Int]
        group.leaderId = value.get(GROUP_METADATA_LEADER_V0).asInstanceOf[String]
        group.protocol = value.get(GROUP_METADATA_PROTOCOL_V0).asInstanceOf[String]

        value.getArray(GROUP_METADATA_MEMBERS_V0).foreach {
          case memberMetadataObj =>
            val memberMetadata = memberMetadataObj.asInstanceOf[Struct]
            val memberId = memberMetadata.get(MEMBER_METADATA_MEMBER_ID_V0).asInstanceOf[String]
            val clientId = memberMetadata.get(MEMBER_METADATA_CLIENT_ID_V0).asInstanceOf[String]
            val clientHost = memberMetadata.get(MEMBER_METADATA_CLIENT_HOST_V0).asInstanceOf[String]
            val sessionTimeout = memberMetadata.get(MEMBER_METADATA_SESSION_TIMEOUT_V0).asInstanceOf[Int]
            val subscription = Utils.toArray(memberMetadata.get(MEMBER_METADATA_SUBSCRIPTION_V0).asInstanceOf[ByteBuffer])

            val member = new MemberMetadata(memberId, groupId, clientId, clientHost, sessionTimeout,
              List((group.protocol, subscription)))

            member.assignment = Utils.toArray(memberMetadata.get(MEMBER_METADATA_ASSIGNMENT_V0).asInstanceOf[ByteBuffer])

            group.add(memberId, member)
        }

        group
      } else {
        throw new IllegalStateException("Unknown group metadata message version")
      }
    }
  }

  // Formatter for use with tools such as console consumer: Consumer should also set exclude.internal.topics to false.
  // (specify --formatter "kafka.coordinator.GroupMetadataManager\$OffsetsMessageFormatter" when consuming __consumer_offsets)
  class OffsetsMessageFormatter extends MessageFormatter {
    def writeTo(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]], output: PrintStream) {
      Option(consumerRecord.key).map(key => GroupMetadataManager.readMessageKey(ByteBuffer.wrap(key))).foreach {
        // Only print if the message is an offset record.
        // We ignore the timestamp of the message because GroupMetadataMessage has its own timestamp.
        case offsetKey: OffsetKey =>
          val groupTopicPartition = offsetKey.key
          val value = consumerRecord.value
          val formattedValue =
            if (value == null) "NULL"
            else GroupMetadataManager.readOffsetMessageValue(ByteBuffer.wrap(value)).toString
          output.write(groupTopicPartition.toString.getBytes)
          output.write("::".getBytes)
          output.write(formattedValue.getBytes)
          output.write("\n".getBytes)
        case _ => // no-op
      }
    }
  }

  // Formatter for use with tools to read group metadata history
  class GroupMetadataMessageFormatter extends MessageFormatter {
    def writeTo(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]], output: PrintStream) {
      Option(consumerRecord.key).map(key => GroupMetadataManager.readMessageKey(ByteBuffer.wrap(key))).foreach {
        // Only print if the message is a group metadata record.
        // We ignore the timestamp of the message because GroupMetadataMessage has its own timestamp.
        case groupMetadataKey: GroupMetadataKey =>
          val groupId = groupMetadataKey.key
          val value = consumerRecord.value
          val formattedValue =
            if (value == null) "NULL"
            else GroupMetadataManager.readGroupMessageValue(groupId, ByteBuffer.wrap(value)).toString
          output.write(groupId.getBytes)
          output.write("::".getBytes)
          output.write(formattedValue.getBytes)
          output.write("\n".getBytes)
        case _ => // no-op
      }
    }
  }

}

case class GroupTopicPartition(group: String, topicPartition: TopicPartition) {

  def this(group: String, topic: String, partition: Int) =
    this(group, new TopicPartition(topic, partition))

  override def toString =
    "[%s,%s,%d]".format(group, topicPartition.topic, topicPartition.partition)
}

trait BaseKey{
  def version: Short
  def key: Object
}

case class OffsetKey(version: Short, key: GroupTopicPartition) extends BaseKey {

  override def toString = key.toString
}

case class GroupMetadataKey(version: Short, key: String) extends BaseKey {

  override def toString = key
}

