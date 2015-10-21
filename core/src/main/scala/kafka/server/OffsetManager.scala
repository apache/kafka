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

import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.protocol.types.{Struct, Schema, Field}
import org.apache.kafka.common.protocol.types.Type.STRING
import org.apache.kafka.common.protocol.types.Type.INT32
import org.apache.kafka.common.protocol.types.Type.INT64
import org.apache.kafka.common.utils.Utils

import kafka.utils._
import kafka.common._
import kafka.log.FileMessageSet
import kafka.message._
import kafka.metrics.KafkaMetricsGroup
import kafka.common.TopicAndPartition
import kafka.tools.MessageFormatter
import kafka.api.ProducerResponseStatus
import kafka.coordinator.GroupCoordinator

import scala.Some
import scala.collection._
import java.io.PrintStream
import java.util.concurrent.atomic.AtomicBoolean
import java.nio.ByteBuffer
import java.util.concurrent.TimeUnit

import com.yammer.metrics.core.Gauge
import org.I0Itec.zkclient.ZkClient

/**
 * Configuration settings for in-built offset management
 * @param maxMetadataSize The maximum allowed metadata for any offset commit.
 * @param loadBufferSize Batch size for reading from the offsets segments when loading offsets into the cache.
 * @param offsetsRetentionMs Offsets older than this retention period will be discarded.
 * @param offsetsRetentionCheckIntervalMs Frequency at which to check for expired offsets.
 * @param offsetsTopicNumPartitions The number of partitions for the offset commit topic (should not change after deployment).
 * @param offsetsTopicSegmentBytes The offsets topic segment bytes should be kept relatively small to facilitate faster
 *                                 log compaction and faster offset loads
 * @param offsetsTopicReplicationFactor The replication factor for the offset commit topic (set higher to ensure availability).
 * @param offsetsTopicCompressionCodec Compression codec for the offsets topic - compression should be turned on in
 *                                     order to achieve "atomic" commits.
 * @param offsetCommitTimeoutMs The offset commit will be delayed until all replicas for the offsets topic receive the
 *                              commit or this timeout is reached. (Similar to the producer request timeout.)
 * @param offsetCommitRequiredAcks The required acks before the commit can be accepted. In general, the default (-1)
 *                                 should not be overridden.
 */
case class OffsetManagerConfig(maxMetadataSize: Int = OffsetManagerConfig.DefaultMaxMetadataSize,
                               loadBufferSize: Int = OffsetManagerConfig.DefaultLoadBufferSize,
                               offsetsRetentionMs: Long = OffsetManagerConfig.DefaultOffsetRetentionMs,
                               offsetsRetentionCheckIntervalMs: Long = OffsetManagerConfig.DefaultOffsetsRetentionCheckIntervalMs,
                               offsetsTopicNumPartitions: Int = OffsetManagerConfig.DefaultOffsetsTopicNumPartitions,
                               offsetsTopicSegmentBytes: Int = OffsetManagerConfig.DefaultOffsetsTopicSegmentBytes,
                               offsetsTopicReplicationFactor: Short = OffsetManagerConfig.DefaultOffsetsTopicReplicationFactor,
                               offsetsTopicCompressionCodec: CompressionCodec = OffsetManagerConfig.DefaultOffsetsTopicCompressionCodec,
                               offsetCommitTimeoutMs: Int = OffsetManagerConfig.DefaultOffsetCommitTimeoutMs,
                               offsetCommitRequiredAcks: Short = OffsetManagerConfig.DefaultOffsetCommitRequiredAcks)

object OffsetManagerConfig {
  val DefaultMaxMetadataSize = 4096
  val DefaultLoadBufferSize = 5*1024*1024
  val DefaultOffsetRetentionMs = 24*60*60*1000L
  val DefaultOffsetsRetentionCheckIntervalMs = 600000L
  val DefaultOffsetsTopicNumPartitions = 50
  val DefaultOffsetsTopicSegmentBytes = 100*1024*1024
  val DefaultOffsetsTopicReplicationFactor = 3.toShort
  val DefaultOffsetsTopicCompressionCodec = NoCompressionCodec
  val DefaultOffsetCommitTimeoutMs = 5000
  val DefaultOffsetCommitRequiredAcks = (-1).toShort
}

class OffsetManager(val config: OffsetManagerConfig,
                    replicaManager: ReplicaManager,
                    zkUtils: ZkUtils,
                    scheduler: Scheduler) extends Logging with KafkaMetricsGroup {

  /* offsets and metadata cache */
  private val offsetsCache = new Pool[GroupTopicPartition, OffsetAndMetadata]
  private val followerTransitionLock = new Object
  private val loadingPartitions: mutable.Set[Int] = mutable.Set()
  private val cleanupOrLoadMutex = new Object
  private val shuttingDown = new AtomicBoolean(false)
  private val offsetsTopicPartitionCount = getOffsetsTopicPartitionCount

  this.logIdent = "[Offset Manager on Broker " + replicaManager.config.brokerId + "]: "

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
      def value = offsetsCache.keys.map(_.group).toSet.size
    }
  )

  private def deleteExpiredOffsets() {
    debug("Collecting expired offsets.")
    val startMs = SystemTime.milliseconds

    val numExpiredOffsetsRemoved = cleanupOrLoadMutex synchronized {
      val expiredOffsets = offsetsCache.filter { case (groupTopicPartition, offsetAndMetadata) =>
        offsetAndMetadata.expireTimestamp < startMs
      }

      debug("Found %d expired offsets.".format(expiredOffsets.size))

      // delete the expired offsets from the table and generate tombstone messages to remove them from the log
      val tombstonesForPartition = expiredOffsets.map { case (groupTopicAndPartition, offsetAndMetadata) =>
        val offsetsPartition = partitionFor(groupTopicAndPartition.group)
        trace("Removing expired offset and metadata for %s: %s".format(groupTopicAndPartition, offsetAndMetadata))

        offsetsCache.remove(groupTopicAndPartition)

        val commitKey = OffsetManager.offsetCommitKey(groupTopicAndPartition.group,
          groupTopicAndPartition.topicPartition.topic, groupTopicAndPartition.topicPartition.partition)

        (offsetsPartition, new Message(bytes = null, key = commitKey))
      }.groupBy { case (partition, tombstone) => partition }

      // Append the tombstone messages to the offset partitions. It is okay if the replicas don't receive these (say,
      // if we crash or leaders move) since the new leaders will get rid of expired offsets during their own purge cycles.
      tombstonesForPartition.flatMap { case (offsetsPartition, tombstones) =>
        val partitionOpt = replicaManager.getPartition(GroupCoordinator.OffsetsTopicName, offsetsPartition)
        partitionOpt.map { partition =>
          val appendPartition = TopicAndPartition(GroupCoordinator.OffsetsTopicName, offsetsPartition)
          val messages = tombstones.map(_._2).toSeq

          trace("Marked %d offsets in %s for deletion.".format(messages.size, appendPartition))

          try {
            // do not need to require acks since even if the tombsone is lost,
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

    info("Removed %d expired offsets in %d milliseconds.".format(numExpiredOffsetsRemoved, SystemTime.milliseconds - startMs))
  }


  def partitionFor(group: String): Int = Utils.abs(group.hashCode) % offsetsTopicPartitionCount

  /**
   * Fetch the current offset for the given group/topic/partition from the underlying offsets storage.
   *
   * @param key The requested group-topic-partition
   * @return If the key is present, return the offset and metadata; otherwise return None
   */
  private def getOffset(key: GroupTopicPartition) = {
    val offsetAndMetadata = offsetsCache.get(key)
    if (offsetAndMetadata == null)
      OffsetMetadataAndError.NoOffset
    else
      OffsetMetadataAndError(offsetAndMetadata.offset, offsetAndMetadata.metadata, ErrorMapping.NoError)
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

  /*
   * Check if the offset metadata length is valid
   */
  def validateOffsetMetadataLength(metadata: String) : Boolean = {
    metadata == null || metadata.length() <= config.maxMetadataSize
  }

  /**
   * Store offsets by appending it to the replicated log and then inserting to cache
   */
  def storeOffsets(groupId: String,
                   consumerId: String,
                   generationId: Int,
                   offsetMetadata: immutable.Map[TopicAndPartition, OffsetAndMetadata],
                   responseCallback: immutable.Map[TopicAndPartition, Short] => Unit) {
    // first filter out partitions with offset metadata size exceeding limit
    val filteredOffsetMetadata = offsetMetadata.filter { case (topicAndPartition, offsetAndMetadata) =>
      validateOffsetMetadataLength(offsetAndMetadata.metadata)
    }

    // construct the message set to append
    val messages = filteredOffsetMetadata.map { case (topicAndPartition, offsetAndMetadata) =>
      new Message(
        key = OffsetManager.offsetCommitKey(groupId, topicAndPartition.topic, topicAndPartition.partition),
        bytes = OffsetManager.offsetCommitValue(offsetAndMetadata)
      )
    }.toSeq

    val offsetTopicPartition = TopicAndPartition(GroupCoordinator.OffsetsTopicName, partitionFor(groupId))

    val offsetsAndMetadataMessageSet = Map(offsetTopicPartition ->
      new ByteBufferMessageSet(config.offsetsTopicCompressionCodec, messages:_*))

    // set the callback function to insert offsets into cache after log append completed
    def putCacheCallback(responseStatus: Map[TopicAndPartition, ProducerResponseStatus]) {
      // the append response should only contain the topics partition
      if (responseStatus.size != 1 || ! responseStatus.contains(offsetTopicPartition))
        throw new IllegalStateException("Append status %s should only have one partition %s"
          .format(responseStatus, offsetTopicPartition))

      // construct the commit response status and insert
      // the offset and metadata to cache if the append status has no error
      val status = responseStatus(offsetTopicPartition)

      val responseCode =
        if (status.error == ErrorMapping.NoError) {
          filteredOffsetMetadata.foreach { case (topicAndPartition, offsetAndMetadata) =>
            putOffset(GroupTopicPartition(groupId, topicAndPartition), offsetAndMetadata)
          }
          ErrorMapping.NoError
        } else {
          debug("Offset commit %s from group %s consumer %s with generation %d failed when appending to log due to %s"
            .format(filteredOffsetMetadata, groupId, consumerId, generationId, ErrorMapping.exceptionNameFor(status.error)))

          // transform the log append error code to the corresponding the commit status error code
          if (status.error == ErrorMapping.UnknownTopicOrPartitionCode)
            ErrorMapping.ConsumerCoordinatorNotAvailableCode
          else if (status.error == ErrorMapping.NotLeaderForPartitionCode)
            ErrorMapping.NotCoordinatorForConsumerCode
          else if (status.error == ErrorMapping.MessageSizeTooLargeCode
                || status.error == ErrorMapping.MessageSetSizeTooLargeCode
                || status.error == ErrorMapping.InvalidFetchSizeCode)
            Errors.INVALID_COMMIT_OFFSET_SIZE.code
          else
            status.error
        }


      // compute the final error codes for the commit response
      val commitStatus = offsetMetadata.map { case (topicAndPartition, offsetAndMetadata) =>
        if (validateOffsetMetadataLength(offsetAndMetadata.metadata))
          (topicAndPartition, responseCode)
        else
          (topicAndPartition, ErrorMapping.OffsetMetadataTooLargeCode)
      }

      // finally trigger the callback logic passed from the API layer
      responseCallback(commitStatus)
    }

    // call replica manager to append the offset messages
    replicaManager.appendMessages(
      config.offsetCommitTimeoutMs.toLong,
      config.offsetCommitRequiredAcks,
      true, // allow appending to internal offset topic
      offsetsAndMetadataMessageSet,
      putCacheCallback)
  }

  /**
   * The most important guarantee that this API provides is that it should never return a stale offset. i.e., it either
   * returns the current offset or it begins to sync the cache from the log (and returns an error code).
   */
  def getOffsets(group: String, topicPartitions: Seq[TopicAndPartition]): Map[TopicAndPartition, OffsetMetadataAndError] = {
    trace("Getting offsets %s for group %s.".format(topicPartitions, group))

    val offsetsPartition = partitionFor(group)

    /**
     * followerTransitionLock protects against fetching from an empty/cleared offset cache (i.e., cleared due to a
     * leader->follower transition). i.e., even if leader-is-local is true a follower transition can occur right after
     * the check and clear the cache. i.e., we would read from the empty cache and incorrectly return NoOffset.
     */
    followerTransitionLock synchronized {
      if (leaderIsLocal(offsetsPartition)) {
        if (loadingPartitions synchronized loadingPartitions.contains(offsetsPartition)) {
          debug("Cannot fetch offsets for group %s due to ongoing offset load.".format(group))
          topicPartitions.map { topicAndPartition =>
            val groupTopicPartition = GroupTopicPartition(group, topicAndPartition)
            (groupTopicPartition.topicPartition, OffsetMetadataAndError.OffsetsLoading)
          }.toMap
        } else {
          if (topicPartitions.size == 0) {
           // Return offsets for all partitions owned by this consumer group. (this only applies to consumers that commit offsets to Kafka.)
            offsetsCache.filter(_._1.group == group).map { case(groupTopicPartition, offsetAndMetadata) =>
              (groupTopicPartition.topicPartition, OffsetMetadataAndError(offsetAndMetadata.offset, offsetAndMetadata.metadata, ErrorMapping.NoError))
            }.toMap
          } else {
            topicPartitions.map { topicAndPartition =>
              val groupTopicPartition = GroupTopicPartition(group, topicAndPartition)
              (groupTopicPartition.topicPartition, getOffset(groupTopicPartition))
            }.toMap
          }
        }
      } else {
        debug("Could not fetch offsets for group %s (not offset coordinator).".format(group))
        topicPartitions.map { topicAndPartition =>
          val groupTopicPartition = GroupTopicPartition(group, topicAndPartition)
          (groupTopicPartition.topicPartition, OffsetMetadataAndError.NotCoordinatorForGroup)
        }.toMap
      }
    }
  }

  /**
   * Asynchronously read the partition from the offsets topic and populate the cache
   */
  def loadOffsetsFromLog(offsetsPartition: Int) {

    val topicPartition = TopicAndPartition(GroupCoordinator.OffsetsTopicName, offsetsPartition)

    loadingPartitions synchronized {
      if (loadingPartitions.contains(offsetsPartition)) {
        info("Offset load from %s already in progress.".format(topicPartition))
      } else {
        loadingPartitions.add(offsetsPartition)
        scheduler.schedule(topicPartition.toString, loadOffsets)
      }
    }

    def loadOffsets() {
      info("Loading offsets from " + topicPartition)

      val startMs = SystemTime.milliseconds
      try {
        replicaManager.logManager.getLog(topicPartition) match {
          case Some(log) =>
            var currOffset = log.logSegments.head.baseOffset
            val buffer = ByteBuffer.allocate(config.loadBufferSize)
            // loop breaks if leader changes at any time during the load, since getHighWatermark is -1
            cleanupOrLoadMutex synchronized {
              while (currOffset < getHighWatermark(offsetsPartition) && !shuttingDown.get()) {
                buffer.clear()
                val messages = log.read(currOffset, config.loadBufferSize).messageSet.asInstanceOf[FileMessageSet]
                messages.readInto(buffer, 0)
                val messageSet = new ByteBufferMessageSet(buffer)
                messageSet.foreach { msgAndOffset =>
                  require(msgAndOffset.message.key != null, "Offset entry key should not be null")
                  val key = OffsetManager.readMessageKey(msgAndOffset.message.key)
                  if (msgAndOffset.message.payload == null) {
                    if (offsetsCache.remove(key) != null)
                      trace("Removed offset for %s due to tombstone entry.".format(key))
                    else
                      trace("Ignoring redundant tombstone for %s.".format(key))
                  } else {
                    // special handling for version 0:
                    // set the expiration time stamp as commit time stamp + server default retention time
                    val value = OffsetManager.readMessageValue(msgAndOffset.message.payload)
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
                  currOffset = msgAndOffset.nextOffset
                }
              }
            }

            if (!shuttingDown.get())
              info("Finished loading offsets from %s in %d milliseconds."
                   .format(topicPartition, SystemTime.milliseconds - startMs))
          case None =>
            warn("No log found for " + topicPartition)
        }
      }
      catch {
        case t: Throwable =>
          error("Error in loading offsets from " + topicPartition, t)
      }
      finally {
        loadingPartitions synchronized loadingPartitions.remove(offsetsPartition)
      }
    }
  }

  private def getHighWatermark(partitionId: Int): Long = {
    val partitionOpt = replicaManager.getPartition(GroupCoordinator.OffsetsTopicName, partitionId)

    val hw = partitionOpt.map { partition =>
      partition.leaderReplicaIfLocal().map(_.highWatermark.messageOffset).getOrElse(-1L)
    }.getOrElse(-1L)

    hw
  }

  def leaderIsLocal(partition: Int) = { getHighWatermark(partition) != -1L }

  /**
   * When this broker becomes a follower for an offsets topic partition clear out the cache for groups that belong to
   * that partition.
   * @param offsetsPartition Groups belonging to this partition of the offsets topic will be deleted from the cache.
   */
  def removeOffsetsFromCacheForPartition(offsetsPartition: Int) {
    var numRemoved = 0
    followerTransitionLock synchronized {
      offsetsCache.keys.foreach { key =>
        if (partitionFor(key.group) == offsetsPartition) {
          offsetsCache.remove(key)
          numRemoved += 1
        }
      }
    }

    if (numRemoved > 0) info("Removed %d cached offsets for %s on follower transition."
                             .format(numRemoved, TopicAndPartition(GroupCoordinator.OffsetsTopicName, offsetsPartition)))
  }

  def shutdown() {
    shuttingDown.set(true)
  }

  /**
   * Gets the partition count of the offsets topic from ZooKeeper.
   * If the topic does not exist, the configured partition count is returned.
   */
  private def getOffsetsTopicPartitionCount = {
    val topic = GroupCoordinator.OffsetsTopicName
    val topicData = zkUtils.getPartitionAssignmentForTopics(Seq(topic))
    if (topicData(topic).nonEmpty)
      topicData(topic).size
    else
      config.offsetsTopicNumPartitions
  }
}

object OffsetManager {

  private case class KeyAndValueSchemas(keySchema: Schema, valueSchema: Schema)

  private val CURRENT_OFFSET_SCHEMA_VERSION = 1.toShort

  private val OFFSET_COMMIT_KEY_SCHEMA_V0 = new Schema(new Field("group", STRING),
                                                       new Field("topic", STRING),
                                                       new Field("partition", INT32))
  private val KEY_GROUP_FIELD = OFFSET_COMMIT_KEY_SCHEMA_V0.get("group")
  private val KEY_TOPIC_FIELD = OFFSET_COMMIT_KEY_SCHEMA_V0.get("topic")
  private val KEY_PARTITION_FIELD = OFFSET_COMMIT_KEY_SCHEMA_V0.get("partition")

  private val OFFSET_COMMIT_VALUE_SCHEMA_V0 = new Schema(new Field("offset", INT64),
                                                         new Field("metadata", STRING, "Associated metadata.", ""),
                                                         new Field("timestamp", INT64))

  private val OFFSET_COMMIT_VALUE_SCHEMA_V1 = new Schema(new Field("offset", INT64),
                                                         new Field("metadata", STRING, "Associated metadata.", ""),
                                                         new Field("commit_timestamp", INT64),
                                                         new Field("expire_timestamp", INT64))

  private val VALUE_OFFSET_FIELD_V0 = OFFSET_COMMIT_VALUE_SCHEMA_V0.get("offset")
  private val VALUE_METADATA_FIELD_V0 = OFFSET_COMMIT_VALUE_SCHEMA_V0.get("metadata")
  private val VALUE_TIMESTAMP_FIELD_V0 = OFFSET_COMMIT_VALUE_SCHEMA_V0.get("timestamp")

  private val VALUE_OFFSET_FIELD_V1 = OFFSET_COMMIT_VALUE_SCHEMA_V1.get("offset")
  private val VALUE_METADATA_FIELD_V1 = OFFSET_COMMIT_VALUE_SCHEMA_V1.get("metadata")
  private val VALUE_COMMIT_TIMESTAMP_FIELD_V1 = OFFSET_COMMIT_VALUE_SCHEMA_V1.get("commit_timestamp")
  private val VALUE_EXPIRE_TIMESTAMP_FIELD_V1 = OFFSET_COMMIT_VALUE_SCHEMA_V1.get("expire_timestamp")

  // map of versions to schemas
  private val OFFSET_SCHEMAS = Map(0 -> KeyAndValueSchemas(OFFSET_COMMIT_KEY_SCHEMA_V0, OFFSET_COMMIT_VALUE_SCHEMA_V0),
                                   1 -> KeyAndValueSchemas(OFFSET_COMMIT_KEY_SCHEMA_V0, OFFSET_COMMIT_VALUE_SCHEMA_V1))

  private val CURRENT_SCHEMA = schemaFor(CURRENT_OFFSET_SCHEMA_VERSION)

  private def schemaFor(version: Int) = {
    val schemaOpt = OFFSET_SCHEMAS.get(version)
    schemaOpt match {
      case Some(schema) => schema
      case _ => throw new KafkaException("Unknown offset schema version " + version)
    }
  }

  /**
   * Generates the key for offset commit message for given (group, topic, partition)
   *
   * @return key for offset commit message
   */
  private def offsetCommitKey(group: String, topic: String, partition: Int, versionId: Short = 0): Array[Byte] = {
    val key = new Struct(CURRENT_SCHEMA.keySchema)
    key.set(KEY_GROUP_FIELD, group)
    key.set(KEY_TOPIC_FIELD, topic)
    key.set(KEY_PARTITION_FIELD, partition)

    val byteBuffer = ByteBuffer.allocate(2 /* version */ + key.sizeOf)
    byteBuffer.putShort(CURRENT_OFFSET_SCHEMA_VERSION)
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
    val value = new Struct(CURRENT_SCHEMA.valueSchema)
    value.set(VALUE_OFFSET_FIELD_V1, offsetAndMetadata.offset)
    value.set(VALUE_METADATA_FIELD_V1, offsetAndMetadata.metadata)
    value.set(VALUE_COMMIT_TIMESTAMP_FIELD_V1, offsetAndMetadata.commitTimestamp)
    value.set(VALUE_EXPIRE_TIMESTAMP_FIELD_V1, offsetAndMetadata.expireTimestamp)
    val byteBuffer = ByteBuffer.allocate(2 /* version */ + value.sizeOf)
    byteBuffer.putShort(CURRENT_OFFSET_SCHEMA_VERSION)
    value.writeTo(byteBuffer)
    byteBuffer.array()
  }

  /**
   * Decodes the offset messages' key
   *
   * @param buffer input byte-buffer
   * @return an GroupTopicPartition object
   */
  private def readMessageKey(buffer: ByteBuffer): GroupTopicPartition = {
    val version = buffer.getShort()
    val keySchema = schemaFor(version).keySchema
    val key = keySchema.read(buffer).asInstanceOf[Struct]

    val group = key.get(KEY_GROUP_FIELD).asInstanceOf[String]
    val topic = key.get(KEY_TOPIC_FIELD).asInstanceOf[String]
    val partition = key.get(KEY_PARTITION_FIELD).asInstanceOf[Int]

    GroupTopicPartition(group, TopicAndPartition(topic, partition))
  }

  /**
   * Decodes the offset messages' payload and retrieves offset and metadata from it
   *
   * @param buffer input byte-buffer
   * @return an offset-metadata object from the message
   */
  private def readMessageValue(buffer: ByteBuffer): OffsetAndMetadata = {
    val structAndVersion = readMessageValueStruct(buffer)

    if (structAndVersion.value == null) { // tombstone
      null
    } else {
      if (structAndVersion.version == 0) {
        val offset = structAndVersion.value.get(VALUE_OFFSET_FIELD_V0).asInstanceOf[Long]
        val metadata = structAndVersion.value.get(VALUE_METADATA_FIELD_V0).asInstanceOf[String]
        val timestamp = structAndVersion.value.get(VALUE_TIMESTAMP_FIELD_V0).asInstanceOf[Long]

        OffsetAndMetadata(offset, metadata, timestamp)
      } else if (structAndVersion.version == 1) {
        val offset = structAndVersion.value.get(VALUE_OFFSET_FIELD_V1).asInstanceOf[Long]
        val metadata = structAndVersion.value.get(VALUE_METADATA_FIELD_V1).asInstanceOf[String]
        val commitTimestamp = structAndVersion.value.get(VALUE_COMMIT_TIMESTAMP_FIELD_V1).asInstanceOf[Long]
        val expireTimestamp = structAndVersion.value.get(VALUE_EXPIRE_TIMESTAMP_FIELD_V1).asInstanceOf[Long]

        OffsetAndMetadata(offset, metadata, commitTimestamp, expireTimestamp)
      } else {
        throw new IllegalStateException("Unknown offset message version")
      }
    }
  }

  private def readMessageValueStruct(buffer: ByteBuffer): MessageValueStructAndVersion = {
    if(buffer == null) { // tombstone
      MessageValueStructAndVersion(null, -1)
    } else {
      val version = buffer.getShort()
      val valueSchema = schemaFor(version).valueSchema
      val value = valueSchema.read(buffer).asInstanceOf[Struct]

      MessageValueStructAndVersion(value, version)
    }
  }

  // Formatter for use with tools such as console consumer: Consumer should also set exclude.internal.topics to false.
  // (specify --formatter "kafka.server.OffsetManager\$OffsetsMessageFormatter" when consuming __consumer_offsets)
  class OffsetsMessageFormatter extends MessageFormatter {
    def writeTo(key: Array[Byte], value: Array[Byte], output: PrintStream) {
      val formattedKey = if (key == null) "NULL" else OffsetManager.readMessageKey(ByteBuffer.wrap(key)).toString
      val formattedValue = if (value == null) "NULL" else OffsetManager.readMessageValueStruct(ByteBuffer.wrap(value)).value.toString
      output.write(formattedKey.getBytes)
      output.write("::".getBytes)
      output.write(formattedValue.getBytes)
      output.write("\n".getBytes)
    }
  }

}

case class MessageValueStructAndVersion(value: Struct, version: Short)

case class GroupTopicPartition(group: String, topicPartition: TopicAndPartition) {

  def this(group: String, topic: String, partition: Int) =
    this(group, new TopicAndPartition(topic, partition))

  override def toString =
    "[%s,%s,%d]".format(group, topicPartition.topic, topicPartition.partition)

}
