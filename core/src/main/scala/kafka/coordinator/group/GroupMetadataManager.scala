/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.coordinator.group

import java.io.PrintStream
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.Optional
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock

import com.yammer.metrics.core.Gauge
import kafka.api.{ApiVersion, KAFKA_0_10_1_IV0, KAFKA_2_1_IV0, KAFKA_2_1_IV1}
import kafka.common.{MessageFormatter, OffsetAndMetadata}
import kafka.metrics.KafkaMetricsGroup
import kafka.server.ReplicaManager
import kafka.utils.CoreUtils.inLock
import kafka.utils._
import kafka.zk.KafkaZkClient
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.{KafkaException, TopicPartition}
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.protocol.types.Type._
import org.apache.kafka.common.protocol.types._
import org.apache.kafka.common.record._
import org.apache.kafka.common.requests.{OffsetCommitRequest, OffsetFetchResponse}
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.utils.{Time, Utils}

import scala.collection.JavaConverters._
import scala.collection._
import scala.collection.mutable.ListBuffer

class GroupMetadataManager(brokerId: Int,
                           interBrokerProtocolVersion: ApiVersion,
                           config: OffsetConfig,
                           replicaManager: ReplicaManager,
                           zkClient: KafkaZkClient,
                           time: Time) extends Logging with KafkaMetricsGroup {

  private val compressionType: CompressionType = CompressionType.forId(config.offsetsTopicCompressionCodec.codec)

  private val groupMetadataCache = new Pool[String, GroupMetadata]

  /* lock protecting access to loading and owned partition sets */
  private val partitionLock = new ReentrantLock()

  /* partitions of consumer groups that are being loaded, its lock should be always called BEFORE the group lock if needed */
  private val loadingPartitions: mutable.Set[Int] = mutable.Set()

  /* partitions of consumer groups that are assigned, using the same loading partition lock */
  private val ownedPartitions: mutable.Set[Int] = mutable.Set()

  /* shutting down flag */
  private val shuttingDown = new AtomicBoolean(false)

  /* number of partitions for the consumer metadata topic */
  private val groupMetadataTopicPartitionCount = getGroupMetadataTopicPartitionCount

  /* single-thread scheduler to handle offset/group metadata cache loading and unloading */
  private val scheduler = new KafkaScheduler(threads = 1, threadNamePrefix = "group-metadata-manager-")

  /* The groups with open transactional offsets commits per producer. We need this because when the commit or abort
   * marker comes in for a transaction, it is for a particular partition on the offsets topic and a particular producerId.
   * We use this structure to quickly find the groups which need to be updated by the commit/abort marker. */
  private val openGroupsForProducer = mutable.HashMap[Long, mutable.Set[String]]()

  this.logIdent = s"[GroupMetadataManager brokerId=$brokerId] "

  private def recreateGauge[T](name: String, gauge: Gauge[T]): Gauge[T] = {
    removeMetric(name)
    newGauge(name, gauge)
  }

  recreateGauge("NumOffsets",
    new Gauge[Int] {
      def value = groupMetadataCache.values.map(group => {
        group.inLock { group.numOffsets }
      }).sum
    })

  recreateGauge("NumGroups",
    new Gauge[Int] {
      def value = groupMetadataCache.size
    })

  recreateGauge("NumGroupsPreparingRebalance",
    new Gauge[Int] {
      def value(): Int = groupMetadataCache.values.count(group => {
        group synchronized { group.is(PreparingRebalance) }
      })
    })

  recreateGauge("NumGroupsCompletingRebalance",
    new Gauge[Int] {
      def value(): Int = groupMetadataCache.values.count(group => {
        group synchronized { group.is(CompletingRebalance) }
      })
    })

  recreateGauge("NumGroupsStable",
    new Gauge[Int] {
      def value(): Int = groupMetadataCache.values.count(group => {
        group synchronized { group.is(Stable) }
      })
    })

  recreateGauge("NumGroupsDead",
    new Gauge[Int] {
      def value(): Int = groupMetadataCache.values.count(group => {
        group synchronized { group.is(Dead) }
      })
    })

  recreateGauge("NumGroupsEmpty",
    new Gauge[Int] {
      def value(): Int = groupMetadataCache.values.count(group => {
        group synchronized { group.is(Empty) }
      })
    })

  def startup(enableMetadataExpiration: Boolean) {
    scheduler.startup()
    if (enableMetadataExpiration) {
      scheduler.schedule(name = "delete-expired-group-metadata",
        fun = () => cleanupGroupMetadata,
        period = config.offsetsRetentionCheckIntervalMs,
        unit = TimeUnit.MILLISECONDS)
    }
  }

  def currentGroups: Iterable[GroupMetadata] = groupMetadataCache.values

  def isPartitionOwned(partition: Int) = inLock(partitionLock) { ownedPartitions.contains(partition) }

  def isPartitionLoading(partition: Int) = inLock(partitionLock) { loadingPartitions.contains(partition) }

  def partitionFor(groupId: String): Int = Utils.abs(groupId.hashCode) % groupMetadataTopicPartitionCount

  def isGroupLocal(groupId: String): Boolean = isPartitionOwned(partitionFor(groupId))

  def isGroupLoading(groupId: String): Boolean = isPartitionLoading(partitionFor(groupId))

  def isLoading: Boolean = inLock(partitionLock) { loadingPartitions.nonEmpty }

  // return true iff group is owned and the group doesn't exist
  def groupNotExists(groupId: String) = inLock(partitionLock) {
    isGroupLocal(groupId) && getGroup(groupId).forall { group =>
      group.inLock(group.is(Dead))
    }
  }

  // visible for testing
  private[group] def isGroupOpenForProducer(producerId: Long, groupId: String) = openGroupsForProducer.get(producerId) match {
    case Some(groups) =>
      groups.contains(groupId)
    case None =>
      false
  }
  /**
   * Get the group associated with the given groupId, or null if not found
   */
  def getGroup(groupId: String): Option[GroupMetadata] = {
    Option(groupMetadataCache.get(groupId))
  }

  /**
   * Add a group or get the group associated with the given groupId if it already exists
   */
  def addGroup(group: GroupMetadata): GroupMetadata = {
    val currentGroup = groupMetadataCache.putIfNotExists(group.groupId, group)
    if (currentGroup != null) {
      currentGroup
    } else {
      group
    }
  }

  def storeGroup(group: GroupMetadata,
                 groupAssignment: Map[String, Array[Byte]],
                 responseCallback: Errors => Unit): Unit = {
    getMagic(partitionFor(group.groupId)) match {
      case Some(magicValue) =>
        // We always use CREATE_TIME, like the producer. The conversion to LOG_APPEND_TIME (if necessary) happens automatically.
        val timestampType = TimestampType.CREATE_TIME
        val timestamp = time.absoluteMilliseconds()
        val key = GroupMetadataManager.groupMetadataKey(group.groupId)
        val value = GroupMetadataManager.groupMetadataValue(group, groupAssignment, interBrokerProtocolVersion)

        val records = {
          val buffer = ByteBuffer.allocate(AbstractRecords.estimateSizeInBytes(magicValue, compressionType,
            Seq(new SimpleRecord(timestamp, key, value)).asJava))
          val builder = MemoryRecords.builder(buffer, magicValue, compressionType, timestampType, 0L)
          builder.append(timestamp, key, value)
          builder.build()
        }

        val groupMetadataPartition = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, partitionFor(group.groupId))
        val groupMetadataRecords = Map(groupMetadataPartition -> records)
        val generationId = group.generationId

        // set the callback function to insert the created group into cache after log append completed
        def putCacheCallback(responseStatus: Map[TopicPartition, PartitionResponse]) {
          // the append response should only contain the topics partition
          if (responseStatus.size != 1 || !responseStatus.contains(groupMetadataPartition))
            throw new IllegalStateException("Append status %s should only have one partition %s"
              .format(responseStatus, groupMetadataPartition))

          // construct the error status in the propagated assignment response in the cache
          val status = responseStatus(groupMetadataPartition)

          val responseError = if (status.error == Errors.NONE) {
            Errors.NONE
          } else {
            debug(s"Metadata from group ${group.groupId} with generation $generationId failed when appending to log " +
              s"due to ${status.error.exceptionName}")

            // transform the log append error code to the corresponding the commit status error code
            status.error match {
              case Errors.UNKNOWN_TOPIC_OR_PARTITION
                   | Errors.NOT_ENOUGH_REPLICAS
                   | Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND =>
                Errors.COORDINATOR_NOT_AVAILABLE

              case Errors.NOT_LEADER_FOR_PARTITION
                   | Errors.KAFKA_STORAGE_ERROR =>
                Errors.NOT_COORDINATOR

              case Errors.REQUEST_TIMED_OUT =>
                Errors.REBALANCE_IN_PROGRESS

              case Errors.MESSAGE_TOO_LARGE
                   | Errors.RECORD_LIST_TOO_LARGE
                   | Errors.INVALID_FETCH_SIZE =>

                error(s"Appending metadata message for group ${group.groupId} generation $generationId failed due to " +
                  s"${status.error.exceptionName}, returning UNKNOWN error code to the client")

                Errors.UNKNOWN_SERVER_ERROR

              case other =>
                error(s"Appending metadata message for group ${group.groupId} generation $generationId failed " +
                  s"due to unexpected error: ${status.error.exceptionName}")

                other
            }
          }

          responseCallback(responseError)
        }
        appendForGroup(group, groupMetadataRecords, putCacheCallback)

      case None =>
        responseCallback(Errors.NOT_COORDINATOR)
        None
    }
  }

  private def appendForGroup(group: GroupMetadata,
                             records: Map[TopicPartition, MemoryRecords],
                             callback: Map[TopicPartition, PartitionResponse] => Unit): Unit = {
    // call replica manager to append the group message
    replicaManager.appendRecords(
      timeout = config.offsetCommitTimeoutMs.toLong,
      requiredAcks = config.offsetCommitRequiredAcks,
      internalTopicsAllowed = true,
      isFromClient = false,
      entriesPerPartition = records,
      delayedProduceLock = Some(group.lock),
      responseCallback = callback)
  }

  /**
   * Store offsets by appending it to the replicated log and then inserting to cache
   */
  def storeOffsets(group: GroupMetadata,
                   consumerId: String,
                   offsetMetadata: immutable.Map[TopicPartition, OffsetAndMetadata],
                   responseCallback: immutable.Map[TopicPartition, Errors] => Unit,
                   producerId: Long = RecordBatch.NO_PRODUCER_ID,
                   producerEpoch: Short = RecordBatch.NO_PRODUCER_EPOCH): Unit = {
    // first filter out partitions with offset metadata size exceeding limit
    val filteredOffsetMetadata = offsetMetadata.filter { case (_, offsetAndMetadata) =>
      validateOffsetMetadataLength(offsetAndMetadata.metadata)
    }

    group.inLock {
      if (!group.hasReceivedConsistentOffsetCommits)
        warn(s"group: ${group.groupId} with leader: ${group.leaderOrNull} has received offset commits from consumers as well " +
          s"as transactional producers. Mixing both types of offset commits will generally result in surprises and " +
          s"should be avoided.")
    }

    val isTxnOffsetCommit = producerId != RecordBatch.NO_PRODUCER_ID
    // construct the message set to append
    if (filteredOffsetMetadata.isEmpty) {
      // compute the final error codes for the commit response
      val commitStatus = offsetMetadata.mapValues(_ => Errors.OFFSET_METADATA_TOO_LARGE)
      responseCallback(commitStatus)
      None
    } else {
      getMagic(partitionFor(group.groupId)) match {
        case Some(magicValue) =>
          // We always use CREATE_TIME, like the producer. The conversion to LOG_APPEND_TIME (if necessary) happens automatically.
          val timestampType = TimestampType.CREATE_TIME
          val timestamp = time.absoluteMilliseconds()

          val records = filteredOffsetMetadata.map { case (topicPartition, offsetAndMetadata) =>
            val key = GroupMetadataManager.offsetCommitKey(group.groupId, topicPartition)
            val value = GroupMetadataManager.offsetCommitValue(offsetAndMetadata, interBrokerProtocolVersion)
            new SimpleRecord(timestamp, key, value)
          }
          val offsetTopicPartition = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, partitionFor(group.groupId))
          val buffer = ByteBuffer.allocate(AbstractRecords.estimateSizeInBytes(magicValue, compressionType, records.asJava))

          if (isTxnOffsetCommit && magicValue < RecordBatch.MAGIC_VALUE_V2)
            throw Errors.UNSUPPORTED_FOR_MESSAGE_FORMAT.exception("Attempting to make a transaction offset commit with an invalid magic: " + magicValue)

          val builder = MemoryRecords.builder(buffer, magicValue, compressionType, timestampType, 0L, time.absoluteMilliseconds(),
            producerId, producerEpoch, 0, isTxnOffsetCommit, RecordBatch.NO_PARTITION_LEADER_EPOCH)

          records.foreach(builder.append)
          val entries = Map(offsetTopicPartition -> builder.build())

          // set the callback function to insert offsets into cache after log append completed
          def putCacheCallback(responseStatus: Map[TopicPartition, PartitionResponse]) {
            // the append response should only contain the topics partition
            if (responseStatus.size != 1 || !responseStatus.contains(offsetTopicPartition))
              throw new IllegalStateException("Append status %s should only have one partition %s"
                .format(responseStatus, offsetTopicPartition))

            // construct the commit response status and insert
            // the offset and metadata to cache if the append status has no error
            val status = responseStatus(offsetTopicPartition)

            val responseError = group.inLock {
              if (status.error == Errors.NONE) {
                if (!group.is(Dead)) {
                  filteredOffsetMetadata.foreach { case (topicPartition, offsetAndMetadata) =>
                    if (isTxnOffsetCommit)
                      group.onTxnOffsetCommitAppend(producerId, topicPartition, CommitRecordMetadataAndOffset(Some(status.baseOffset), offsetAndMetadata))
                    else
                      group.onOffsetCommitAppend(topicPartition, CommitRecordMetadataAndOffset(Some(status.baseOffset), offsetAndMetadata))
                  }
                }
                Errors.NONE
              } else {
                if (!group.is(Dead)) {
                  if (!group.hasPendingOffsetCommitsFromProducer(producerId))
                    removeProducerGroup(producerId, group.groupId)
                  filteredOffsetMetadata.foreach { case (topicPartition, offsetAndMetadata) =>
                    if (isTxnOffsetCommit)
                      group.failPendingTxnOffsetCommit(producerId, topicPartition)
                    else
                      group.failPendingOffsetWrite(topicPartition, offsetAndMetadata)
                  }
                }

                debug(s"Offset commit $filteredOffsetMetadata from group ${group.groupId}, consumer $consumerId " +
                  s"with generation ${group.generationId} failed when appending to log due to ${status.error.exceptionName}")

                // transform the log append error code to the corresponding the commit status error code
                status.error match {
                  case Errors.UNKNOWN_TOPIC_OR_PARTITION
                       | Errors.NOT_ENOUGH_REPLICAS
                       | Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND =>
                    Errors.COORDINATOR_NOT_AVAILABLE

                  case Errors.NOT_LEADER_FOR_PARTITION
                       | Errors.KAFKA_STORAGE_ERROR =>
                    Errors.NOT_COORDINATOR

                  case Errors.MESSAGE_TOO_LARGE
                       | Errors.RECORD_LIST_TOO_LARGE
                       | Errors.INVALID_FETCH_SIZE =>
                    Errors.INVALID_COMMIT_OFFSET_SIZE

                  case other => other
                }
              }
            }

            // compute the final error codes for the commit response
            val commitStatus = offsetMetadata.map { case (topicPartition, offsetAndMetadata) =>
              if (validateOffsetMetadataLength(offsetAndMetadata.metadata))
                (topicPartition, responseError)
              else
                (topicPartition, Errors.OFFSET_METADATA_TOO_LARGE)
            }

            // finally trigger the callback logic passed from the API layer
            responseCallback(commitStatus)
          }

          if (isTxnOffsetCommit) {
            group.inLock {
              addProducerGroup(producerId, group.groupId)
              group.prepareTxnOffsetCommit(producerId, offsetMetadata)
            }
          } else {
            group.inLock {
              group.prepareOffsetCommit(offsetMetadata)
            }
          }

          appendForGroup(group, entries, putCacheCallback)

        case None =>
          val commitStatus = offsetMetadata.map { case (topicPartition, _) =>
            (topicPartition, Errors.NOT_COORDINATOR)
          }
          responseCallback(commitStatus)
          None
      }
    }
  }

  /**
   * The most important guarantee that this API provides is that it should never return a stale offset. i.e., it either
   * returns the current offset or it begins to sync the cache from the log (and returns an error code).
   */
  def getOffsets(groupId: String, topicPartitionsOpt: Option[Seq[TopicPartition]]): Map[TopicPartition, OffsetFetchResponse.PartitionData] = {
    trace("Getting offsets of %s for group %s.".format(topicPartitionsOpt.getOrElse("all partitions"), groupId))
    val group = groupMetadataCache.get(groupId)
    if (group == null) {
      topicPartitionsOpt.getOrElse(Seq.empty[TopicPartition]).map { topicPartition =>
        val partitionData = new OffsetFetchResponse.PartitionData(OffsetFetchResponse.INVALID_OFFSET,
          Optional.empty(), "", Errors.NONE)
        topicPartition -> partitionData
      }.toMap
    } else {
      group.inLock {
        if (group.is(Dead)) {
          topicPartitionsOpt.getOrElse(Seq.empty[TopicPartition]).map { topicPartition =>
            val partitionData = new OffsetFetchResponse.PartitionData(OffsetFetchResponse.INVALID_OFFSET,
              Optional.empty(), "", Errors.NONE)
            topicPartition -> partitionData
          }.toMap
        } else {
          topicPartitionsOpt match {
            case None =>
              // Return offsets for all partitions owned by this consumer group. (this only applies to consumers
              // that commit offsets to Kafka.)
              group.allOffsets.map { case (topicPartition, offsetAndMetadata) =>
                topicPartition -> new OffsetFetchResponse.PartitionData(offsetAndMetadata.offset,
                  offsetAndMetadata.leaderEpoch, offsetAndMetadata.metadata, Errors.NONE)
              }

            case Some(topicPartitions) =>
              topicPartitions.map { topicPartition =>
                val partitionData = group.offset(topicPartition) match {
                  case None =>
                    new OffsetFetchResponse.PartitionData(OffsetFetchResponse.INVALID_OFFSET,
                      Optional.empty(), "", Errors.NONE)
                  case Some(offsetAndMetadata) =>
                    new OffsetFetchResponse.PartitionData(offsetAndMetadata.offset,
                      offsetAndMetadata.leaderEpoch, offsetAndMetadata.metadata, Errors.NONE)
                }
                topicPartition -> partitionData
              }.toMap
          }
        }
      }
    }
  }

  /**
   * Asynchronously read the partition from the offsets topic and populate the cache
   */
  def scheduleLoadGroupAndOffsets(offsetsPartition: Int, onGroupLoaded: GroupMetadata => Unit) {
    val topicPartition = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, offsetsPartition)
    if (addLoadingPartition(offsetsPartition)) {
      info(s"Scheduling loading of offsets and group metadata from $topicPartition")
      scheduler.schedule(topicPartition.toString, () => loadGroupsAndOffsets(topicPartition, onGroupLoaded))
    } else {
      info(s"Already loading offsets and group metadata from $topicPartition")
    }
  }

  private[group] def loadGroupsAndOffsets(topicPartition: TopicPartition, onGroupLoaded: GroupMetadata => Unit) {
    try {
      val startMs = time.absoluteMilliseconds()
      doLoadGroupsAndOffsets(topicPartition, onGroupLoaded)
      info(s"Finished loading offsets and group metadata from $topicPartition in ${time.absoluteMilliseconds() - startMs} milliseconds.")
    } catch {
      case t: Throwable => error(s"Error loading offsets from $topicPartition", t)
    } finally {
      inLock(partitionLock) {
        ownedPartitions.add(topicPartition.partition)
        loadingPartitions.remove(topicPartition.partition)
      }
    }
  }

  private def doLoadGroupsAndOffsets(topicPartition: TopicPartition, onGroupLoaded: GroupMetadata => Unit) {
    def logEndOffset: Long = replicaManager.getLogEndOffset(topicPartition).getOrElse(-1L)

    replicaManager.getLog(topicPartition) match {
      case None =>
        warn(s"Attempted to load offsets and group metadata from $topicPartition, but found no log")

      case Some(log) =>
        var currOffset = log.logStartOffset

        // buffer may not be needed if records are read from memory
        var buffer = ByteBuffer.allocate(0)

        // loop breaks if leader changes at any time during the load, since getHighWatermark is -1
        val loadedOffsets = mutable.Map[GroupTopicPartition, CommitRecordMetadataAndOffset]()
        val pendingOffsets = mutable.Map[Long, mutable.Map[GroupTopicPartition, CommitRecordMetadataAndOffset]]()
        val loadedGroups = mutable.Map[String, GroupMetadata]()
        val removedGroups = mutable.Set[String]()

        while (currOffset < logEndOffset && !shuttingDown.get()) {
          val fetchDataInfo = log.read(currOffset, config.loadBufferSize, maxOffset = None,
            minOneMessage = true, includeAbortedTxns = false)
          val memRecords = fetchDataInfo.records match {
            case records: MemoryRecords => records
            case fileRecords: FileRecords =>
              val sizeInBytes = fileRecords.sizeInBytes
              val bytesNeeded = Math.max(config.loadBufferSize, sizeInBytes)

              // minOneMessage = true in the above log.read means that the buffer may need to be grown to ensure progress can be made
              if (buffer.capacity < bytesNeeded) {
                if (config.loadBufferSize < bytesNeeded)
                  warn(s"Loaded offsets and group metadata from $topicPartition with buffer larger ($bytesNeeded bytes) than " +
                    s"configured offsets.load.buffer.size (${config.loadBufferSize} bytes)")

                buffer = ByteBuffer.allocate(bytesNeeded)
              } else {
                buffer.clear()
              }

              fileRecords.readInto(buffer, 0)
              MemoryRecords.readableRecords(buffer)
          }

          memRecords.batches.asScala.foreach { batch =>
            val isTxnOffsetCommit = batch.isTransactional
            if (batch.isControlBatch) {
              val recordIterator = batch.iterator
              if (recordIterator.hasNext) {
                val record = recordIterator.next()
                val controlRecord = ControlRecordType.parse(record.key)
                if (controlRecord == ControlRecordType.COMMIT) {
                  pendingOffsets.getOrElse(batch.producerId, mutable.Map[GroupTopicPartition, CommitRecordMetadataAndOffset]())
                    .foreach {
                      case (groupTopicPartition, commitRecordMetadataAndOffset) =>
                        if (!loadedOffsets.contains(groupTopicPartition) || loadedOffsets(groupTopicPartition).olderThan(commitRecordMetadataAndOffset))
                          loadedOffsets.put(groupTopicPartition, commitRecordMetadataAndOffset)
                    }
                }
                pendingOffsets.remove(batch.producerId)
              }
            } else {
              var batchBaseOffset: Option[Long] = None
              for (record <- batch.asScala) {
                require(record.hasKey, "Group metadata/offset entry key should not be null")
                if (batchBaseOffset.isEmpty)
                  batchBaseOffset = Some(record.offset)
                GroupMetadataManager.readMessageKey(record.key) match {

                  case offsetKey: OffsetKey =>
                    if (isTxnOffsetCommit && !pendingOffsets.contains(batch.producerId))
                      pendingOffsets.put(batch.producerId, mutable.Map[GroupTopicPartition, CommitRecordMetadataAndOffset]())

                    // load offset
                    val groupTopicPartition = offsetKey.key
                    if (!record.hasValue) {
                      if (isTxnOffsetCommit)
                        pendingOffsets(batch.producerId).remove(groupTopicPartition)
                      else
                        loadedOffsets.remove(groupTopicPartition)
                    } else {
                      val offsetAndMetadata = GroupMetadataManager.readOffsetMessageValue(record.value)
                      if (isTxnOffsetCommit)
                        pendingOffsets(batch.producerId).put(groupTopicPartition, CommitRecordMetadataAndOffset(batchBaseOffset, offsetAndMetadata))
                      else
                        loadedOffsets.put(groupTopicPartition, CommitRecordMetadataAndOffset(batchBaseOffset, offsetAndMetadata))
                    }

                  case groupMetadataKey: GroupMetadataKey =>
                    // load group metadata
                    val groupId = groupMetadataKey.key
                    val groupMetadata = GroupMetadataManager.readGroupMessageValue(groupId, record.value, time)
                    if (groupMetadata != null) {
                      removedGroups.remove(groupId)
                      loadedGroups.put(groupId, groupMetadata)
                    } else {
                      loadedGroups.remove(groupId)
                      removedGroups.add(groupId)
                    }

                  case unknownKey =>
                    throw new IllegalStateException(s"Unexpected message key $unknownKey while loading offsets and group metadata")
                }
              }
            }
            currOffset = batch.nextOffset
          }
        }

        val (groupOffsets, emptyGroupOffsets) = loadedOffsets
          .groupBy(_._1.group)
          .mapValues(_.map { case (groupTopicPartition, offset) => (groupTopicPartition.topicPartition, offset) })
          .partition { case (group, _) => loadedGroups.contains(group) }

        val pendingOffsetsByGroup = mutable.Map[String, mutable.Map[Long, mutable.Map[TopicPartition, CommitRecordMetadataAndOffset]]]()
        pendingOffsets.foreach { case (producerId, producerOffsets) =>
          producerOffsets.keySet.map(_.group).foreach(addProducerGroup(producerId, _))
          producerOffsets
            .groupBy(_._1.group)
            .mapValues(_.map { case (groupTopicPartition, offset) => (groupTopicPartition.topicPartition, offset)})
            .foreach { case (group, offsets) =>
              val groupPendingOffsets = pendingOffsetsByGroup.getOrElseUpdate(group, mutable.Map.empty[Long, mutable.Map[TopicPartition, CommitRecordMetadataAndOffset]])
              val groupProducerOffsets = groupPendingOffsets.getOrElseUpdate(producerId, mutable.Map.empty[TopicPartition, CommitRecordMetadataAndOffset])
              groupProducerOffsets ++= offsets
            }
        }

        val (pendingGroupOffsets, pendingEmptyGroupOffsets) = pendingOffsetsByGroup
          .partition { case (group, _) => loadedGroups.contains(group)}

        loadedGroups.values.foreach { group =>
          val offsets = groupOffsets.getOrElse(group.groupId, Map.empty[TopicPartition, CommitRecordMetadataAndOffset])
          val pendingOffsets = pendingGroupOffsets.getOrElse(group.groupId, Map.empty[Long, mutable.Map[TopicPartition, CommitRecordMetadataAndOffset]])
          debug(s"Loaded group metadata $group with offsets $offsets and pending offsets $pendingOffsets")
          loadGroup(group, offsets, pendingOffsets)
          onGroupLoaded(group)
        }

        // load groups which store offsets in kafka, but which have no active members and thus no group
        // metadata stored in the log
        (emptyGroupOffsets.keySet ++ pendingEmptyGroupOffsets.keySet).foreach { groupId =>
          val group = new GroupMetadata(groupId, Empty, time)
          val offsets = emptyGroupOffsets.getOrElse(groupId, Map.empty[TopicPartition, CommitRecordMetadataAndOffset])
          val pendingOffsets = pendingEmptyGroupOffsets.getOrElse(groupId, Map.empty[Long, mutable.Map[TopicPartition, CommitRecordMetadataAndOffset]])
          debug(s"Loaded group metadata $group with offsets $offsets and pending offsets $pendingOffsets")
          loadGroup(group, offsets, pendingOffsets)
          onGroupLoaded(group)
        }

        removedGroups.foreach { groupId =>
          // if the cache already contains a group which should be removed, raise an error. Note that it
          // is possible (however unlikely) for a consumer group to be removed, and then to be used only for
          // offset storage (i.e. by "simple" consumers)
          if (groupMetadataCache.contains(groupId) && !emptyGroupOffsets.contains(groupId))
            throw new IllegalStateException(s"Unexpected unload of active group $groupId while " +
              s"loading partition $topicPartition")
        }
    }
  }

  private def loadGroup(group: GroupMetadata, offsets: Map[TopicPartition, CommitRecordMetadataAndOffset],
                        pendingTransactionalOffsets: Map[Long, mutable.Map[TopicPartition, CommitRecordMetadataAndOffset]]): Unit = {
    // offsets are initialized prior to loading the group into the cache to ensure that clients see a consistent
    // view of the group's offsets
    trace(s"Initialized offsets $offsets for group ${group.groupId}")
    group.initializeOffsets(offsets, pendingTransactionalOffsets.toMap)

    val currentGroup = addGroup(group)
    if (group != currentGroup)
      debug(s"Attempt to load group ${group.groupId} from log with generation ${group.generationId} failed " +
        s"because there is already a cached group with generation ${currentGroup.generationId}")
  }

  /**
   * When this broker becomes a follower for an offsets topic partition clear out the cache for groups that belong to
   * that partition.
   *
   * @param offsetsPartition Groups belonging to this partition of the offsets topic will be deleted from the cache.
   */
  def removeGroupsForPartition(offsetsPartition: Int,
                               onGroupUnloaded: GroupMetadata => Unit) {
    val topicPartition = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, offsetsPartition)
    info(s"Scheduling unloading of offsets and group metadata from $topicPartition")
    scheduler.schedule(topicPartition.toString, () => removeGroupsAndOffsets)

    def removeGroupsAndOffsets() {
      var numOffsetsRemoved = 0
      var numGroupsRemoved = 0

      inLock(partitionLock) {
        // we need to guard the group removal in cache in the loading partition lock
        // to prevent coordinator's check-and-get-group race condition
        ownedPartitions.remove(offsetsPartition)

        for (group <- groupMetadataCache.values) {
          if (partitionFor(group.groupId) == offsetsPartition) {
            onGroupUnloaded(group)
            groupMetadataCache.remove(group.groupId, group)
            removeGroupFromAllProducers(group.groupId)
            numGroupsRemoved += 1
            numOffsetsRemoved += group.numOffsets
          }
        }
      }

      info(s"Finished unloading $topicPartition. Removed $numOffsetsRemoved cached offsets " +
        s"and $numGroupsRemoved cached groups.")
    }
  }

  // visible for testing
  private[group] def cleanupGroupMetadata(): Unit = {
    val currentTimestamp = time.absoluteMilliseconds()
    val numOffsetsRemoved = cleanupGroupMetadata(groupMetadataCache.values, group => {
      group.removeExpiredOffsets(currentTimestamp, config.offsetsRetentionMs)
    })
    info(s"Removed $numOffsetsRemoved expired offsets in ${time.absoluteMilliseconds() - currentTimestamp} milliseconds.")
  }

  /**
    * This function is used to clean up group offsets given the groups and also a function that performs the offset deletion.
    * @param groups Groups whose metadata are to be cleaned up
    * @param selector A function that implements deletion of (all or part of) group offsets. This function is called while
    *                 a group lock is held, therefore there is no need for the caller to also obtain a group lock.
    * @return The cumulative number of offsets removed
    */
  def cleanupGroupMetadata(groups: Iterable[GroupMetadata], selector: GroupMetadata => Map[TopicPartition, OffsetAndMetadata]): Int = {
    var offsetsRemoved = 0

    groups.foreach { group =>
      val groupId = group.groupId
      val (removedOffsets, groupIsDead, generation) = group.inLock {
        val removedOffsets = selector(group)
        if (group.is(Empty) && !group.hasOffsets) {
          info(s"Group $groupId transitioned to Dead in generation ${group.generationId}")
          group.transitionTo(Dead)
        }
        (removedOffsets, group.is(Dead), group.generationId)
      }

    val offsetsPartition = partitionFor(groupId)
    val appendPartition = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, offsetsPartition)
    getMagic(offsetsPartition) match {
      case Some(magicValue) =>
        // We always use CREATE_TIME, like the producer. The conversion to LOG_APPEND_TIME (if necessary) happens automatically.
        val timestampType = TimestampType.CREATE_TIME
        val timestamp = time.absoluteMilliseconds()

          replicaManager.nonOfflinePartition(appendPartition).foreach { partition =>
            val tombstones = ListBuffer.empty[SimpleRecord]
            removedOffsets.foreach { case (topicPartition, offsetAndMetadata) =>
              trace(s"Removing expired/deleted offset and metadata for $groupId, $topicPartition: $offsetAndMetadata")
              val commitKey = GroupMetadataManager.offsetCommitKey(groupId, topicPartition)
              tombstones += new SimpleRecord(timestamp, commitKey, null)
            }
            trace(s"Marked ${removedOffsets.size} offsets in $appendPartition for deletion.")

            // We avoid writing the tombstone when the generationId is 0, since this group is only using
            // Kafka for offset storage.
            if (groupIsDead && groupMetadataCache.remove(groupId, group) && generation > 0) {
              // Append the tombstone messages to the partition. It is okay if the replicas don't receive these (say,
              // if we crash or leaders move) since the new leaders will still expire the consumers with heartbeat and
              // retry removing this group.
              val groupMetadataKey = GroupMetadataManager.groupMetadataKey(group.groupId)
              tombstones += new SimpleRecord(timestamp, groupMetadataKey, null)
              trace(s"Group $groupId removed from the metadata cache and marked for deletion in $appendPartition.")
            }

            if (tombstones.nonEmpty) {
              try {
                // do not need to require acks since even if the tombstone is lost,
                // it will be appended again in the next purge cycle
                val records = MemoryRecords.withRecords(magicValue, 0L, compressionType, timestampType, tombstones: _*)
                partition.appendRecordsToLeader(records, isFromClient = false, requiredAcks = 0)

                offsetsRemoved += removedOffsets.size
                trace(s"Successfully appended ${tombstones.size} tombstones to $appendPartition for expired/deleted " +
                  s"offsets and/or metadata for group $groupId")
              } catch {
                case t: Throwable =>
                  error(s"Failed to append ${tombstones.size} tombstones to $appendPartition for expired/deleted " +
                    s"offsets and/or metadata for group $groupId.", t)
                // ignore and continue
              }
            }
          }

        case None =>
          info(s"BrokerId $brokerId is no longer a coordinator for the group $groupId. Proceeding cleanup for other alive groups")
      }
    }

    offsetsRemoved
  }

  /**
   * Complete pending transactional offset commits of the groups of `producerId` from the provided
   * `completedPartitions`. This method is invoked when a commit or abort marker is fully written
   * to the log. It may be invoked when a group lock is held by the caller, for instance when delayed
   * operations are completed while appending offsets for a group. Since we need to acquire one or
   * more group metadata locks to handle transaction completion, this operation is scheduled on
   * the scheduler thread to avoid deadlocks.
   */
  def scheduleHandleTxnCompletion(producerId: Long, completedPartitions: Set[Int], isCommit: Boolean): Unit = {
    scheduler.schedule(s"handleTxnCompletion-$producerId", () =>
      handleTxnCompletion(producerId, completedPartitions, isCommit))
  }

  private[group] def handleTxnCompletion(producerId: Long, completedPartitions: Set[Int], isCommit: Boolean): Unit = {
    val pendingGroups = groupsBelongingToPartitions(producerId, completedPartitions)
    pendingGroups.foreach { case (groupId) =>
      getGroup(groupId) match {
        case Some(group) => group.inLock {
          if (!group.is(Dead)) {
            group.completePendingTxnOffsetCommit(producerId, isCommit)
            removeProducerGroup(producerId, groupId)
          }
        }
        case _ =>
          info(s"Group $groupId has moved away from $brokerId after transaction marker was written but before the " +
            s"cache was updated. The cache on the new group owner will be updated instead.")
      }
    }
  }

  private def addProducerGroup(producerId: Long, groupId: String) = openGroupsForProducer synchronized {
    openGroupsForProducer.getOrElseUpdate(producerId, mutable.Set.empty[String]).add(groupId)
  }

  private def removeProducerGroup(producerId: Long, groupId: String) = openGroupsForProducer synchronized {
    openGroupsForProducer.getOrElseUpdate(producerId, mutable.Set.empty[String]).remove(groupId)
    if (openGroupsForProducer(producerId).isEmpty)
      openGroupsForProducer.remove(producerId)
  }

  private def groupsBelongingToPartitions(producerId: Long, partitions: Set[Int]) = openGroupsForProducer synchronized {
    val (ownedGroups, _) = openGroupsForProducer.getOrElse(producerId, mutable.Set.empty[String])
      .partition { case (group) => partitions.contains(partitionFor(group)) }
    ownedGroups
  }

  private def removeGroupFromAllProducers(groupId: String) = openGroupsForProducer synchronized {
    openGroupsForProducer.foreach { case (_, groups) =>
      groups.remove(groupId)
    }
  }

  /*
   * Check if the offset metadata length is valid
   */
  private def validateOffsetMetadataLength(metadata: String) : Boolean = {
    metadata == null || metadata.length() <= config.maxMetadataSize
  }


  def shutdown() {
    shuttingDown.set(true)
    if (scheduler.isStarted)
      scheduler.shutdown()

    // TODO: clear the caches
  }

  /**
   * Gets the partition count of the group metadata topic from ZooKeeper.
   * If the topic does not exist, the configured partition count is returned.
   */
  private def getGroupMetadataTopicPartitionCount: Int = {
    zkClient.getTopicPartitionCount(Topic.GROUP_METADATA_TOPIC_NAME).getOrElse(config.offsetsTopicNumPartitions)
  }

  /**
   * Check if the replica is local and return the message format version and timestamp
   *
   * @param   partition  Partition of GroupMetadataTopic
   * @return  Some(MessageFormatVersion) if replica is local, None otherwise
   */
  private def getMagic(partition: Int): Option[Byte] =
    replicaManager.getMagic(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, partition))

  /**
   * Add the partition into the owned list
   *
   * NOTE: this is for test only
   */
  private[group] def addPartitionOwnership(partition: Int) {
    inLock(partitionLock) {
      ownedPartitions.add(partition)
    }
  }

  /**
   * Add a partition to the loading partitions set. Return true if the partition was not
   * already loading.
   *
   * Visible for testing
   */
  private[group] def addLoadingPartition(partition: Int): Boolean = {
    inLock(partitionLock) {
      loadingPartitions.add(partition)
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
 *    -> value version 0:       [protocol_type, generation, protocol, leader, members]
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

  private val OFFSET_COMMIT_VALUE_SCHEMA_V2 = new Schema(new Field("offset", INT64),
    new Field("metadata", STRING, "Associated metadata.", ""),
    new Field("commit_timestamp", INT64))
  private val OFFSET_VALUE_OFFSET_FIELD_V2 = OFFSET_COMMIT_VALUE_SCHEMA_V2.get("offset")
  private val OFFSET_VALUE_METADATA_FIELD_V2 = OFFSET_COMMIT_VALUE_SCHEMA_V2.get("metadata")
  private val OFFSET_VALUE_COMMIT_TIMESTAMP_FIELD_V2 = OFFSET_COMMIT_VALUE_SCHEMA_V2.get("commit_timestamp")

  private val OFFSET_COMMIT_VALUE_SCHEMA_V3 = new Schema(
    new Field("offset", INT64),
    new Field("leader_epoch", INT32),
    new Field("metadata", STRING, "Associated metadata.", ""),
    new Field("commit_timestamp", INT64))
  private val OFFSET_VALUE_OFFSET_FIELD_V3 = OFFSET_COMMIT_VALUE_SCHEMA_V3.get("offset")
  private val OFFSET_VALUE_LEADER_EPOCH_FIELD_V3 = OFFSET_COMMIT_VALUE_SCHEMA_V3.get("leader_epoch")
  private val OFFSET_VALUE_METADATA_FIELD_V3 = OFFSET_COMMIT_VALUE_SCHEMA_V3.get("metadata")
  private val OFFSET_VALUE_COMMIT_TIMESTAMP_FIELD_V3 = OFFSET_COMMIT_VALUE_SCHEMA_V3.get("commit_timestamp")

  private val GROUP_METADATA_KEY_SCHEMA = new Schema(new Field("group", STRING))
  private val GROUP_KEY_GROUP_FIELD = GROUP_METADATA_KEY_SCHEMA.get("group")

  private val MEMBER_ID_KEY = "member_id"
  private val CLIENT_ID_KEY = "client_id"
  private val CLIENT_HOST_KEY = "client_host"
  private val REBALANCE_TIMEOUT_KEY = "rebalance_timeout"
  private val SESSION_TIMEOUT_KEY = "session_timeout"
  private val SUBSCRIPTION_KEY = "subscription"
  private val ASSIGNMENT_KEY = "assignment"

  private val MEMBER_METADATA_V0 = new Schema(
    new Field(MEMBER_ID_KEY, STRING),
    new Field(CLIENT_ID_KEY, STRING),
    new Field(CLIENT_HOST_KEY, STRING),
    new Field(SESSION_TIMEOUT_KEY, INT32),
    new Field(SUBSCRIPTION_KEY, BYTES),
    new Field(ASSIGNMENT_KEY, BYTES))

  private val MEMBER_METADATA_V1 = new Schema(
    new Field(MEMBER_ID_KEY, STRING),
    new Field(CLIENT_ID_KEY, STRING),
    new Field(CLIENT_HOST_KEY, STRING),
    new Field(REBALANCE_TIMEOUT_KEY, INT32),
    new Field(SESSION_TIMEOUT_KEY, INT32),
    new Field(SUBSCRIPTION_KEY, BYTES),
    new Field(ASSIGNMENT_KEY, BYTES))

  private val MEMBER_METADATA_V2 = MEMBER_METADATA_V1

  private val PROTOCOL_TYPE_KEY = "protocol_type"
  private val GENERATION_KEY = "generation"
  private val PROTOCOL_KEY = "protocol"
  private val LEADER_KEY = "leader"
  private val CURRENT_STATE_TIMESTAMP_KEY = "current_state_timestamp"
  private val MEMBERS_KEY = "members"

  private val GROUP_METADATA_VALUE_SCHEMA_V0 = new Schema(
    new Field(PROTOCOL_TYPE_KEY, STRING),
    new Field(GENERATION_KEY, INT32),
    new Field(PROTOCOL_KEY, NULLABLE_STRING),
    new Field(LEADER_KEY, NULLABLE_STRING),
    new Field(MEMBERS_KEY, new ArrayOf(MEMBER_METADATA_V0)))

  private val GROUP_METADATA_VALUE_SCHEMA_V1 = new Schema(
    new Field(PROTOCOL_TYPE_KEY, STRING),
    new Field(GENERATION_KEY, INT32),
    new Field(PROTOCOL_KEY, NULLABLE_STRING),
    new Field(LEADER_KEY, NULLABLE_STRING),
    new Field(MEMBERS_KEY, new ArrayOf(MEMBER_METADATA_V1)))

  private val GROUP_METADATA_VALUE_SCHEMA_V2 = new Schema(
    new Field(PROTOCOL_TYPE_KEY, STRING),
    new Field(GENERATION_KEY, INT32),
    new Field(PROTOCOL_KEY, NULLABLE_STRING),
    new Field(LEADER_KEY, NULLABLE_STRING),
    new Field(CURRENT_STATE_TIMESTAMP_KEY, INT64),
    new Field(MEMBERS_KEY, new ArrayOf(MEMBER_METADATA_V2)))

  // map of versions to key schemas as data types
  private val MESSAGE_TYPE_SCHEMAS = Map(
    0 -> OFFSET_COMMIT_KEY_SCHEMA,
    1 -> OFFSET_COMMIT_KEY_SCHEMA,
    2 -> GROUP_METADATA_KEY_SCHEMA)

  // map of version of offset value schemas
  private val OFFSET_VALUE_SCHEMAS = Map(
    0 -> OFFSET_COMMIT_VALUE_SCHEMA_V0,
    1 -> OFFSET_COMMIT_VALUE_SCHEMA_V1,
    2 -> OFFSET_COMMIT_VALUE_SCHEMA_V2,
    3 -> OFFSET_COMMIT_VALUE_SCHEMA_V3)

  // map of version of group metadata value schemas
  private val GROUP_VALUE_SCHEMAS = Map(
    0 -> GROUP_METADATA_VALUE_SCHEMA_V0,
    1 -> GROUP_METADATA_VALUE_SCHEMA_V1,
    2 -> GROUP_METADATA_VALUE_SCHEMA_V2)

  private val CURRENT_OFFSET_KEY_SCHEMA = schemaForKey(CURRENT_OFFSET_KEY_SCHEMA_VERSION)
  private val CURRENT_GROUP_KEY_SCHEMA = schemaForKey(CURRENT_GROUP_KEY_SCHEMA_VERSION)

  private def schemaForKey(version: Int) = {
    val schemaOpt = MESSAGE_TYPE_SCHEMAS.get(version)
    schemaOpt match {
      case Some(schema) => schema
      case _ => throw new KafkaException("Unknown offset schema version " + version)
    }
  }

  private def schemaForOffsetValue(version: Int) = {
    val schemaOpt = OFFSET_VALUE_SCHEMAS.get(version)
    schemaOpt match {
      case Some(schema) => schema
      case _ => throw new KafkaException("Unknown offset schema version " + version)
    }
  }

  private def schemaForGroupValue(version: Int) = {
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
  private[group] def offsetCommitKey(group: String,
                                     topicPartition: TopicPartition): Array[Byte] = {
    val key = new Struct(CURRENT_OFFSET_KEY_SCHEMA)
    key.set(OFFSET_KEY_GROUP_FIELD, group)
    key.set(OFFSET_KEY_TOPIC_FIELD, topicPartition.topic)
    key.set(OFFSET_KEY_PARTITION_FIELD, topicPartition.partition)

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
  private[group] def groupMetadataKey(group: String): Array[Byte] = {
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
   * @param apiVersion the api version
   * @return payload for offset commit message
   */
  private[group] def offsetCommitValue(offsetAndMetadata: OffsetAndMetadata,
                                       apiVersion: ApiVersion): Array[Byte] = {
    // generate commit value according to schema version
    val (version, value) = {
      if (apiVersion < KAFKA_2_1_IV0 || offsetAndMetadata.expireTimestamp.nonEmpty) {
        val value = new Struct(OFFSET_COMMIT_VALUE_SCHEMA_V1)
        value.set(OFFSET_VALUE_OFFSET_FIELD_V1, offsetAndMetadata.offset)
        value.set(OFFSET_VALUE_METADATA_FIELD_V1, offsetAndMetadata.metadata)
        value.set(OFFSET_VALUE_COMMIT_TIMESTAMP_FIELD_V1, offsetAndMetadata.commitTimestamp)
        // version 1 has a non empty expireTimestamp field
        value.set(OFFSET_VALUE_EXPIRE_TIMESTAMP_FIELD_V1,
          offsetAndMetadata.expireTimestamp.getOrElse(OffsetCommitRequest.DEFAULT_TIMESTAMP))
        (1, value)
      } else if (apiVersion < KAFKA_2_1_IV1) {
        val value = new Struct(OFFSET_COMMIT_VALUE_SCHEMA_V2)
        value.set(OFFSET_VALUE_OFFSET_FIELD_V2, offsetAndMetadata.offset)
        value.set(OFFSET_VALUE_METADATA_FIELD_V2, offsetAndMetadata.metadata)
        value.set(OFFSET_VALUE_COMMIT_TIMESTAMP_FIELD_V2, offsetAndMetadata.commitTimestamp)
        (2, value)
      } else {
        val value = new Struct(OFFSET_COMMIT_VALUE_SCHEMA_V3)
        value.set(OFFSET_VALUE_OFFSET_FIELD_V3, offsetAndMetadata.offset)
        value.set(OFFSET_VALUE_LEADER_EPOCH_FIELD_V3,
          offsetAndMetadata.leaderEpoch.orElse(RecordBatch.NO_PARTITION_LEADER_EPOCH))
        value.set(OFFSET_VALUE_METADATA_FIELD_V3, offsetAndMetadata.metadata)
        value.set(OFFSET_VALUE_COMMIT_TIMESTAMP_FIELD_V3, offsetAndMetadata.commitTimestamp)
        (3, value)
      }
    }

    val byteBuffer = ByteBuffer.allocate(2 /* version */ + value.sizeOf)
    byteBuffer.putShort(version.toShort)
    value.writeTo(byteBuffer)
    byteBuffer.array()
  }

  /**
   * Generates the payload for group metadata message from given offset and metadata
   * assuming the generation id, selected protocol, leader and member assignment are all available
   *
   * @param groupMetadata current group metadata
   * @param assignment the assignment for the rebalancing generation
   * @param apiVersion the api version
   * @return payload for offset commit message
   */
  private[group] def groupMetadataValue(groupMetadata: GroupMetadata,
                                        assignment: Map[String, Array[Byte]],
                                        apiVersion: ApiVersion): Array[Byte] = {

    val (version, value) = {
      if (apiVersion < KAFKA_0_10_1_IV0)
        (0.toShort, new Struct(GROUP_METADATA_VALUE_SCHEMA_V0))
      else if (apiVersion < KAFKA_2_1_IV0)
        (1.toShort, new Struct(GROUP_METADATA_VALUE_SCHEMA_V1))
      else
        (2.toShort, new Struct(GROUP_METADATA_VALUE_SCHEMA_V2))
    }

    value.set(PROTOCOL_TYPE_KEY, groupMetadata.protocolType.getOrElse(""))
    value.set(GENERATION_KEY, groupMetadata.generationId)
    value.set(PROTOCOL_KEY, groupMetadata.protocolOrNull)
    value.set(LEADER_KEY, groupMetadata.leaderOrNull)

    if (version >= 2)
      value.set(CURRENT_STATE_TIMESTAMP_KEY, groupMetadata.currentStateTimestampOrDefault)

    val memberArray = groupMetadata.allMemberMetadata.map { memberMetadata =>
      val memberStruct = value.instance(MEMBERS_KEY)
      memberStruct.set(MEMBER_ID_KEY, memberMetadata.memberId)
      memberStruct.set(CLIENT_ID_KEY, memberMetadata.clientId)
      memberStruct.set(CLIENT_HOST_KEY, memberMetadata.clientHost)
      memberStruct.set(SESSION_TIMEOUT_KEY, memberMetadata.sessionTimeoutMs)

      if (version > 0)
        memberStruct.set(REBALANCE_TIMEOUT_KEY, memberMetadata.rebalanceTimeoutMs)

      // The group is non-empty, so the current protocol must be defined
      val protocol = groupMetadata.protocolOrNull
      if (protocol == null)
        throw new IllegalStateException("Attempted to write non-empty group metadata with no defined protocol")

      val metadata = memberMetadata.metadata(protocol)
      memberStruct.set(SUBSCRIPTION_KEY, ByteBuffer.wrap(metadata))

      val memberAssignment = assignment(memberMetadata.memberId)
      assert(memberAssignment != null)

      memberStruct.set(ASSIGNMENT_KEY, ByteBuffer.wrap(memberAssignment))

      memberStruct
    }

    value.set(MEMBERS_KEY, memberArray.toArray)

    val byteBuffer = ByteBuffer.allocate(2 /* version */ + value.sizeOf)
    byteBuffer.putShort(version)
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
      throw new IllegalStateException(s"Unknown group metadata message version: $version")
    }
  }

  /**
   * Decodes the offset messages' payload and retrieves offset and metadata from it
   *
   * @param buffer input byte-buffer
   * @return an offset-metadata object from the message
   */
  def readOffsetMessageValue(buffer: ByteBuffer): OffsetAndMetadata = {
    if (buffer == null) { // tombstone
      null
    } else {
      val version = buffer.getShort
      val valueSchema = schemaForOffsetValue(version)
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
      } else if (version == 2) {
        val offset = value.get(OFFSET_VALUE_OFFSET_FIELD_V2).asInstanceOf[Long]
        val metadata = value.get(OFFSET_VALUE_METADATA_FIELD_V2).asInstanceOf[String]
        val commitTimestamp = value.get(OFFSET_VALUE_COMMIT_TIMESTAMP_FIELD_V2).asInstanceOf[Long]

        OffsetAndMetadata(offset, metadata, commitTimestamp)
      } else if (version == 3) {
        val offset = value.get(OFFSET_VALUE_OFFSET_FIELD_V3).asInstanceOf[Long]
        val leaderEpoch = value.get(OFFSET_VALUE_LEADER_EPOCH_FIELD_V3).asInstanceOf[Int]
        val metadata = value.get(OFFSET_VALUE_METADATA_FIELD_V3).asInstanceOf[String]
        val commitTimestamp = value.get(OFFSET_VALUE_COMMIT_TIMESTAMP_FIELD_V3).asInstanceOf[Long]

        val leaderEpochOpt: Optional[Integer] = if (leaderEpoch < 0) Optional.empty() else Optional.of(leaderEpoch)
        OffsetAndMetadata(offset, leaderEpochOpt, metadata, commitTimestamp)
      } else {
        throw new IllegalStateException(s"Unknown offset message version: $version")
      }
    }
  }

  /**
   * Decodes the group metadata messages' payload and retrieves its member metadata from it
   *
   * @param buffer input byte-buffer
   * @param time the time instance to use
   * @return a group metadata object from the message
   */
  def readGroupMessageValue(groupId: String, buffer: ByteBuffer, time: Time): GroupMetadata = {
    if (buffer == null) { // tombstone
      null
    } else {
      val version = buffer.getShort
      val valueSchema = schemaForGroupValue(version)
      val value = valueSchema.read(buffer)

      if (version >= 0 && version <= 2) {
        val generationId = value.get(GENERATION_KEY).asInstanceOf[Int]
        val protocolType = value.get(PROTOCOL_TYPE_KEY).asInstanceOf[String]
        val protocol = value.get(PROTOCOL_KEY).asInstanceOf[String]
        val leaderId = value.get(LEADER_KEY).asInstanceOf[String]
        val memberMetadataArray = value.getArray(MEMBERS_KEY)
        val initialState = if (memberMetadataArray.isEmpty) Empty else Stable
        val currentStateTimestamp: Option[Long] = version match {
          case version if version == 2 =>
            if (value.hasField(CURRENT_STATE_TIMESTAMP_KEY)) {
              val timestamp = value.getLong(CURRENT_STATE_TIMESTAMP_KEY)
              if (timestamp == -1) None else Some(timestamp)
            } else
              None
          case _ =>
            None
        }

        val members = memberMetadataArray.map { memberMetadataObj =>
          val memberMetadata = memberMetadataObj.asInstanceOf[Struct]
          val memberId = memberMetadata.get(MEMBER_ID_KEY).asInstanceOf[String]
          val clientId = memberMetadata.get(CLIENT_ID_KEY).asInstanceOf[String]
          val clientHost = memberMetadata.get(CLIENT_HOST_KEY).asInstanceOf[String]
          val sessionTimeout = memberMetadata.get(SESSION_TIMEOUT_KEY).asInstanceOf[Int]
          val rebalanceTimeout = if (version == 0) sessionTimeout else memberMetadata.get(REBALANCE_TIMEOUT_KEY).asInstanceOf[Int]
          val subscription = Utils.toArray(memberMetadata.get(SUBSCRIPTION_KEY).asInstanceOf[ByteBuffer])

          val member = new MemberMetadata(memberId, groupId, clientId, clientHost, rebalanceTimeout, sessionTimeout,
            protocolType, List((protocol, subscription)))
          member.assignment = Utils.toArray(memberMetadata.get(ASSIGNMENT_KEY).asInstanceOf[ByteBuffer])
          member
        }
        GroupMetadata.loadGroup(groupId, initialState, generationId, protocolType, protocol, leaderId, currentStateTimestamp, members, time)
      } else {
        throw new IllegalStateException(s"Unknown group metadata message version: $version")
      }
    }
  }

  // Formatter for use with tools such as console consumer: Consumer should also set exclude.internal.topics to false.
  // (specify --formatter "kafka.coordinator.group.GroupMetadataManager\$OffsetsMessageFormatter" when consuming __consumer_offsets)
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
          output.write(groupTopicPartition.toString.getBytes(StandardCharsets.UTF_8))
          output.write("::".getBytes(StandardCharsets.UTF_8))
          output.write(formattedValue.getBytes(StandardCharsets.UTF_8))
          output.write("\n".getBytes(StandardCharsets.UTF_8))
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
            else GroupMetadataManager.readGroupMessageValue(groupId, ByteBuffer.wrap(value), Time.SYSTEM).toString
          output.write(groupId.getBytes(StandardCharsets.UTF_8))
          output.write("::".getBytes(StandardCharsets.UTF_8))
          output.write(formattedValue.getBytes(StandardCharsets.UTF_8))
          output.write("\n".getBytes(StandardCharsets.UTF_8))
        case _ => // no-op
      }
    }
  }

}

case class GroupTopicPartition(group: String, topicPartition: TopicPartition) {

  def this(group: String, topic: String, partition: Int) =
    this(group, new TopicPartition(topic, partition))

  override def toString: String =
    "[%s,%s,%d]".format(group, topicPartition.topic, topicPartition.partition)
}

trait BaseKey{
  def version: Short
  def key: Any
}

case class OffsetKey(version: Short, key: GroupTopicPartition) extends BaseKey {

  override def toString: String = key.toString
}

case class GroupMetadataKey(version: Short, key: String) extends BaseKey {

  override def toString: String = key
}

