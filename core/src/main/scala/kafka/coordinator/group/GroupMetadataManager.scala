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

import java.nio.ByteBuffer
import java.util.{Optional, OptionalInt, OptionalLong}
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{CompletableFuture, ConcurrentHashMap}
import java.util.function.Supplier
import com.yammer.metrics.core.Gauge
import kafka.coordinator.group.GroupMetadataManager.maybeConvertOffsetCommitError
import kafka.server.ReplicaManager
import kafka.utils.CoreUtils.inLock
import kafka.utils._
import org.apache.kafka.common.compress.Compression
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.metrics.{Metrics, Sensor}
import org.apache.kafka.common.metrics.stats.{Avg, Max, Meter}
import org.apache.kafka.common.protocol.{ByteBufferAccessor, Errors, MessageUtil}
import org.apache.kafka.common.record._
import org.apache.kafka.common.requests.OffsetFetchResponse.PartitionData
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.requests.{OffsetCommitRequest, OffsetFetchResponse}
import org.apache.kafka.common.utils.{Time, Utils}
import org.apache.kafka.common.{TopicIdPartition, TopicPartition}
import org.apache.kafka.coordinator.group.{OffsetAndMetadata, OffsetConfig}
import org.apache.kafka.coordinator.group.generated.{CoordinatorRecordType, GroupMetadataValue, LegacyOffsetCommitKey, OffsetCommitKey, OffsetCommitValue, GroupMetadataKey => GroupMetadataKeyData}
import org.apache.kafka.server.common.{MetadataVersion, RequestLocal}
import org.apache.kafka.server.common.MetadataVersion.{IBP_0_10_1_IV0, IBP_2_1_IV0, IBP_2_1_IV1, IBP_2_3_IV0}
import org.apache.kafka.server.metrics.KafkaMetricsGroup
import org.apache.kafka.server.storage.log.FetchIsolation
import org.apache.kafka.server.util.KafkaScheduler
import org.apache.kafka.storage.internals.log.{AppendOrigin, VerificationGuard}

import scala.collection._
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

class GroupMetadataManager(brokerId: Int,
                           interBrokerProtocolVersion: MetadataVersion,
                           config: OffsetConfig,
                           val replicaManager: ReplicaManager,
                           time: Time,
                           metrics: Metrics) extends Logging {
  // Visible for test.
  private[group] val metricsGroup: KafkaMetricsGroup = new KafkaMetricsGroup(this.getClass)

  private val compression: Compression = Compression.of(config.offsetsTopicCompressionType).build()

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
  @volatile private var groupMetadataTopicPartitionCount: Int = _

  /* single-thread scheduler to handle offset/group metadata cache loading and unloading */
  private val scheduler = new KafkaScheduler(1, true, "group-metadata-manager-")

  /* The groups with open transactional offsets commits per producer. We need this because when the commit or abort
   * marker comes in for a transaction, it is for a particular partition on the offsets topic and a particular producerId.
   * We use this structure to quickly find the groups which need to be updated by the commit/abort marker. */
  private val openGroupsForProducer = mutable.HashMap[Long, mutable.Set[String]]()

  /* Track the epoch in which we (un)loaded group state to detect racing LeaderAndIsr requests */
  private [group] val epochForPartitionId = new ConcurrentHashMap[Int, java.lang.Integer]()

  /* setup metrics*/
  private val partitionLoadSensor = metrics.sensor(GroupMetadataManager.LoadTimeSensor)

  partitionLoadSensor.add(metrics.metricName("partition-load-time-max",
    GroupMetadataManager.MetricsGroup,
    "The max time it took to load the partitions in the last 30sec"), new Max())
  partitionLoadSensor.add(metrics.metricName("partition-load-time-avg",
    GroupMetadataManager.MetricsGroup,
    "The avg time it took to load the partitions in the last 30sec"), new Avg())

  val offsetCommitsSensor: Sensor = metrics.sensor(GroupMetadataManager.OffsetCommitsSensor)

  offsetCommitsSensor.add(new Meter(
    metrics.metricName("offset-commit-rate",
      "group-coordinator-metrics",
      "The rate of committed offsets"),
    metrics.metricName("offset-commit-count",
      "group-coordinator-metrics",
      "The total number of committed offsets")))

  val offsetExpiredSensor: Sensor = metrics.sensor(GroupMetadataManager.OffsetExpiredSensor)

  offsetExpiredSensor.add(new Meter(
    metrics.metricName("offset-expiration-rate",
      "group-coordinator-metrics",
      "The rate of expired offsets"),
    metrics.metricName("offset-expiration-count",
      "group-coordinator-metrics",
      "The total number of expired offsets")))

  this.logIdent = s"[GroupMetadataManager brokerId=$brokerId] "

  private def recreateGauge[T](name: String, metric: Supplier[T]): Gauge[T] = {
    metricsGroup.removeMetric(name)
    metricsGroup.newGauge(name, metric)
  }

  recreateGauge("NumOffsets",
    () => groupMetadataCache.values.map { group =>
      group.inLock { group.numOffsets }
    }.sum
  )

  recreateGauge("NumGroups",
    () => groupMetadataCache.size
  )

  recreateGauge("NumGroupsPreparingRebalance",
    () => groupMetadataCache.values.count { group =>
      group synchronized {
        group.is(PreparingRebalance)
      }
    })

  recreateGauge("NumGroupsCompletingRebalance",
    () => groupMetadataCache.values.count { group =>
      group synchronized {
        group.is(CompletingRebalance)
      }
    })

  recreateGauge("NumGroupsStable",
    () => groupMetadataCache.values.count { group =>
      group synchronized {
        group.is(Stable)
      }
    })

  recreateGauge("NumGroupsDead",
    () => groupMetadataCache.values.count { group =>
      group synchronized {
        group.is(Dead)
      }
    })

  recreateGauge("NumGroupsEmpty",
    () => groupMetadataCache.values.count { group =>
      group synchronized {
        group.is(Empty)
      }
    })

  def startup(retrieveGroupMetadataTopicPartitionCount: () => Int, enableMetadataExpiration: Boolean): Unit = {
    groupMetadataTopicPartitionCount = retrieveGroupMetadataTopicPartitionCount()
    scheduler.startup()
    if (enableMetadataExpiration) {
      scheduler.schedule("delete-expired-group-metadata",
        () => cleanupGroupMetadata(),
        0L,
        config.offsetsRetentionCheckIntervalMs)
    }
  }

  def currentGroups: Iterable[GroupMetadata] = groupMetadataCache.values

  def isPartitionOwned(partition: Int): Boolean = inLock(partitionLock) { ownedPartitions.contains(partition) }

  def isPartitionLoading(partition: Int): Boolean = inLock(partitionLock) { loadingPartitions.contains(partition) }

  def partitionFor(groupId: String): Int = Utils.abs(groupId.hashCode) % groupMetadataTopicPartitionCount

  def isGroupLocal(groupId: String): Boolean = isPartitionOwned(partitionFor(groupId))

  def isGroupLoading(groupId: String): Boolean = isPartitionLoading(partitionFor(groupId))

  def isLoading: Boolean = inLock(partitionLock) { loadingPartitions.nonEmpty }

  // return true iff group is owned and the group doesn't exist
  def groupNotExists(groupId: String): Boolean = inLock(partitionLock) {
    isGroupLocal(groupId) && getGroup(groupId).forall { group =>
      group.inLock(group.is(Dead))
    }
  }

  /**
   * Get the group associated with the given groupId or null if not found
   */
  def getGroup(groupId: String): Option[GroupMetadata] = {
    Option(groupMetadataCache.get(groupId))
  }

  /**
   * Get the group associated with the given groupId - the group is created if createIfNotExist
   * is true - or null if not found
   */
  def getOrMaybeCreateGroup(groupId: String, createIfNotExist: Boolean): Option[GroupMetadata] = {
    if (createIfNotExist)
      Option(groupMetadataCache.getAndMaybePut(groupId, new GroupMetadata(groupId, Empty, time)))
    else
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
                 responseCallback: Errors => Unit,
                 requestLocal: RequestLocal = RequestLocal.noCaching): Unit = {
    getMagic(partitionFor(group.groupId)) match {
      case Some(magicValue) =>
        // We always use CREATE_TIME, like the producer. The conversion to LOG_APPEND_TIME (if necessary) happens automatically.
        val timestampType = TimestampType.CREATE_TIME
        val timestamp = time.milliseconds()
        val key = GroupMetadataManager.groupMetadataKey(group.groupId)
        val value = GroupMetadataManager.groupMetadataValue(group, groupAssignment, interBrokerProtocolVersion)

        val records = {
          val buffer = ByteBuffer.allocate(AbstractRecords.estimateSizeInBytes(magicValue, compression.`type`(),
            Seq(new SimpleRecord(timestamp, key, value)).asJava))
          val builder = MemoryRecords.builder(buffer, magicValue, compression, timestampType, 0L)
          builder.append(timestamp, key, value)
          builder.build()
        }

        val groupMetadataPartition = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, partitionFor(group.groupId))
        val groupMetadataRecords = Map(groupMetadataPartition -> records)
        val generationId = group.generationId

        // set the callback function to insert the created group into cache after log append completed
        def putCacheCallback(responseStatus: Map[TopicPartition, PartitionResponse]): Unit = {
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

              case Errors.NOT_LEADER_OR_FOLLOWER
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
        appendForGroup(group, groupMetadataRecords, requestLocal, putCacheCallback)

      case None =>
        responseCallback(Errors.NOT_COORDINATOR)
    }
  }

  // This method should be called under the group lock to ensure atomicity of the update to the the in-memory and persisted state.
  private def appendForGroup(
    group: GroupMetadata,
    records: Map[TopicPartition, MemoryRecords],
    requestLocal: RequestLocal,
    callback: Map[TopicPartition, PartitionResponse] => Unit,
    verificationGuards: Map[TopicPartition, VerificationGuard] = Map.empty
  ): Unit = {
    // call replica manager to append the group message
    replicaManager.appendRecords(
      timeout = config.offsetCommitTimeoutMs.toLong,
      requiredAcks = -1,
      internalTopicsAllowed = true,
      origin = AppendOrigin.COORDINATOR,
      entriesPerPartition = records,
      delayedProduceLock = Some(group.lock),
      responseCallback = callback,
      requestLocal = requestLocal,
      verificationGuards = verificationGuards
    )
  }

  private def generateOffsetRecords(magicValue: Byte,
                            isTxnOffsetCommit: Boolean,
                            groupId: String,
                            offsetTopicPartition: TopicPartition,
                            filteredOffsetMetadata: Map[TopicIdPartition, OffsetAndMetadata],
                            producerId: Long,
                            producerEpoch: Short): Map[TopicPartition, MemoryRecords] = {
      // We always use CREATE_TIME, like the producer. The conversion to LOG_APPEND_TIME (if necessary) happens automatically.
      val timestampType = TimestampType.CREATE_TIME
      val timestamp = time.milliseconds()

      val records = filteredOffsetMetadata.map { case (topicIdPartition, offsetAndMetadata) =>
        val key = GroupMetadataManager.offsetCommitKey(groupId, topicIdPartition.topicPartition)
        val value = GroupMetadataManager.offsetCommitValue(offsetAndMetadata, interBrokerProtocolVersion)
        new SimpleRecord(timestamp, key, value)
      }
      val buffer = ByteBuffer.allocate(AbstractRecords.estimateSizeInBytes(magicValue, compression.`type`(), records.asJava))

      if (isTxnOffsetCommit && magicValue < RecordBatch.MAGIC_VALUE_V2)
        throw Errors.UNSUPPORTED_FOR_MESSAGE_FORMAT.exception("Attempting to make a transaction offset commit with an invalid magic: " + magicValue)

      val builder = MemoryRecords.builder(buffer, magicValue, compression, timestampType, 0L, time.milliseconds(),
        producerId, producerEpoch, 0, isTxnOffsetCommit, RecordBatch.NO_PARTITION_LEADER_EPOCH)

      records.foreach(builder.append)
      Map(offsetTopicPartition -> builder.build())
  }

  private def createPutCacheCallback(isTxnOffsetCommit: Boolean,
                                     group: GroupMetadata,
                                     consumerId: String,
                                     offsetMetadata: immutable.Map[TopicIdPartition, OffsetAndMetadata],
                                     filteredOffsetMetadata: Map[TopicIdPartition, OffsetAndMetadata],
                                     responseCallback: immutable.Map[TopicIdPartition, Errors] => Unit,
                                     producerId: Long,
                                     records: Map[TopicPartition, MemoryRecords]): Map[TopicPartition, PartitionResponse] => Unit = {
    val offsetTopicPartition = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, partitionFor(group.groupId))
    // set the callback function to insert offsets into cache after log append completed
    def putCacheCallback(responseStatus: Map[TopicPartition, PartitionResponse]): Unit = {
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
            filteredOffsetMetadata.foreachEntry { (topicIdPartition, offsetAndMetadata) =>
              if (isTxnOffsetCommit)
                group.onTxnOffsetCommitAppend(producerId, topicIdPartition, CommitRecordMetadataAndOffset(Some(status.baseOffset), offsetAndMetadata))
              else
                group.onOffsetCommitAppend(topicIdPartition, CommitRecordMetadataAndOffset(Some(status.baseOffset), offsetAndMetadata))
            }
          }

          // Record the number of offsets committed to the log
          offsetCommitsSensor.record(records.size)

          Errors.NONE
        } else {
          if (!group.is(Dead)) {
            if (!group.hasPendingOffsetCommitsFromProducer(producerId))
              removeProducerGroup(producerId, group.groupId)
            filteredOffsetMetadata.foreachEntry { (topicIdPartition, offsetAndMetadata) =>
              if (isTxnOffsetCommit)
                group.failPendingTxnOffsetCommit(producerId, topicIdPartition)
              else
                group.failPendingOffsetWrite(topicIdPartition, offsetAndMetadata)
            }
          }

          debug(s"Offset commit $filteredOffsetMetadata from group ${group.groupId}, consumer $consumerId " +
            s"with generation ${group.generationId} failed when appending to log due to ${status.error.exceptionName}")

          maybeConvertOffsetCommitError(status.error)
        }
      }

      // compute the final error codes for the commit response
      val commitStatus = offsetMetadata.map { case (topicIdPartition, offsetAndMetadata) =>
        if (!validateOffsetMetadataLength(offsetAndMetadata.metadata))
          (topicIdPartition, Errors.OFFSET_METADATA_TOO_LARGE)
        else
          (topicIdPartition, responseError)
      }

      // finally trigger the callback logic passed from the API layer
      responseCallback(commitStatus)
    }
    putCacheCallback
  }

  /**
   * Store offsets by appending it to the replicated log and then inserting to cache
   *
   * This method should be called under the group lock to ensure validations and updates are all performed
   * atomically.
   */
  def storeOffsets(group: GroupMetadata,
                   consumerId: String,
                   offsetTopicPartition: TopicPartition,
                   offsetMetadata: immutable.Map[TopicIdPartition, OffsetAndMetadata],
                   responseCallback: immutable.Map[TopicIdPartition, Errors] => Unit,
                   producerId: Long = RecordBatch.NO_PRODUCER_ID,
                   producerEpoch: Short = RecordBatch.NO_PRODUCER_EPOCH,
                   requestLocal: RequestLocal = RequestLocal.noCaching,
                   verificationGuard: Option[VerificationGuard]): Unit = {
    if (!group.hasReceivedConsistentOffsetCommits)
      warn(s"group: ${group.groupId} with leader: ${group.leaderOrNull} has received offset commits from consumers as well " +
        s"as transactional producers. Mixing both types of offset commits will generally result in surprises and " +
        s"should be avoided.")

    val filteredOffsetMetadata = offsetMetadata.filter { case (_, offsetAndMetadata) =>
      validateOffsetMetadataLength(offsetAndMetadata.metadata)
    }
    if (filteredOffsetMetadata.isEmpty) {
      // compute the final error codes for the commit response
      val commitStatus = offsetMetadata.map { case (k, _) => k -> Errors.OFFSET_METADATA_TOO_LARGE }
      responseCallback(commitStatus)
      return
    }

    val magicOpt = getMagic(partitionFor(group.groupId))
    if (magicOpt.isEmpty) {
      val commitStatus = offsetMetadata.map { case (topicIdPartition, _) =>
        (topicIdPartition, Errors.NOT_COORDINATOR)
      }
      responseCallback(commitStatus)
      return
    }

    val isTxnOffsetCommit = producerId != RecordBatch.NO_PRODUCER_ID
    val records = generateOffsetRecords(magicOpt.get, isTxnOffsetCommit, group.groupId, offsetTopicPartition, filteredOffsetMetadata, producerId, producerEpoch)
    val putCacheCallback = createPutCacheCallback(isTxnOffsetCommit, group, consumerId, offsetMetadata, filteredOffsetMetadata, responseCallback, producerId, records)

    val verificationGuards = verificationGuard.map(guard => offsetTopicPartition -> guard).toMap

    if (isTxnOffsetCommit) {
      addProducerGroup(producerId, group.groupId)
      group.prepareTxnOffsetCommit(producerId, filteredOffsetMetadata)
    } else {
      group.prepareOffsetCommit(filteredOffsetMetadata)
    }

    appendForGroup(group, records, requestLocal, putCacheCallback, verificationGuards)
  }

  /**
   * The most important guarantee that this API provides is that it should never return a stale offset. i.e., it either
   * returns the current offset or it begins to sync the cache from the log (and returns an error code).
   */
  def getOffsets(groupId: String, requireStable: Boolean, topicPartitionsOpt: Option[Seq[TopicPartition]]): Map[TopicPartition, PartitionData] = {
    trace("Getting offsets of %s for group %s.".format(topicPartitionsOpt.getOrElse("all partitions"), groupId))
    val group = groupMetadataCache.get(groupId)
    if (group == null) {
      topicPartitionsOpt.getOrElse(Seq.empty[TopicPartition]).map { topicPartition =>
        val partitionData = new PartitionData(OffsetFetchResponse.INVALID_OFFSET,
          Optional.empty(), "", Errors.NONE)
        topicPartition -> partitionData
      }.toMap
    } else {
      group.inLock {
        if (group.is(Dead)) {
          topicPartitionsOpt.getOrElse(Seq.empty[TopicPartition]).map { topicPartition =>
            val partitionData = new PartitionData(OffsetFetchResponse.INVALID_OFFSET,
              Optional.empty(), "", Errors.NONE)
            topicPartition -> partitionData
          }.toMap
        } else {
          val topicPartitions = topicPartitionsOpt.getOrElse(group.allOffsets.keySet)

          topicPartitions.map { topicPartition =>
            if (requireStable && group.hasPendingOffsetCommitsForTopicPartition(topicPartition)) {
              topicPartition -> new PartitionData(OffsetFetchResponse.INVALID_OFFSET,
                Optional.empty(), "", Errors.UNSTABLE_OFFSET_COMMIT)
            } else {
              val partitionData = group.offset(topicPartition) match {
                case None =>
                  new PartitionData(OffsetFetchResponse.INVALID_OFFSET,
                    Optional.empty(), "", Errors.NONE)
                case Some(offsetAndMetadata) =>
                  val leaderEpoch: Optional[Integer] = if (offsetAndMetadata.leaderEpoch.isPresent) {
                    Optional.of(offsetAndMetadata.leaderEpoch.getAsInt)
                  } else {
                    Optional.empty()
                  }
                  new PartitionData(offsetAndMetadata.committedOffset, leaderEpoch, offsetAndMetadata.metadata, Errors.NONE)
              }
              topicPartition -> partitionData
            }
          }.toMap
        }
      }
    }
  }

  /**
   * Asynchronously read the partition from the offsets topic and populate the cache
   */
  def scheduleLoadGroupAndOffsets(offsetsPartition: Int, coordinatorEpoch: Int, onGroupLoaded: GroupMetadata => Unit): Unit = {
    val topicPartition = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, offsetsPartition)
    info(s"Scheduling loading of offsets and group metadata from $topicPartition for epoch $coordinatorEpoch")
    val startTimeMs = time.milliseconds()
    scheduler.scheduleOnce(topicPartition.toString, () => loadGroupsAndOffsets(topicPartition, coordinatorEpoch, onGroupLoaded, startTimeMs))
  }

  private[group] def loadGroupsAndOffsets(
    topicPartition: TopicPartition,
    coordinatorEpoch: Int,
    onGroupLoaded: GroupMetadata => Unit,
    startTimeMs: java.lang.Long
  ): Unit = {
    if (!maybeUpdateCoordinatorEpoch(topicPartition.partition, OptionalInt.of(coordinatorEpoch))) {
      info(s"Not loading offsets and group metadata for $topicPartition " +
        s"in epoch $coordinatorEpoch since current epoch is ${epochForPartitionId.get(topicPartition.partition)}")
    } else if (!addLoadingPartition(topicPartition.partition)) {
      info(s"Already loading offsets and group metadata from $topicPartition")
    } else {
      try {
        val schedulerTimeMs = time.milliseconds() - startTimeMs
        debug(s"Started loading offsets and group metadata from $topicPartition for epoch $coordinatorEpoch")
        doLoadGroupsAndOffsets(topicPartition, onGroupLoaded)
        val endTimeMs = time.milliseconds()
        val totalLoadingTimeMs = endTimeMs - startTimeMs
        partitionLoadSensor.record(totalLoadingTimeMs.toDouble, endTimeMs, false)
        info(s"Finished loading offsets and group metadata from $topicPartition "
          + s"in $totalLoadingTimeMs milliseconds for epoch $coordinatorEpoch, of which " +
          s"$schedulerTimeMs milliseconds was spent in the scheduler.")
      } catch {
        case t: Throwable => error(s"Error loading offsets from $topicPartition", t)
      } finally {
        inLock(partitionLock) {
          ownedPartitions.add(topicPartition.partition)
          loadingPartitions.remove(topicPartition.partition)
        }
      }
    }
  }

  private def doLoadGroupsAndOffsets(topicPartition: TopicPartition, onGroupLoaded: GroupMetadata => Unit): Unit = {
    def logEndOffset: Long = replicaManager.getLogEndOffset(topicPartition).getOrElse(-1L)

    replicaManager.getLog(topicPartition) match {
      case None =>
        warn(s"Attempted to load offsets and group metadata from $topicPartition, but found no log")

      case Some(log) =>
        val loadedOffsets = mutable.Map[GroupTopicPartition, CommitRecordMetadataAndOffset]()
        val pendingOffsets = mutable.Map[Long, mutable.Map[GroupTopicPartition, CommitRecordMetadataAndOffset]]()
        val loadedGroups = mutable.Map[String, GroupMetadata]()
        val removedGroups = mutable.Set[String]()

        // buffer may not be needed if records are read from memory
        var buffer = ByteBuffer.allocate(0)

        // loop breaks if leader changes at any time during the load, since logEndOffset is -1
        var currOffset = log.logStartOffset

        // loop breaks if no records have been read, since the end of the log has been reached
        var readAtLeastOneRecord = true

        while (currOffset < logEndOffset && readAtLeastOneRecord && !shuttingDown.get()) {
          val fetchDataInfo = log.read(currOffset,
            maxLength = config.loadBufferSize,
            isolation = FetchIsolation.LOG_END,
            minOneMessage = true)

          readAtLeastOneRecord = fetchDataInfo.records.sizeInBytes > 0

          val memRecords = (fetchDataInfo.records: @unchecked) match {
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

          memRecords.batches.forEach { batch =>
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

                  case unknownKey: UnknownKey =>
                    warn(s"Unknown message key with version ${unknownKey.version}" +
                      s" while loading offsets and group metadata from $topicPartition. Ignoring it. " +
                      "It could be a left over from an aborted upgrade.")
                }
              }
            }
            currOffset = batch.nextOffset
          }
        }

        val (groupOffsets, emptyGroupOffsets) = loadedOffsets
          .groupBy(_._1.group)
          .map { case (k, v) =>
            k -> v.map { case (groupTopicPartition, offset) => (groupTopicPartition.topicPartition, offset) }
          }.partition { case (group, _) => loadedGroups.contains(group) }

        val pendingOffsetsByGroup = mutable.Map[String, mutable.Map[Long, mutable.Map[TopicPartition, CommitRecordMetadataAndOffset]]]()
        pendingOffsets.foreachEntry { (producerId, producerOffsets) =>
          producerOffsets.keySet.map(_.group).foreach(addProducerGroup(producerId, _))
          producerOffsets
            .groupBy(_._1.group)
            .foreachEntry { (group, offsets) =>
              val groupPendingOffsets = pendingOffsetsByGroup.getOrElseUpdate(group, mutable.Map.empty[Long, mutable.Map[TopicPartition, CommitRecordMetadataAndOffset]])
              val groupProducerOffsets = groupPendingOffsets.getOrElseUpdate(producerId, mutable.Map.empty[TopicPartition, CommitRecordMetadataAndOffset])
              groupProducerOffsets ++= offsets.map { case (groupTopicPartition, offset) =>
                (groupTopicPartition.topicPartition, offset)
              }
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
                               coordinatorEpoch: OptionalInt,
                               onGroupUnloaded: GroupMetadata => Unit): Unit = {
    val topicPartition = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, offsetsPartition)
    info(s"Scheduling unloading of offsets and group metadata from $topicPartition")
    scheduler.scheduleOnce(topicPartition.toString, () => removeGroupsAndOffsets(topicPartition, coordinatorEpoch, onGroupUnloaded))
  }

  private [group] def removeGroupsAndOffsets(topicPartition: TopicPartition,
                                             coordinatorEpoch: OptionalInt,
                                             onGroupUnloaded: GroupMetadata => Unit): Unit = {
    val offsetsPartition = topicPartition.partition
    if (maybeUpdateCoordinatorEpoch(offsetsPartition, coordinatorEpoch)) {
      var numOffsetsRemoved = 0
      var numGroupsRemoved = 0

      debug(s"Started unloading offsets and group metadata for $topicPartition for " +
        s"coordinator epoch $coordinatorEpoch")
      inLock(partitionLock) {
        // we need to guard the group removal in cache in the loading partition lock
        // to prevent coordinator's check-and-get-group race condition
        ownedPartitions.remove(offsetsPartition)
        loadingPartitions.remove(offsetsPartition)

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
      info(s"Finished unloading $topicPartition for coordinator epoch $coordinatorEpoch. " +
        s"Removed $numOffsetsRemoved cached offsets and $numGroupsRemoved cached groups.")
    } else {
      info(s"Not removing offsets and group metadata for $topicPartition " +
        s"in epoch $coordinatorEpoch since current epoch is ${epochForPartitionId.get(topicPartition.partition)}")
    }
  }

  /**
   * Update the cached coordinator epoch if the new value is larger than the old value.
   * @return true if `epochOpt` is either empty or contains a value greater than or equal to the current epoch
   */
  private def maybeUpdateCoordinatorEpoch(
    partitionId: Int,
    epochOpt: OptionalInt
  ): Boolean = {
    val updatedEpoch = epochForPartitionId.compute(partitionId, (_, currentEpoch) => {
      if (currentEpoch == null) {
        if (epochOpt.isPresent) epochOpt.getAsInt
        else null
      } else {
        if (epochOpt.isPresent && epochOpt.getAsInt > currentEpoch) epochOpt.getAsInt
        else currentEpoch
      }
    })
    if (epochOpt.isPresent) {
      epochOpt.getAsInt == updatedEpoch
    } else {
      true
    }
  }

  // visible for testing
  private[group] def cleanupGroupMetadata(): Unit = {
    val currentTimestamp = time.milliseconds()
    val numOffsetsRemoved = cleanupGroupMetadata(groupMetadataCache.values, RequestLocal.noCaching,
      _.removeExpiredOffsets(currentTimestamp, config.offsetsRetentionMs))
    offsetExpiredSensor.record(numOffsetsRemoved)
    if (numOffsetsRemoved > 0)
      info(s"Removed $numOffsetsRemoved expired offsets in ${time.milliseconds() - currentTimestamp} milliseconds.")
  }

  /**
    * This function is used to clean up group offsets given the groups and also a function that performs the offset deletion.
    * @param groups Groups whose metadata are to be cleaned up
    * @param selector A function that implements deletion of (all or part of) group offsets. This function is called while
    *                 a group lock is held, therefore there is no need for the caller to also obtain a group lock.
    * @return The cumulative number of offsets removed
    */
  def cleanupGroupMetadata(groups: Iterable[GroupMetadata], requestLocal: RequestLocal,
                           selector: GroupMetadata => Map[TopicPartition, OffsetAndMetadata]): Int = {
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
        val timestamp = time.milliseconds()

          replicaManager.onlinePartition(appendPartition).foreach { partition =>
            val tombstones = ArrayBuffer.empty[SimpleRecord]
            removedOffsets.foreachEntry { (topicPartition, offsetAndMetadata) =>
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
                val records = MemoryRecords.withRecords(magicValue, 0L, compression, timestampType, tombstones.toArray: _*)
                partition.appendRecordsToLeader(records, origin = AppendOrigin.COORDINATOR, requiredAcks = 0,
                  requestLocal = requestLocal)

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
  def scheduleHandleTxnCompletion(producerId: Long, completedPartitions: Set[Int], isCommit: Boolean): CompletableFuture[Void] = {
    val future = new CompletableFuture[Void]()
    scheduler.scheduleOnce(s"handleTxnCompletion-$producerId", () => {
      try {
        handleTxnCompletion(producerId, completedPartitions, isCommit)
        future.complete(null)
      } catch {
        case e: Throwable => future.completeExceptionally(e)
      }
    })
    future
  }

  private[group] def handleTxnCompletion(producerId: Long, completedPartitions: Set[Int], isCommit: Boolean): Unit = {
    val pendingGroups = groupsBelongingToPartitions(producerId, completedPartitions)
    pendingGroups.foreach { groupId =>
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
      .partition(group => partitions.contains(partitionFor(group)))
    ownedGroups
  }

  private def removeGroupFromAllProducers(groupId: String): Unit = openGroupsForProducer synchronized {
    openGroupsForProducer.foreachEntry { (_, groups) =>
      groups.remove(groupId)
    }
  }

  /*
   * Check if the offset metadata length is valid
   */
  private def validateOffsetMetadataLength(metadata: String) : Boolean = {
    metadata == null || metadata.length() <= config.maxMetadataSize
  }


  def shutdown(): Unit = {
    shuttingDown.set(true)
    scheduler.shutdown()
    metrics.removeSensor(GroupMetadataManager.LoadTimeSensor)
    metrics.removeSensor(GroupMetadataManager.OffsetCommitsSensor)
    metrics.removeSensor(GroupMetadataManager.OffsetExpiredSensor)

    // TODO: clear the caches
  }

  /**
   * Check if the replica is local and return the message format version
   *
   * @param   partition  Partition of GroupMetadataTopic
   * @return  Some(MessageFormatVersion) if replica is local, None otherwise
   */
  private def getMagic(partition: Int): Option[Byte] =
    replicaManager.getMagic(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, partition))

  /**
   * Add a partition to the owned partition set.
   *
   * NOTE: this is for test only.
   */
  private[group] def addOwnedPartition(partition: Int): Unit = {
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
      if (ownedPartitions.contains(partition)) {
        false
      } else {
        loadingPartitions.add(partition)
      }
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
  // Metrics names
  val MetricsGroup: String = "group-coordinator-metrics"
  val LoadTimeSensor: String = "GroupPartitionLoadTime"
  val OffsetCommitsSensor: String = "OffsetCommits"
  val OffsetExpiredSensor: String = "OffsetExpired"

  /**
   * Generates the key for offset commit message for given (group, topic, partition)
   *
   * @param groupId the ID of the group to generate the key
   * @param topicPartition the TopicPartition to generate the key
   * @return key for offset commit message
   */
  def offsetCommitKey(groupId: String, topicPartition: TopicPartition): Array[Byte] = {
    MessageUtil.toCoordinatorTypePrefixedBytes(CoordinatorRecordType.OFFSET_COMMIT.id(),
      new OffsetCommitKey()
        .setGroup(groupId)
        .setTopic(topicPartition.topic)
        .setPartition(topicPartition.partition))
  }

  /**
   * Generates the key for group metadata message for given group
   *
   * @param groupId the ID of the group to generate the key
   * @return key bytes for group metadata message
   */
  def groupMetadataKey(groupId: String): Array[Byte] = {
    MessageUtil.toCoordinatorTypePrefixedBytes(CoordinatorRecordType.GROUP_METADATA.id(),
      new GroupMetadataKeyData()
        .setGroup(groupId))
  }

  /**
   * Generates the payload for offset commit message from given offset and metadata
   *
   * @param offsetAndMetadata consumer's current offset and metadata
   * @param metadataVersion the api version
   * @return payload for offset commit message
   */
  def offsetCommitValue(offsetAndMetadata: OffsetAndMetadata,
                        metadataVersion: MetadataVersion): Array[Byte] = {
    val version =
      if (metadataVersion.isLessThan(IBP_2_1_IV0) || offsetAndMetadata.expireTimestampMs.isPresent) 1.toShort
      else if (metadataVersion.isLessThan(IBP_2_1_IV1)) 2.toShort
      // Serialize with the highest supported non-flexible version
      // until a tagged field is introduced or the version is bumped.
      else 3.toShort
    MessageUtil.toVersionPrefixedBytes(version, new OffsetCommitValue()
      .setOffset(offsetAndMetadata.committedOffset)
      .setMetadata(offsetAndMetadata.metadata)
      .setCommitTimestamp(offsetAndMetadata.commitTimestampMs)
      .setLeaderEpoch(offsetAndMetadata.leaderEpoch.orElse(RecordBatch.NO_PARTITION_LEADER_EPOCH))
      // version 1 has a non empty expireTimestamp field
      .setExpireTimestamp(offsetAndMetadata.expireTimestampMs.orElse(OffsetCommitRequest.DEFAULT_TIMESTAMP))
    )
  }

  /**
   * Generates the payload for group metadata message from given offset and metadata
   * assuming the generation id, selected protocol, leader and member assignment are all available
   *
   * @param groupMetadata current group metadata
   * @param assignment the assignment for the rebalancing generation
   * @param metadataVersion the api version
   * @return payload for offset commit message
   */
  def groupMetadataValue(groupMetadata: GroupMetadata,
                         assignment: Map[String, Array[Byte]],
                         metadataVersion: MetadataVersion): Array[Byte] = {

    val version =
      if (metadataVersion.isLessThan(IBP_0_10_1_IV0)) 0.toShort
      else if (metadataVersion.isLessThan(IBP_2_1_IV0)) 1.toShort
      else if (metadataVersion.isLessThan(IBP_2_3_IV0)) 2.toShort
      // Serialize with the highest supported non-flexible version
      // until a tagged field is introduced or the version is bumped.
      else 3.toShort

    MessageUtil.toVersionPrefixedBytes(version, new GroupMetadataValue()
      .setProtocolType(groupMetadata.protocolType.getOrElse(""))
      .setGeneration(groupMetadata.generationId)
      .setProtocol(groupMetadata.protocolName.orNull)
      .setLeader(groupMetadata.leaderOrNull)
      .setCurrentStateTimestamp(groupMetadata.currentStateTimestampOrDefault)
      .setMembers(groupMetadata.allMemberMetadata.map { memberMetadata =>
        new GroupMetadataValue.MemberMetadata()
          .setMemberId(memberMetadata.memberId)
          .setClientId(memberMetadata.clientId)
          .setClientHost(memberMetadata.clientHost)
          .setSessionTimeout(memberMetadata.sessionTimeoutMs)
          .setRebalanceTimeout(memberMetadata.rebalanceTimeoutMs)
          .setGroupInstanceId(memberMetadata.groupInstanceId.orNull)
          // The group is non-empty, so the current protocol must be defined
          .setSubscription(groupMetadata.protocolName.map(memberMetadata.metadata)
            .getOrElse(throw new IllegalStateException("Attempted to write non-empty group metadata with no defined protocol.")))
          .setAssignment(assignment.getOrElse(memberMetadata.memberId,
            throw new IllegalStateException(s"Attempted to write member ${memberMetadata.memberId} of group ${groupMetadata.groupId} with no assignment.")))
      }.asJava))
  }

  /**
   * Decodes the offset messages' key
   *
   * @param buffer input byte-buffer
   * @return an OffsetKey or GroupMetadataKey object from the message
   */
  def readMessageKey(buffer: ByteBuffer): BaseKey = {
    val version = buffer.getShort
    try {
      CoordinatorRecordType.fromId(version) match {
        case CoordinatorRecordType.LEGACY_OFFSET_COMMIT =>
          // version 0 refers to the legacy offset commit.
          val key = new LegacyOffsetCommitKey(new ByteBufferAccessor(buffer), 0.toShort)
          OffsetKey(version, GroupTopicPartition(key.group, new TopicPartition(key.topic, key.partition)))

        case CoordinatorRecordType.OFFSET_COMMIT =>
          // version 1 refers to offset commit.
          val key = new OffsetCommitKey(new ByteBufferAccessor(buffer), 0.toShort)
          OffsetKey(version, GroupTopicPartition(key.group, new TopicPartition(key.topic, key.partition)))

        case CoordinatorRecordType.GROUP_METADATA =>
          // version 2 refers to group metadata.
          val key = new GroupMetadataKeyData(new ByteBufferAccessor(buffer), 0.toShort)
          GroupMetadataKey(version, key.group)

        case _ =>
          UnknownKey(version)
      }
    } catch {
      case _: UnsupportedVersionException => UnknownKey(version)
    }
  }

  /**
   * Decodes the offset messages' payload and retrieves offset and metadata from it
   *
   * @param buffer input byte-buffer
   * @return an offset-metadata object from the message
   */
  def readOffsetMessageValue(buffer: ByteBuffer): OffsetAndMetadata = {
    // tombstone
    if (buffer == null) null
    else {
      val version = buffer.getShort
      if (version >= OffsetCommitValue.LOWEST_SUPPORTED_VERSION && version <= OffsetCommitValue.HIGHEST_SUPPORTED_VERSION) {
        val value = new OffsetCommitValue(new ByteBufferAccessor(buffer), version)
        new OffsetAndMetadata(
          value.offset,
          if (value.leaderEpoch == RecordBatch.NO_PARTITION_LEADER_EPOCH) OptionalInt.empty() else OptionalInt.of(value.leaderEpoch),
          value.metadata,
          value.commitTimestamp,
          if (value.expireTimestamp == OffsetCommitRequest.DEFAULT_TIMESTAMP) OptionalLong.empty() else OptionalLong.of(value.expireTimestamp))
      } else throw new IllegalStateException(s"Unknown offset message version: $version")
    }
  }

  /**
   * Decodes the group metadata messages' payload and retrieves its member metadata from it
   *
   * @param groupId The ID of the group to be read
   * @param buffer input byte-buffer
   * @param time the time instance to use
   * @return a group metadata object from the message
   */
  def readGroupMessageValue(groupId: String, buffer: ByteBuffer, time: Time): GroupMetadata = {
    // tombstone
    if (buffer == null) null
    else {
      val version = buffer.getShort
      if (version >= GroupMetadataValue.LOWEST_SUPPORTED_VERSION && version <= GroupMetadataValue.HIGHEST_SUPPORTED_VERSION) {
        val value = new GroupMetadataValue(new ByteBufferAccessor(buffer), version)
        val members = value.members.asScala.map { memberMetadata =>
          new MemberMetadata(
            memberId = memberMetadata.memberId,
            groupInstanceId = Option(memberMetadata.groupInstanceId),
            clientId = memberMetadata.clientId,
            clientHost = memberMetadata.clientHost,
            rebalanceTimeoutMs = if (version == 0) memberMetadata.sessionTimeout else memberMetadata.rebalanceTimeout,
            sessionTimeoutMs = memberMetadata.sessionTimeout,
            protocolType = value.protocolType,
            supportedProtocols = List((value.protocol, memberMetadata.subscription)),
            assignment = memberMetadata.assignment)
        }
        GroupMetadata.loadGroup(
          groupId = groupId,
          initialState = if (members.isEmpty) Empty else Stable,
          generationId = value.generation,
          protocolType = value.protocolType,
          protocolName = value.protocol,
          leaderId = value.leader,
          currentStateTimestamp = if (value.currentStateTimestamp == -1) None else Some(value.currentStateTimestamp),
          members = members,
          time = time)
      } else throw new IllegalStateException(s"Unknown group metadata message version: $version")
    }
  }

  def maybeConvertOffsetCommitError(error: Errors) : Errors = {
    error match {
      case Errors.NETWORK_EXCEPTION =>
        // When committing offsets transactionally, we now verify the transaction with the
        // transaction coordinator. Verification can fail with `NETWORK_EXCEPTION`, a retriable
        // error which older clients may not expect and retry correctly. We translate the error to
        // `COORDINATOR_LOAD_IN_PROGRESS` because it causes clients to retry the request without an
        // unnecessary coordinator lookup.
        Errors.COORDINATOR_LOAD_IN_PROGRESS

      case Errors.UNKNOWN_TOPIC_OR_PARTITION
           | Errors.NOT_ENOUGH_REPLICAS
           | Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND =>
        Errors.COORDINATOR_NOT_AVAILABLE

      case Errors.NOT_LEADER_OR_FOLLOWER
           | Errors.KAFKA_STORAGE_ERROR =>
        Errors.NOT_COORDINATOR

      case Errors.MESSAGE_TOO_LARGE
           | Errors.RECORD_LIST_TOO_LARGE
           | Errors.INVALID_FETCH_SIZE =>
        Errors.INVALID_COMMIT_OFFSET_SIZE

      // We may see INVALID_TXN_STATE or INVALID_PID_MAPPING here due to transaction verification.
      // They can be returned without mapping to a new error.
      case other => other
    }
  }

}

case class GroupTopicPartition(group: String, topicPartition: TopicPartition) {

  def this(group: String, topic: String, partition: Int) =
    this(group, new TopicPartition(topic, partition))

  override def toString: String =
    "[%s,%s,%d]".format(group, topicPartition.topic, topicPartition.partition)
}

sealed trait BaseKey{
  def version: Short
  def key: Any
}

case class OffsetKey(version: Short, key: GroupTopicPartition) extends BaseKey {
  override def toString: String = key.toString
}

case class GroupMetadataKey(version: Short, key: String) extends BaseKey {
  override def toString: String = key
}

case class UnknownKey(version: Short) extends BaseKey {
  override def key: String = null
  override def toString: String = key
}
