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
package kafka.server.metadata

import java.util
import java.util.concurrent.TimeUnit
import kafka.coordinator.group.GroupCoordinator
import kafka.coordinator.transaction.TransactionCoordinator
import kafka.metrics.KafkaMetricsGroup
import kafka.server.{RaftReplicaManager, RequestHandlerHelper}
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.metadata.MetadataRecordType._
import org.apache.kafka.common.metadata._
import org.apache.kafka.common.protocol.ApiMessage
import org.apache.kafka.common.utils.{LogContext, Time}
import org.apache.kafka.metalog.{MetaLogLeader, MetaLogListener}
import org.apache.kafka.queue.{EventQueue, KafkaEventQueue}

import scala.jdk.CollectionConverters._

object BrokerMetadataListener{
  val MetadataBatchProcessingTimeUs = "MetadataBatchProcessingTimeUs"
  val MetadataBatchSizes = "MetadataBatchSizes"
}

class BrokerMetadataListener(brokerId: Int,
                             time: Time,
                             metadataCache: RaftMetadataCache,
                             configRepository: CachedConfigRepository,
                             groupCoordinator: GroupCoordinator,
                             replicaManager: RaftReplicaManager,
                             txnCoordinator: TransactionCoordinator,
                             threadNamePrefix: Option[String],
                             clientQuotaManager: ClientQuotaMetadataManager
                            ) extends MetaLogListener with KafkaMetricsGroup {
  private val logContext = new LogContext(s"[BrokerMetadataListener id=${brokerId}] ")
  private val log = logContext.logger(classOf[BrokerMetadataListener])
  logIdent = logContext.logPrefix()

  /**
   * A histogram tracking the time in microseconds it took to process batches of events.
   */
  private val batchProcessingTimeHist = newHistogram(BrokerMetadataListener.MetadataBatchProcessingTimeUs)

  /**
   * A histogram tracking the sizes of batches that we have processed.
   */
  private val metadataBatchSizeHist = newHistogram(BrokerMetadataListener.MetadataBatchSizes)

  /**
   * The highest metadata offset that we've seen.  Written only from the event queue thread.
   */
  @volatile private var _highestMetadataOffset = -1L

  val eventQueue = new KafkaEventQueue(time, logContext, threadNamePrefix.getOrElse(""))

  def highestMetadataOffset(): Long = _highestMetadataOffset

  /**
   * Handle new metadata records.
   */
  override def handleCommits(lastOffset: Long, records: util.List[ApiMessage]): Unit = {
    eventQueue.append(new HandleCommitsEvent(lastOffset, records))
  }

  // Visible for testing. It's useful to execute events synchronously
  private[metadata] def execCommits(lastOffset: Long, records: util.List[ApiMessage]): Unit = {
    new HandleCommitsEvent(lastOffset, records).run()
  }

  class HandleCommitsEvent(lastOffset: Long,
                           records: util.List[ApiMessage])
      extends EventQueue.FailureLoggingEvent(log) {
    override def run(): Unit = {
      if (isDebugEnabled) {
        debug(s"Metadata batch ${lastOffset}: handling ${records.size()} record(s).")
      }
      val imageBuilder =
        MetadataImageBuilder(brokerId, log, metadataCache.currentImage())
      val startNs = time.nanoseconds()
      var index = 0
      metadataBatchSizeHist.update(records.size())
      records.iterator().asScala.foreach { record =>
        try {
          if (isTraceEnabled) {
            trace("Metadata batch %d: processing [%d/%d]: %s.".format(lastOffset, index + 1,
              records.size(), record.toString))
          }
          handleMessage(imageBuilder, record, lastOffset)
        } catch {
          case e: Exception => error(s"Unable to handle record ${index} in batch " +
            s"ending at offset ${lastOffset}", e)
        }
        index = index + 1
      }
      if (imageBuilder.hasChanges) {
        val newImage = imageBuilder.build()
        if (isTraceEnabled) {
          trace(s"Metadata batch ${lastOffset}: creating new metadata image ${newImage}")
        } else if (isDebugEnabled) {
          debug(s"Metadata batch ${lastOffset}: creating new metadata image")
        }
        metadataCache.image(newImage)
      } else if (isDebugEnabled) {
        debug(s"Metadata batch ${lastOffset}: no new metadata image required.")
      }
      if (imageBuilder.hasPartitionChanges) {
        if (isDebugEnabled) {
          debug(s"Metadata batch ${lastOffset}: applying partition changes")
        }
        replicaManager.handleMetadataRecords(imageBuilder, lastOffset,
          RequestHandlerHelper.onLeadershipChange(groupCoordinator, txnCoordinator, _, _))
      } else if (isDebugEnabled) {
        debug(s"Metadata batch ${lastOffset}: no partition changes found.")
      }
      _highestMetadataOffset = lastOffset
      val endNs = time.nanoseconds()
      val deltaUs = TimeUnit.MICROSECONDS.convert(endNs - startNs, TimeUnit.NANOSECONDS)
      debug(s"Metadata batch ${lastOffset}: advanced highest metadata offset in ${deltaUs} " +
        "microseconds.")
      batchProcessingTimeHist.update(deltaUs)
    }
  }

  private def handleMessage(imageBuilder: MetadataImageBuilder,
                            record: ApiMessage,
                            lastOffset: Long): Unit = {
    val recordType = try {
      fromId(record.apiKey())
    } catch {
      case e: Exception => throw new RuntimeException("Unknown metadata record type " +
      s"${record.apiKey()} in batch ending at offset ${lastOffset}.")
    }

    record match {
      case rec: RegisterBrokerRecord => handleRegisterBrokerRecord(imageBuilder, rec)
      case rec: UnregisterBrokerRecord => handleUnregisterBrokerRecord(imageBuilder, rec)
      case rec: FenceBrokerRecord => handleFenceBrokerRecord(imageBuilder, rec)
      case rec: UnfenceBrokerRecord => handleUnfenceBrokerRecord(imageBuilder, rec)
      case rec: TopicRecord => handleTopicRecord(imageBuilder, rec)
      case rec: PartitionRecord => handlePartitionRecord(imageBuilder, rec)
      case rec: PartitionChangeRecord => handlePartitionChangeRecord(imageBuilder, rec)
      case rec: RemoveTopicRecord => handleRemoveTopicRecord(imageBuilder, rec)
      case rec: ConfigRecord => handleConfigRecord(rec)
      case rec: QuotaRecord => handleQuotaRecord(imageBuilder, rec)
      case _ => throw new RuntimeException(s"Unhandled record $record with type $recordType")
    }
  }

  def handleRegisterBrokerRecord(imageBuilder: MetadataImageBuilder,
                                 record: RegisterBrokerRecord): Unit = {
    val broker = MetadataBroker(record)
    imageBuilder.brokersBuilder().add(broker)
  }

  def handleUnregisterBrokerRecord(imageBuilder: MetadataImageBuilder,
                                   record: UnregisterBrokerRecord): Unit = {
    imageBuilder.brokersBuilder().remove(record.brokerId())
  }

  def handleTopicRecord(imageBuilder: MetadataImageBuilder,
                        record: TopicRecord): Unit = {
    imageBuilder.partitionsBuilder().addUuidMapping(record.name(), record.topicId())
  }

  def handlePartitionRecord(imageBuilder: MetadataImageBuilder,
                            record: PartitionRecord): Unit = {
    imageBuilder.topicIdToName(record.topicId()) match {
      case None => throw new RuntimeException(s"Unable to locate topic with ID ${record.topicId}")
      case Some(name) =>
        val partition = MetadataPartition(name, record)
        imageBuilder.partitionsBuilder().set(partition)
    }
  }

  def handleConfigRecord(record: ConfigRecord): Unit = {
    val t = ConfigResource.Type.forId(record.resourceType())
    if (t == ConfigResource.Type.UNKNOWN) {
      throw new RuntimeException("Unable to understand config resource type " +
        s"${Integer.valueOf(record.resourceType())}")
    }
    val resource = new ConfigResource(t, record.resourceName())
    configRepository.setConfig(resource, record.name(), record.value())
  }

  def handlePartitionChangeRecord(imageBuilder: MetadataImageBuilder,
                                  record: PartitionChangeRecord): Unit = {
    imageBuilder.partitionsBuilder().handleChange(record)
  }

  def handleFenceBrokerRecord(imageBuilder: MetadataImageBuilder,
                              record: FenceBrokerRecord): Unit = {
    // TODO: add broker epoch to metadata cache, and check it here.
    imageBuilder.brokersBuilder().changeFencing(record.id(), fenced = true)
  }

  def handleUnfenceBrokerRecord(imageBuilder: MetadataImageBuilder,
                                record: UnfenceBrokerRecord): Unit = {
    // TODO: add broker epoch to metadata cache, and check it here.
    imageBuilder.brokersBuilder().changeFencing(record.id(), fenced = false)
  }

  def handleRemoveTopicRecord(imageBuilder: MetadataImageBuilder,
                              record: RemoveTopicRecord): Unit = {
    imageBuilder.topicIdToName(record.topicId()) match {
      case None =>
        throw new RuntimeException(s"Unable to locate topic with ID ${record.topicId}")

      case Some(topicName) =>
        info(s"Processing deletion of topic $topicName with id ${record.topicId}")
        val removedPartitions = imageBuilder.partitionsBuilder().removeTopicById(record.topicId())
        groupCoordinator.handleDeletedPartitions(removedPartitions.map(_.toTopicPartition).toSeq)
        configRepository.remove(new ConfigResource(ConfigResource.Type.TOPIC, topicName))
    }
  }

  def handleQuotaRecord(imageBuilder: MetadataImageBuilder,
                        record: QuotaRecord): Unit = {
    // TODO add quotas to MetadataImageBuilder
    clientQuotaManager.handleQuotaRecord(record)
  }

  class HandleNewLeaderEvent(leader: MetaLogLeader)
      extends EventQueue.FailureLoggingEvent(log) {
    override def run(): Unit = {
      val imageBuilder =
        MetadataImageBuilder(brokerId, log, metadataCache.currentImage())
      if (leader.nodeId() < 0) {
        imageBuilder.controllerId(None)
      } else {
        imageBuilder.controllerId(Some(leader.nodeId()))
      }
      metadataCache.image(imageBuilder.build())
    }
  }

  override def handleNewLeader(leader: MetaLogLeader): Unit = {
    eventQueue.append(new HandleNewLeaderEvent(leader))
  }

  class ShutdownEvent() extends EventQueue.FailureLoggingEvent(log) {
    override def run(): Unit = {
      removeMetric(BrokerMetadataListener.MetadataBatchProcessingTimeUs)
      removeMetric(BrokerMetadataListener.MetadataBatchSizes)
    }
  }

  override def beginShutdown(): Unit = {
    eventQueue.beginShutdown("beginShutdown", new ShutdownEvent())
  }

  def close(): Unit = {
    beginShutdown()
    eventQueue.close()
  }
}
