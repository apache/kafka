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

package kafka.api

import kafka.server.KafkaConfig
import kafka.utils.BrokerLocalStorage
import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.metadata.BrokerState
import org.apache.kafka.server.log.remote.metadata.storage.{TopicBasedRemoteLogMetadataManager, TopicBasedRemoteLogMetadataManagerConfig}
import org.apache.kafka.server.log.remote.storage.LocalTieredStorage.{STORAGE_CONFIG_PREFIX, STORAGE_DIR_PROP}
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig.{REMOTE_LOG_METADATA_MANAGER_CONFIG_PREFIX_PROP, REMOTE_STORAGE_MANAGER_CONFIG_PREFIX_PROP}
import org.apache.kafka.server.log.remote.storage.{LocalTieredStorage, RemoteLogManagerConfig}

import java.util.Properties
import scala.collection.Seq

class TransactionsTestWithTieredStore extends TransactionsTest {

  override val numServers = 3

  /**
   * InitialTaskDelayMs is set to 30 seconds for the delete-segment scheduler in Apache Kafka.
   * Hence, we need to wait at least that amount of time before segments eligible for deletion
   * gets physically removed.
   */
  private val storageWaitTimeoutSec = 35

  def storageConfigPrefix(key: String = ""): String = {
    STORAGE_CONFIG_PREFIX + key
  }

  def metadataConfigPrefix(key: String = ""): String = {
    "rlmm.config." + key
  }

  override def generateConfigs: Seq[KafkaConfig] = {
    val overridingProps = serverProps()
    //
    // The directory of the second-tier storage needs to be constant across all instances of storage managers
    // in every broker and throughout the test. Indeed, as brokers are restarted during the test.
    //
    // You can override this property with a fixed path of your choice if you wish to use a non-temporary
    // directory to access its content after a test terminated.
    //
    overridingProps.setProperty(REMOTE_STORAGE_MANAGER_CONFIG_PREFIX_PROP, storageConfigPrefix())
    overridingProps.setProperty(storageConfigPrefix(STORAGE_DIR_PROP), TestUtils.tempDir().getAbsolutePath)
    TestUtils.createBrokerConfigs(numServers, zkConnect).map(KafkaConfig.fromProps(_, overridingProps))
  }

  override def serverProps(): Properties = {
    val overridingProps = super.serverProps()
    //
    // Configure the tiered storage in Kafka. Set an interval of 100 ms for the remote log manager background
    // activity to ensure the tiered storage has enough room to be exercised within the lifetime of a test.
    //
    // The replication factor of the remote log metadata topic needs to be chosen so that in resiliency
    // tests, metadata can survive the loss of one replica for its topic-partitions.
    //
    // The second-tier storage system is mocked via the LocalTieredStorage instance which persists transferred
    // data files on the local file system.
    //
    overridingProps.put(RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, true.toString)
    overridingProps.setProperty(RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CLASS_NAME_PROP, classOf[LocalTieredStorage].getName)
    overridingProps.setProperty(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP, classOf[TopicBasedRemoteLogMetadataManager].getName)
    overridingProps.setProperty(RemoteLogManagerConfig.REMOTE_LOG_MANAGER_TASK_INTERVAL_MS_PROP, 100.toString)

    overridingProps.setProperty(REMOTE_LOG_METADATA_MANAGER_CONFIG_PREFIX_PROP, metadataConfigPrefix())
    overridingProps.setProperty(
      metadataConfigPrefix(TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_TOPIC_PARTITIONS_PROP), 3.toString)
    overridingProps.setProperty(
      metadataConfigPrefix(TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_TOPIC_REPLICATION_FACTOR_PROP), 2.toString)

    //
    // This configuration ensures inactive log segments are deleted fast enough so that
    // the integration tests can confirm a given log segment is present only in the second-tier storage.
    // Note that this does not impact the eligibility of a log segment to be offloaded to the
    // second-tier storage.
    //
    overridingProps.setProperty(KafkaConfig.LogCleanupIntervalMsProp, 100.toString)
    overridingProps.setProperty(LocalTieredStorage.DELETE_ON_CLOSE_PROP, true.toString)
    overridingProps
  }

  override def topicConfig(): Properties = {
    val overridingProps = super.topicConfig()
    //
    // Enables remote log storage for this topic.
    //
    overridingProps.put(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, true.toString)
    //
    // Ensure offset and time indexes are generated for every record.
    //
    overridingProps.put(TopicConfig.INDEX_INTERVAL_BYTES_CONFIG, 1.toString)
    //
    // Leverage the use of the segment index size to create a log-segment accepting one and only one record.
    // The minimum size of the indexes is that of an entry, which is 8 for the offset index and 12 for the
    // time index. Hence, since the topic is configured to generate index entries for every record with, for
    // a "small" number of records (i.e. such that the average record size times the number of records is
    // much less than the segment size), the number of records which hold in a segment is the multiple of 12
    // defined below.
    //
    overridingProps.put(TopicConfig.SEGMENT_INDEX_BYTES_CONFIG, (12 * 1).toString)
    //
    // To verify records physically absent from Kafka's storage can be consumed via the second tier storage, we
    // want to delete log segments as soon as possible. When tiered storage is active, an inactive log
    // segment is not eligible for deletion until it has been offloaded, which guarantees all segments
    // should be offloaded before deletion, and their consumption is possible thereafter.
    //
    overridingProps.put(TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG, 1.toString)
    overridingProps
  }

  override def maybeWaitForAtLeastOneSegmentUpload(topicPartitions: TopicPartition*): Unit = {
    topicPartitions.foreach(topicPartition => {
      val localStorages = servers.map(s => new BrokerLocalStorage(s.config.brokerId, s.config.logDirs.head,
        storageWaitTimeoutSec))
      localStorages
        //
        // Select brokers which are assigned a replica of the topic-partition
        //
        .filter(s => isAssignedReplica(topicPartition, s.brokerId))
        //
        // Filter out inactive brokers, which may still contain log segments we would expect
        // to be deleted based on the retention configuration.
        //
        .filter(s => isActive(s.brokerId))
        //
        // Wait until the brokers local storage have been cleared from the inactive log segments.
        //
        .foreach(_.waitForAtLeastEarliestOffset(topicPartition, 1L))
    })
  }

  private def isAssignedReplica(topicPartition: TopicPartition, replicaId: Int): Boolean = {
    val assignments = zkClient.getPartitionAssignmentForTopics(Set(topicPartition.topic()))
    assignments(topicPartition.topic())(topicPartition.partition()).replicas.contains(replicaId)
  }

  private def isActive(brokerId: Int): Boolean = {
    servers(brokerId).brokerState equals BrokerState.RUNNING
  }
}
