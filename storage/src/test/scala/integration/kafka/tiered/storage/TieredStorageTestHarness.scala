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

package kafka.tiered.storage

import java.util.Properties
import kafka.api.IntegrationTestHarness
import kafka.log.remote.ClassLoaderAwareRemoteStorageManager
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.BrokerLocalStorage
import kafka.utils.TestUtils
import kafka.utils.TestUtils.createBrokerConfigs
import org.apache.kafka.server.log.remote.storage.{LocalTieredStorage, RemoteLogManagerConfig}
import org.apache.kafka.server.log.remote.storage.LocalTieredStorage.{DELETE_ON_CLOSE_PROP, STORAGE_DIR_PROP}
import org.apache.kafka.common.replica.ReplicaSelector
import org.apache.kafka.server.log.remote.metadata.storage.{TopicBasedRemoteLogMetadataManager, TopicBasedRemoteLogMetadataManagerConfig}
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig.{REMOTE_LOG_METADATA_MANAGER_CONFIG_PREFIX_PROP, REMOTE_STORAGE_MANAGER_CONFIG_PREFIX_PROP}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}

import scala.collection.Seq
import scala.util.{Failure, Success, Try}

/**
  * Base class for integration tests exercising the tiered storage functionality in Apache Kafka.
  */
abstract class TieredStorageTestHarness extends IntegrationTestHarness {

  protected def numMetadataPartitions: Int = 5

  override def generateConfigs: Seq[KafkaConfig] = {
    val overridingProps = new Properties()

    //
    // Configure the tiered storage in Kafka. Set an interval of 1 second for the remote log manager background
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
    overridingProps.setProperty(RemoteLogManagerConfig.REMOTE_LOG_MANAGER_TASK_INTERVAL_MS_PROP, 1000.toString)

    overridingProps.setProperty(REMOTE_STORAGE_MANAGER_CONFIG_PREFIX_PROP, storageConfigPrefix())
    overridingProps.setProperty(REMOTE_LOG_METADATA_MANAGER_CONFIG_PREFIX_PROP, metadataConfigPrefix())

    overridingProps.setProperty(
      metadataConfigPrefix(TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_TOPIC_PARTITIONS_PROP), numMetadataPartitions.toString)
    overridingProps.setProperty(
      metadataConfigPrefix(TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_TOPIC_REPLICATION_FACTOR_PROP), brokerCount.toString)

    //
    // This configuration ensures inactive log segments are deleted fast enough so that
    // the integration tests can confirm a given log segment is present only in the second-tier storage.
    // Note that this does not impact the eligibility of a log segment to be offloaded to the
    // second-tier storage.
    //
    overridingProps.setProperty(KafkaConfig.LogCleanupIntervalMsProp, 1000.toString)

    //
    // This can be customized to read remote log segments from followers.
    //
    readReplicaSelectorClass.foreach(c => overridingProps.put(KafkaConfig.ReplicaSelectorClassProp, c.getName))

    //
    // The directory of the second-tier storage needs to be constant across all instances of storage managers
    // in every broker and throughout the test. Indeed, as brokers are restarted during the test.
    //
    // You can override this property with a fixed path of your choice if you wish to use a non-temporary
    // directory to access its content after a test terminated.
    //
    overridingProps.setProperty(storageConfigPrefix(STORAGE_DIR_PROP), TestUtils.tempDir().getAbsolutePath)

    overridingProps.setProperty(KafkaConfig.UncleanLeaderElectionEnableProp, true.toString)

    // This configuration will remove all the remote files when close is called in remote storage manager.
    // Storage manager close is being called while the server is actively processing the socket requests,
    // so enabling this config can break the existing tests.
    //
    // NOTE: When using TestUtils#tempDir(), the folder gets deleted when VM terminates.
    overridingProps.setProperty(storageConfigPrefix(DELETE_ON_CLOSE_PROP), false.toString)

    createBrokerConfigs(numConfigs = brokerCount, zkConnect).map(KafkaConfig.fromProps(_, overridingProps))
  }

  private var contextOpt: Option[TieredStorageTestContext] = None

  protected def readReplicaSelectorClass: Option[Class[_ <: ReplicaSelector]] = None

  protected def writeTestSpecifications(builder: TieredStorageTestBuilder): Unit

  @BeforeEach
  override def setUp(): Unit = {
    super.setUp()
    contextOpt = Some(new TieredStorageTestContext(zkClient, servers, producerConfig, consumerConfig, securityProtocol))
  }

  @Test
  def executeTieredStorageTest(): Unit = {
    val builder = new TieredStorageTestBuilder
    writeTestSpecifications(builder)

    Try(builder.complete()) match {
      case Success(actions) =>
        contextOpt.foreach(context => actions.foreach(_.execute(context)))

      case Failure(e) =>
        throw new AssertionError("Could not build test specifications. No test was executed.", e)
    }
  }

  @AfterEach
  override def tearDown(): Unit = {
    contextOpt.foreach(_.close())
    super.tearDown()
    contextOpt.foreach(_.printReport(Console.out))
  }

  private def storageConfigPrefix(key: String = "") = LocalTieredStorage.STORAGE_CONFIG_PREFIX + key

  private def metadataConfigPrefix(key: String = "") = "rlmm.config." + key
}

object TieredStorageTestHarness {
  /**
    * InitialTaskDelayMs is set to 30 seconds for the delete-segment scheduler in Apache Kafka.
    * Hence, we need to wait at least that amount of time before segments eligible for deletion
    * gets physically removed.
    */
  private val storageWaitTimeoutSec = 35

  def getTieredStorages(brokers: Seq[KafkaServer]): Seq[LocalTieredStorage] = {
    brokers.map(_.remoteLogManager.storageManager().asInstanceOf[ClassLoaderAwareRemoteStorageManager])
      .map(_.delegate().asInstanceOf[LocalTieredStorage])
  }

  def getLocalStorages(brokers: Seq[KafkaServer]): Seq[BrokerLocalStorage] = {
    brokers.map(b => new BrokerLocalStorage(b.config.brokerId, b.config.logDirs.head, storageWaitTimeoutSec))
  }
}
