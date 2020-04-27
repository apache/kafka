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
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.TestUtils
import kafka.utils.TestUtils.createBrokerConfigs
import org.apache.kafka.common.log.remote.metadata.storage.RLMMWithTopicStorage
import org.apache.kafka.common.log.remote.storage.LocalTieredStorage
import org.apache.kafka.common.log.remote.storage.LocalTieredStorage.STORAGE_DIR_PROP
import org.apache.kafka.common.replica.ReplicaSelector
import org.junit.{After, Before, Test}
import unit.kafka.utils.BrokerLocalStorage

import scala.collection.Seq

/**
  * Base class for integration tests exercising the tiered storage functionality in Apache Kafka.
  */
abstract class TieredStorageTestHarness extends IntegrationTestHarness {

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
    overridingProps.setProperty(KafkaConfig.RemoteLogStorageEnableProp, true.toString)
    overridingProps.setProperty(KafkaConfig.RemoteLogStorageManagerProp, classOf[LocalTieredStorage].getName)
    overridingProps.setProperty(KafkaConfig.RemoteLogMetadataManagerProp, classOf[RLMMWithTopicStorage].getName)
    overridingProps.setProperty(KafkaConfig.RemoteLogManagerTaskIntervalMsProp, 1000.toString)
    overridingProps.setProperty(KafkaConfig.RemoteLogMetadataTopicReplicationFactorProp, brokerCount.toString)

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
    overridingProps.setProperty(STORAGE_DIR_PROP, TestUtils.tempDir().getAbsolutePath)

    createBrokerConfigs(numConfigs = brokerCount, zkConnect).map(KafkaConfig.fromProps(_, overridingProps))
  }

  private var context: TieredStorageTestContext = _

  protected def readReplicaSelectorClass: Option[Class[_ <: ReplicaSelector]] = None

  protected def writeTestSpecifications(builder: TieredStorageTestBuilder): Unit

  @Before
  override def setUp(): Unit = {
    super.setUp()
    context = new TieredStorageTestContext(zkClient, servers, producerConfig, consumerConfig, securityProtocol)
  }

  @Test
  def executeTieredStorageTest(): Unit = {
    val builder = new TieredStorageTestBuilder
    writeTestSpecifications(builder)
    builder.complete().foreach(_.execute(context))
  }

  @After
  override def tearDown(): Unit = {
    if (context != null) {
      context.close()
    }
    super.tearDown()
  }
}

object TieredStorageTestHarness {
  /**
    * InitialTaskDelayMs is set to 30 seconds for the delete-segment scheduler in Apache Kafka.
    * Hence, we need to wait at least that amount of time before segments eligible for deletion
    * gets physically removed.
    */
  private val storageWaitTimeoutSec = 35

  def getTieredStorages(brokers: Seq[KafkaServer]): Seq[LocalTieredStorage] = {
    brokers.map(_.remoteLogManager.get.storageManager().asInstanceOf[LocalTieredStorage])
  }

  def getLocalStorages(brokers: Seq[KafkaServer]): Seq[BrokerLocalStorage] = {
    brokers.map(b => new BrokerLocalStorage(b.config.logDirs.head, storageWaitTimeoutSec))
  }
}
