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
package kafka.admin

import kafka.api.IntegrationTestHarness
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.apache.kafka.common.config.{ConfigException, TopicConfig}
import org.apache.kafka.server.config.ServerLogConfigs
import org.apache.kafka.server.log.remote.storage._
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{BeforeEach, Tag, Test, TestInfo}

import java.util.Properties
import scala.collection.Seq
import scala.util.Random

@Tag("integration")
class RemoteTopicCrudTest extends IntegrationTestHarness {

  val numPartitions = 2
  val numReplicationFactor = 2

  var testTopicName: String = _
  var sysRemoteStorageEnabled = true
  var storageManagerClassName: String = classOf[NoOpRemoteStorageManager].getName
  var metadataManagerClassName: String = classOf[NoOpRemoteLogMetadataManager].getName

  override protected def brokerCount: Int = 2

  override protected def modifyConfigs(props: Seq[Properties]): Unit = {
    props.foreach(p => p.putAll(overrideProps()))
  }

  override protected def kraftControllerConfigs(testInfo: TestInfo): Seq[Properties] = {
    Seq(overrideProps())
  }

  @BeforeEach
  override def setUp(info: TestInfo): Unit = {
    super.setUp(info)
    testTopicName = s"${info.getTestMethod.get().getName}-${Random.alphanumeric.take(10).mkString}"
  }

  @Test
  def testClusterWideDisablementOfTieredStorageWithEnabledTieredTopic(): Unit = {
    val topicConfig = new Properties()
    topicConfig.setProperty(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true")

    TestUtils.createTopicWithAdmin(createAdminClient(), testTopicName, brokers, controllerServers, numPartitions, brokerCount,
      topicConfig = topicConfig)

    val tsDisabledProps = TestUtils.createBrokerConfigs(1).head
    instanceConfigs = List(KafkaConfig.fromProps(tsDisabledProps))

    recreateBrokers(startup = true)
    assertTrue(faultHandler.firstException().getCause.isInstanceOf[ConfigException])
    // Normally the exception is thrown as part of the TearDown method of the parent class(es). We would like to not do this.
    faultHandler.setIgnore(true)
  }

  @Test
  def testClusterWithoutTieredStorageStartsSuccessfullyIfTopicWithTieringDisabled(): Unit = {
    val topicConfig = new Properties()
    topicConfig.setProperty(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, false.toString)

    TestUtils.createTopicWithAdmin(createAdminClient(), testTopicName, brokers, controllerServers, numPartitions, brokerCount,
      topicConfig = topicConfig)

    val tsDisabledProps = TestUtils.createBrokerConfigs(1).head
    instanceConfigs = List(KafkaConfig.fromProps(tsDisabledProps))

    recreateBrokers(startup = true)
  }

  private def overrideProps(): Properties = {
    val props = new Properties()
    props.put(RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, sysRemoteStorageEnabled.toString)
    props.put(RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CLASS_NAME_PROP, storageManagerClassName)
    props.put(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP, metadataManagerClassName)
    props.put(ServerLogConfigs.LOG_RETENTION_TIME_MILLIS_CONFIG, "2000")
    props.put(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_MS_PROP, "1000")
    props.put(ServerLogConfigs.LOG_RETENTION_BYTES_CONFIG, "2048")
    props.put(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_BYTES_PROP, "1024")
    props
  }
}