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
import kafka.utils.{TestInfoUtils, TestUtils}
import org.apache.kafka.clients.admin.{AlterConfigOp, ConfigEntry}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.{ConfigResource, TopicConfig}
import org.apache.kafka.common.errors.InvalidConfigurationException
import org.apache.kafka.server.log.remote.storage.{NoOpRemoteLogMetadataManager, NoOpRemoteStorageManager, RemoteLogManagerConfig}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.function.Executable
import org.junit.jupiter.api.{BeforeEach, Tag, TestInfo}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.util
import java.util.{Collections, Properties}
import scala.collection.Seq
import scala.concurrent.ExecutionException
import scala.util.Random

@Tag("integration")
class RemoteTopicCrudTest extends IntegrationTestHarness {

  val numPartitions = 2
  val numReplicationFactor = 2
  var testTopicName: String = _
  var sysRemoteStorageEnabled = true

  override protected def brokerCount: Int = 2

  override protected def modifyConfigs(props: Seq[Properties]): Unit = {
    props.foreach(p => p.putAll(overrideProps()))
  }

  override protected def kraftControllerConfigs(): Seq[Properties] = {
    Seq(overrideProps())
  }

  @BeforeEach
  override def setUp(info: TestInfo): Unit = {
    if (info.getTestMethod.get().getName.endsWith("SystemRemoteStorageIsDisabled")) {
      sysRemoteStorageEnabled = false
    }
    super.setUp(info)
    testTopicName = s"${info.getTestMethod.get().getName}-${Random.alphanumeric.take(10).mkString}"
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testCreateRemoteTopicWithValidRetentionTime(quorum: String): Unit = {
    val topicConfig = new Properties()
    topicConfig.put(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true")
    topicConfig.put(TopicConfig.RETENTION_MS_CONFIG, "200")
    topicConfig.put(TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG, "100")
    TestUtils.createTopicWithAdmin(createAdminClient(), testTopicName, brokers, numPartitions, numReplicationFactor,
      topicConfig = topicConfig)
    verifyRemoteLogTopicConfigs(topicConfig)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testCreateRemoteTopicWithValidRetentionSize(quorum: String): Unit = {
    val topicConfig = new Properties()
    topicConfig.put(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true")
    topicConfig.put(TopicConfig.RETENTION_BYTES_CONFIG, "512")
    topicConfig.put(TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG, "256")
    TestUtils.createTopicWithAdmin(createAdminClient(), testTopicName, brokers, numPartitions, numReplicationFactor,
      topicConfig = topicConfig)
    verifyRemoteLogTopicConfigs(topicConfig)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testCreateRemoteTopicWithInheritedLocalRetentionTime(quorum: String): Unit = {
    // inherited local retention ms is 1000
    val topicConfig = new Properties()
    topicConfig.put(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true")
    topicConfig.put(TopicConfig.RETENTION_MS_CONFIG, "1001")
    TestUtils.createTopicWithAdmin(createAdminClient(), testTopicName, brokers, numPartitions, numReplicationFactor,
      topicConfig = topicConfig)
    verifyRemoteLogTopicConfigs(topicConfig)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testCreateRemoteTopicWithInheritedLocalRetentionSize(quorum: String): Unit = {
    // inherited local retention bytes is 1024
    val topicConfig = new Properties()
    topicConfig.put(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true")
    topicConfig.put(TopicConfig.RETENTION_BYTES_CONFIG, "1025")
    TestUtils.createTopicWithAdmin(createAdminClient(), testTopicName, brokers, numPartitions, numReplicationFactor,
      topicConfig = topicConfig)
    verifyRemoteLogTopicConfigs(topicConfig)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testCreateRemoteTopicWithInvalidRetentionTime(quorum: String): Unit = {
    // inherited local retention ms is 1000
    val topicConfig = new Properties()
    topicConfig.put(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true")
    topicConfig.put(TopicConfig.RETENTION_MS_CONFIG, "200")
    assertThrowsException(classOf[InvalidConfigurationException], () =>
      TestUtils.createTopicWithAdmin(createAdminClient(), testTopicName, brokers, numPartitions, numReplicationFactor,
        topicConfig = topicConfig))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testCreateRemoteTopicWithInvalidRetentionSize(quorum: String): Unit = {
    // inherited local retention bytes is 1024
    val topicConfig = new Properties()
    topicConfig.put(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true")
    topicConfig.put(TopicConfig.RETENTION_BYTES_CONFIG, "512")
    assertThrowsException(classOf[InvalidConfigurationException], () =>
      TestUtils.createTopicWithAdmin(createAdminClient(), testTopicName, brokers, numPartitions, numReplicationFactor,
        topicConfig = topicConfig))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testCreateCompactedRemoteStorage(quorum: String): Unit = {
    val topicConfig = new Properties()
    topicConfig.put(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true")
    topicConfig.put(TopicConfig.CLEANUP_POLICY_CONFIG, "compact")
    assertThrowsException(classOf[InvalidConfigurationException], () =>
      TestUtils.createTopicWithAdmin(createAdminClient(), testTopicName, brokers, numPartitions, numReplicationFactor,
        topicConfig = topicConfig))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testEnableRemoteLogOnExistingTopicTest(quorum: String): Unit = {
    val admin = createAdminClient()
    val topicConfig = new Properties()
    TestUtils.createTopicWithAdmin(admin, testTopicName, brokers, numPartitions, numReplicationFactor,
      topicConfig = topicConfig)

    val configs = new util.HashMap[ConfigResource, util.Collection[AlterConfigOp]]()
    configs.put(new ConfigResource(ConfigResource.Type.TOPIC, testTopicName),
      Collections.singleton(
      new AlterConfigOp(new ConfigEntry(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true"),
        AlterConfigOp.OpType.SET))
    )
    admin.incrementalAlterConfigs(configs).all().get()
    verifyRemoteLogTopicConfigs(topicConfig)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testEnableRemoteLogWhenSystemRemoteStorageIsDisabled(quorum: String): Unit = {
    val admin = createAdminClient()

    val topicConfigWithRemoteStorage = new Properties()
    topicConfigWithRemoteStorage.put(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true")
    val message = assertThrowsException(classOf[InvalidConfigurationException],
      () => TestUtils.createTopicWithAdmin(createAdminClient(), testTopicName, brokers, numPartitions,
        numReplicationFactor, topicConfig = topicConfigWithRemoteStorage))
    assertTrue(message.getMessage.contains("Tiered Storage functionality is disabled in the broker"))

    TestUtils.createTopicWithAdmin(admin, testTopicName, brokers, numPartitions, numReplicationFactor)
    val configs = new util.HashMap[ConfigResource, util.Collection[AlterConfigOp]]()
    configs.put(new ConfigResource(ConfigResource.Type.TOPIC, testTopicName),
      Collections.singleton(
        new AlterConfigOp(new ConfigEntry(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true"),
          AlterConfigOp.OpType.SET))
    )
    val errorMessage = assertThrowsException(classOf[InvalidConfigurationException],
      () => admin.incrementalAlterConfigs(configs).all().get())
    assertTrue(errorMessage.getMessage.contains("Tiered Storage functionality is disabled in the broker"))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testUpdateTopicConfigWithValidRetentionTimeTest(quorum: String): Unit = {
    val admin = createAdminClient()
    val topicConfig = new Properties()
    topicConfig.put(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true")
    TestUtils.createTopicWithAdmin(admin, testTopicName, brokers, numPartitions, numReplicationFactor,
      topicConfig = topicConfig)

    val configs = new util.HashMap[ConfigResource, util.Collection[AlterConfigOp]]()
    configs.put(new ConfigResource(ConfigResource.Type.TOPIC, testTopicName),
      util.Arrays.asList(
        new AlterConfigOp(new ConfigEntry(TopicConfig.RETENTION_MS_CONFIG, "200"),
          AlterConfigOp.OpType.SET),
        new AlterConfigOp(new ConfigEntry(TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG, "100"),
          AlterConfigOp.OpType.SET)
    ))
    admin.incrementalAlterConfigs(configs).all().get()
    verifyRemoteLogTopicConfigs(topicConfig)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testUpdateTopicConfigWithValidRetentionSizeTest(quorum: String): Unit = {
    val admin = createAdminClient()
    val topicConfig = new Properties()
    topicConfig.put(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true")
    TestUtils.createTopicWithAdmin(admin, testTopicName, brokers, numPartitions, numReplicationFactor,
      topicConfig = topicConfig)

    val configs = new util.HashMap[ConfigResource, util.Collection[AlterConfigOp]]()
    configs.put(new ConfigResource(ConfigResource.Type.TOPIC, testTopicName),
      util.Arrays.asList(
        new AlterConfigOp(new ConfigEntry(TopicConfig.RETENTION_BYTES_CONFIG, "200"),
          AlterConfigOp.OpType.SET),
        new AlterConfigOp(new ConfigEntry(TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG, "100"),
          AlterConfigOp.OpType.SET)
      ))
    admin.incrementalAlterConfigs(configs).all().get()
    verifyRemoteLogTopicConfigs(topicConfig)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testUpdateTopicConfigWithInheritedLocalRetentionTime(quorum: String): Unit = {
    val admin = createAdminClient()
    val topicConfig = new Properties()
    topicConfig.put(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true")
    TestUtils.createTopicWithAdmin(admin, testTopicName, brokers, numPartitions, numReplicationFactor,
      topicConfig = topicConfig)

    // inherited local retention ms is 1000
    val configs = new util.HashMap[ConfigResource, util.Collection[AlterConfigOp]]()
    configs.put(new ConfigResource(ConfigResource.Type.TOPIC, testTopicName),
      util.Arrays.asList(
        new AlterConfigOp(new ConfigEntry(TopicConfig.RETENTION_MS_CONFIG, "200"),
          AlterConfigOp.OpType.SET),
      ))
    assertThrowsException(classOf[InvalidConfigurationException],
      () => admin.incrementalAlterConfigs(configs).all().get())
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testUpdateTopicConfigWithInheritedLocalRetentionSize(quorum: String): Unit = {
    val admin = createAdminClient()
    val topicConfig = new Properties()
    topicConfig.put(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true")
    TestUtils.createTopicWithAdmin(admin, testTopicName, brokers, numPartitions, numReplicationFactor,
      topicConfig = topicConfig)

    // inherited local retention bytes is 1024
    val configs = new util.HashMap[ConfigResource, util.Collection[AlterConfigOp]]()
    configs.put(new ConfigResource(ConfigResource.Type.TOPIC, testTopicName),
      util.Arrays.asList(
        new AlterConfigOp(new ConfigEntry(TopicConfig.RETENTION_BYTES_CONFIG, "512"),
          AlterConfigOp.OpType.SET),
      ))
    assertThrowsException(classOf[InvalidConfigurationException],
      () => admin.incrementalAlterConfigs(configs).all().get(), "Invalid local retention size")
  }

  private def assertThrowsException(exceptionType: Class[_ <: Throwable],
                                    executable: Executable,
                                    message: String = ""): Throwable = {
    assertThrows(exceptionType, () => {
      try {
        executable.execute()
      } catch {
        case e: ExecutionException => throw e.getCause
      }
    }, message)
  }

  private def verifyRemoteLogTopicConfigs(topicConfig: Properties): Unit = {
    TestUtils.waitUntilTrue(() => {
      val logBuffer = brokers.flatMap(_.logManager.getLog(new TopicPartition(testTopicName, 0)))
      var result = logBuffer.nonEmpty
      if (result) {
        if (topicConfig.containsKey(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG)) {
          result = result &&
            topicConfig.getProperty(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG).toBoolean ==
              logBuffer.head.config.remoteStorageEnable()
        }
        if (topicConfig.containsKey(TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG)) {
          result = result &&
            topicConfig.getProperty(TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG).toLong ==
              logBuffer.head.config.localRetentionBytes()
        }
        if (topicConfig.containsKey(TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG)) {
          result = result &&
            topicConfig.getProperty(TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG).toLong ==
              logBuffer.head.config.localRetentionMs()
        }
        if (topicConfig.containsKey(TopicConfig.RETENTION_MS_CONFIG)) {
          result = result &&
            topicConfig.getProperty(TopicConfig.RETENTION_MS_CONFIG).toLong ==
              logBuffer.head.config.retentionMs
        }
        if (topicConfig.containsKey(TopicConfig.RETENTION_BYTES_CONFIG)) {
          result = result &&
            topicConfig.getProperty(TopicConfig.RETENTION_BYTES_CONFIG).toLong ==
              logBuffer.head.config.retentionSize
        }
      }
      result
    }, s"Failed to update topic config $topicConfig")
  }

  private def overrideProps(): Properties = {
    val props = new Properties()
    props.put(RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, sysRemoteStorageEnabled.toString)
    props.put(RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CLASS_NAME_PROP,
      classOf[NoOpRemoteStorageManager].getName)
    props.put(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP,
      classOf[NoOpRemoteLogMetadataManager].getName)

    props.put(KafkaConfig.LogRetentionTimeMillisProp, "2000")
    props.put(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_MS_PROP, "1000")
    props.put(KafkaConfig.LogRetentionBytesProp, "2048")
    props.put(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_BYTES_PROP, "1024")
    props
  }
}
