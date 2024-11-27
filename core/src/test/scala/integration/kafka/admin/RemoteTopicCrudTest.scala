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
import org.apache.kafka.clients.admin.{AlterConfigOp, ConfigEntry}
import org.apache.kafka.common.{TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.common.config.{ConfigException, ConfigResource, TopicConfig}
import org.apache.kafka.common.errors.{InvalidConfigurationException, UnknownTopicOrPartitionException}
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.server.config.ServerLogConfigs
import org.apache.kafka.server.log.remote.storage.{NoOpRemoteLogMetadataManager, NoOpRemoteStorageManager, RemoteLogManagerConfig, RemoteLogSegmentId, RemoteLogSegmentMetadata, RemoteLogSegmentState}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.function.Executable
import org.junit.jupiter.api.{BeforeEach, Tag, TestInfo}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{CsvSource, ValueSource}

import java.util
import java.util.concurrent.atomic.AtomicInteger
import java.util.{Collections, Optional, Properties}
import scala.collection.Seq
import scala.concurrent.ExecutionException
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
    if (info.getTestMethod.get().getName.endsWith("SystemRemoteStorageIsDisabled")) {
      sysRemoteStorageEnabled = false
    }
    if (info.getTestMethod.get().getName.equals("testTopicDeletion")) {
      storageManagerClassName = classOf[MyRemoteStorageManager].getName
      metadataManagerClassName = classOf[MyRemoteLogMetadataManager].getName
    }
    super.setUp(info)
    testTopicName = s"${info.getTestMethod.get().getName}-${Random.alphanumeric.take(10).mkString}"
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testCreateRemoteTopicWithValidRetentionTime(quorum: String): Unit = {
    val topicConfig = new Properties()
    topicConfig.put(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true")
    topicConfig.put(TopicConfig.RETENTION_MS_CONFIG, "200")
    topicConfig.put(TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG, "100")
    TestUtils.createTopicWithAdmin(createAdminClient(), testTopicName, brokers, controllerServers, numPartitions, numReplicationFactor,
      topicConfig = topicConfig)
    verifyRemoteLogTopicConfigs(topicConfig)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testCreateRemoteTopicWithValidRetentionSize(quorum: String): Unit = {
    val topicConfig = new Properties()
    topicConfig.put(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true")
    topicConfig.put(TopicConfig.RETENTION_BYTES_CONFIG, "512")
    topicConfig.put(TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG, "256")
    TestUtils.createTopicWithAdmin(createAdminClient(), testTopicName, brokers, controllerServers, numPartitions, numReplicationFactor,
      topicConfig = topicConfig)
    verifyRemoteLogTopicConfigs(topicConfig)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testCreateRemoteTopicWithInheritedLocalRetentionTime(quorum: String): Unit = {
    // inherited local retention ms is 1000
    val topicConfig = new Properties()
    topicConfig.put(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true")
    topicConfig.put(TopicConfig.RETENTION_MS_CONFIG, "1001")
    TestUtils.createTopicWithAdmin(createAdminClient(), testTopicName, brokers, controllerServers, numPartitions, numReplicationFactor,
      topicConfig = topicConfig)
    verifyRemoteLogTopicConfigs(topicConfig)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testCreateRemoteTopicWithInheritedLocalRetentionSize(quorum: String): Unit = {
    // inherited local retention bytes is 1024
    val topicConfig = new Properties()
    topicConfig.put(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true")
    topicConfig.put(TopicConfig.RETENTION_BYTES_CONFIG, "1025")
    TestUtils.createTopicWithAdmin(createAdminClient(), testTopicName, brokers, controllerServers, numPartitions, numReplicationFactor,
      topicConfig = topicConfig)
    verifyRemoteLogTopicConfigs(topicConfig)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testCreateRemoteTopicWithInvalidRetentionTime(quorum: String): Unit = {
    // inherited local retention ms is 1000
    val topicConfig = new Properties()
    topicConfig.put(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true")
    topicConfig.put(TopicConfig.RETENTION_MS_CONFIG, "200")
    assertThrowsException(classOf[InvalidConfigurationException], () =>
      TestUtils.createTopicWithAdmin(createAdminClient(), testTopicName, brokers, controllerServers, numPartitions, numReplicationFactor,
        topicConfig = topicConfig))
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testCreateRemoteTopicWithInvalidRetentionSize(quorum: String): Unit = {
    // inherited local retention bytes is 1024
    val topicConfig = new Properties()
    topicConfig.put(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true")
    topicConfig.put(TopicConfig.RETENTION_BYTES_CONFIG, "512")
    assertThrowsException(classOf[InvalidConfigurationException], () =>
      TestUtils.createTopicWithAdmin(createAdminClient(), testTopicName, brokers, controllerServers, numPartitions, numReplicationFactor,
        topicConfig = topicConfig))
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testCreateCompactedRemoteStorage(quorum: String): Unit = {
    val topicConfig = new Properties()
    topicConfig.put(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true")
    topicConfig.put(TopicConfig.CLEANUP_POLICY_CONFIG, "compact")
    assertThrowsException(classOf[InvalidConfigurationException], () =>
      TestUtils.createTopicWithAdmin(createAdminClient(), testTopicName, brokers, controllerServers, numPartitions, numReplicationFactor,
        topicConfig = topicConfig))
  }

  // `remote.log.delete.on.disable` and `remote.log.copy.disable` only works in KRaft mode.
  @ParameterizedTest
  @CsvSource(Array("kraft,true,true", "kraft,true,false", "kraft,false,true", "kraft,false,false"))
  def testCreateRemoteTopicWithCopyDisabledAndDeleteOnDisable(quorum: String, copyDisabled: Boolean, deleteOnDisable: Boolean): Unit = {
    val topicConfig = new Properties()
    topicConfig.put(TopicConfig.REMOTE_LOG_COPY_DISABLE_CONFIG, copyDisabled.toString)
    topicConfig.put(TopicConfig.REMOTE_LOG_DELETE_ON_DISABLE_CONFIG, deleteOnDisable.toString)
    TestUtils.createTopicWithAdmin(createAdminClient(), testTopicName, brokers, controllerServers, numPartitions, numReplicationFactor,
      topicConfig = topicConfig)
    verifyRemoteLogTopicConfigs(topicConfig)
  }

  // `remote.log.delete.on.disable` only works in KRaft mode.
  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testCreateTopicRetentionMsValidationWithRemoteCopyDisabled(quorum: String): Unit = {
    val testTopicName2 = testTopicName + "2"
    val testTopicName3 = testTopicName + "3"
    val errorMsgMs = "When `remote.log.copy.disable` is set to true, the `local.retention.ms` and `retention.ms` " +
      "must be set to the identical value because there will be no more logs copied to the remote storage."

    // 1. create a topic with `remote.log.copy.disable=true` and have different local.retention.ms and retention.ms value,
    //    it should fail to create the topic
    val topicConfig = new Properties()
    topicConfig.put(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true")
    topicConfig.put(TopicConfig.REMOTE_LOG_COPY_DISABLE_CONFIG, "true")
    topicConfig.put(TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG, "100")
    topicConfig.put(TopicConfig.RETENTION_MS_CONFIG, "1000")
    topicConfig.put(TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG, "-2")

    val admin = createAdminClient()
    val err = assertThrowsException(classOf[InvalidConfigurationException],
            () => TestUtils.createTopicWithAdmin(admin, testTopicName, brokers, controllerServers, numPartitions,
              numReplicationFactor, topicConfig = topicConfig))
    assertEquals(errorMsgMs, err.getMessage)

    // 2. change the local.retention.ms value to the same value as retention.ms should successfully create the topic
    topicConfig.put(TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG, "1000")
    TestUtils.createTopicWithAdmin(admin, testTopicName, brokers, controllerServers, numPartitions, numReplicationFactor,
      topicConfig = topicConfig)

    // 3. change the local.retention.ms value to "-2" should also successfully create the topic
    topicConfig.put(TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG, "-2")
    TestUtils.createTopicWithAdmin(admin, testTopicName2, brokers, controllerServers, numPartitions, numReplicationFactor,
      topicConfig = topicConfig)

    // 4. create a topic with `remote.log.copy.disable=false` and have different local.retention.ms and retention.ms value,
    //    it should successfully creates the topic.
    topicConfig.clear()
    topicConfig.put(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true")
    topicConfig.put(TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG, "100")
    topicConfig.put(TopicConfig.RETENTION_MS_CONFIG, "1000")
    topicConfig.put(TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG, "-2")
    TestUtils.createTopicWithAdmin(admin, testTopicName3, brokers, controllerServers, numPartitions, numReplicationFactor,
      topicConfig = topicConfig)

    // 5. alter the config to `remote.log.copy.disable=true`, it should fail the config change
    val configs = new util.HashMap[ConfigResource, util.Collection[AlterConfigOp]]()
    configs.put(new ConfigResource(ConfigResource.Type.TOPIC, testTopicName3),
      util.Arrays.asList(
        new AlterConfigOp(new ConfigEntry(TopicConfig.REMOTE_LOG_COPY_DISABLE_CONFIG, "true"),
          AlterConfigOp.OpType.SET),
      ))
    val err2 = assertThrowsException(classOf[InvalidConfigurationException],
      () => admin.incrementalAlterConfigs(configs).all().get())
    assertEquals(errorMsgMs, err2.getMessage)

    // 6. alter the config to `remote.log.copy.disable=true` and local.retention.ms == retention.ms, it should work without error
    configs.put(new ConfigResource(ConfigResource.Type.TOPIC, testTopicName3),
      util.Arrays.asList(
        new AlterConfigOp(new ConfigEntry(TopicConfig.REMOTE_LOG_COPY_DISABLE_CONFIG, "true"),
          AlterConfigOp.OpType.SET),
        new AlterConfigOp(new ConfigEntry(TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG, "1000"),
          AlterConfigOp.OpType.SET),
      ))

    admin.incrementalAlterConfigs(configs).all().get()
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testCreateTopicRetentionBytesValidationWithRemoteCopyDisabled(quorum: String): Unit = {
    val testTopicName2 = testTopicName + "2"
    val testTopicName3 = testTopicName + "3"
    val errorMsgBytes = "When `remote.log.copy.disable` is set to true, the `local.retention.bytes` and `retention.bytes` " +
      "must be set to the identical value because there will be no more logs copied to the remote storage."

    // 1. create a topic with `remote.log.copy.disable=true` and have different local.retention.bytes and retention.bytes value,
    //    it should fail to create the topic
    val topicConfig = new Properties()
    topicConfig.put(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true")
    topicConfig.put(TopicConfig.REMOTE_LOG_COPY_DISABLE_CONFIG, "true")
    topicConfig.put(TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG, "100")
    topicConfig.put(TopicConfig.RETENTION_BYTES_CONFIG, "1000")
    topicConfig.put(TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG, "-2")

    val admin = createAdminClient()
    val err = assertThrowsException(classOf[InvalidConfigurationException],
      () => TestUtils.createTopicWithAdmin(admin, testTopicName, brokers, controllerServers, numPartitions,
        numReplicationFactor, topicConfig = topicConfig))
    assertEquals(errorMsgBytes, err.getMessage)

    // 2. change the local.retention.bytes value to the same value as retention.bytes should successfully create the topic
    topicConfig.put(TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG, "1000")
    TestUtils.createTopicWithAdmin(admin, testTopicName, brokers, controllerServers, numPartitions, numReplicationFactor,
      topicConfig = topicConfig)

    // 3. change the local.retention.bytes value to "-2" should also successfully create the topic
    topicConfig.put(TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG, "-2")
    TestUtils.createTopicWithAdmin(admin, testTopicName2, brokers, controllerServers, numPartitions, numReplicationFactor,
      topicConfig = topicConfig)

    // 4. create a topic with `remote.log.copy.disable=false` and have different local.retention.bytes and retention.bytes value,
    //    it should successfully creates the topic.
    topicConfig.clear()
    topicConfig.put(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true")
    topicConfig.put(TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG, "100")
    topicConfig.put(TopicConfig.RETENTION_BYTES_CONFIG, "1000")
    topicConfig.put(TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG, "-2")
    TestUtils.createTopicWithAdmin(admin, testTopicName3, brokers, controllerServers, numPartitions, numReplicationFactor,
      topicConfig = topicConfig)

    // 5. alter the config to `remote.log.copy.disable=true`, it should fail the config change
    val configs = new util.HashMap[ConfigResource, util.Collection[AlterConfigOp]]()
    configs.put(new ConfigResource(ConfigResource.Type.TOPIC, testTopicName3),
      util.Arrays.asList(
        new AlterConfigOp(new ConfigEntry(TopicConfig.REMOTE_LOG_COPY_DISABLE_CONFIG, "true"),
          AlterConfigOp.OpType.SET),
      ))
    val err2 = assertThrowsException(classOf[InvalidConfigurationException],
      () => admin.incrementalAlterConfigs(configs).all().get())
    assertEquals(errorMsgBytes, err2.getMessage)

    // 6. alter the config to `remote.log.copy.disable=true` and local.retention.bytes == retention.bytes, it should work without error
    configs.put(new ConfigResource(ConfigResource.Type.TOPIC, testTopicName3),
      util.Arrays.asList(
        new AlterConfigOp(new ConfigEntry(TopicConfig.REMOTE_LOG_COPY_DISABLE_CONFIG, "true"),
          AlterConfigOp.OpType.SET),
        new AlterConfigOp(new ConfigEntry(TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG, "1000"),
          AlterConfigOp.OpType.SET),
      ))
    admin.incrementalAlterConfigs(configs).all().get()
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testEnableRemoteLogOnExistingTopicTest(quorum: String): Unit = {
    val admin = createAdminClient()
    val topicConfig = new Properties()
    TestUtils.createTopicWithAdmin(admin, testTopicName, brokers, controllerServers, numPartitions, numReplicationFactor,
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

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testEnableRemoteLogWhenSystemRemoteStorageIsDisabled(quorum: String): Unit = {
    val admin = createAdminClient()

    val topicConfigWithRemoteStorage = new Properties()
    topicConfigWithRemoteStorage.put(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true")
    val message = assertThrowsException(classOf[InvalidConfigurationException],
      () => TestUtils.createTopicWithAdmin(createAdminClient(), testTopicName, brokers, controllerServers, numPartitions,
        numReplicationFactor, topicConfig = topicConfigWithRemoteStorage))
    assertTrue(message.getMessage.contains("Tiered Storage functionality is disabled in the broker"))

    TestUtils.createTopicWithAdmin(admin, testTopicName, brokers, controllerServers, numPartitions, numReplicationFactor)
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

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testUpdateTopicConfigWithValidRetentionTimeTest(quorum: String): Unit = {
    val admin = createAdminClient()
    val topicConfig = new Properties()
    topicConfig.put(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true")
    TestUtils.createTopicWithAdmin(admin, testTopicName, brokers, controllerServers, numPartitions, numReplicationFactor,
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

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testUpdateTopicConfigWithValidRetentionSizeTest(quorum: String): Unit = {
    val admin = createAdminClient()
    val topicConfig = new Properties()
    topicConfig.put(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true")
    TestUtils.createTopicWithAdmin(admin, testTopicName, brokers, controllerServers, numPartitions, numReplicationFactor,
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

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testUpdateTopicConfigWithInheritedLocalRetentionTime(quorum: String): Unit = {
    val admin = createAdminClient()
    val topicConfig = new Properties()
    topicConfig.put(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true")
    TestUtils.createTopicWithAdmin(admin, testTopicName, brokers, controllerServers, numPartitions, numReplicationFactor,
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

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testUpdateTopicConfigWithInheritedLocalRetentionSize(quorum: String): Unit = {
    val admin = createAdminClient()
    val topicConfig = new Properties()
    topicConfig.put(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true")
    TestUtils.createTopicWithAdmin(admin, testTopicName, brokers, controllerServers, numPartitions, numReplicationFactor,
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

  // The remote storage config validation on controller level only works in KRaft
  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testUpdateTopicConfigWithDisablingRemoteStorage(quorum: String): Unit = {
    val admin = createAdminClient()
    val topicConfig = new Properties
    topicConfig.setProperty(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true")
    TestUtils.createTopicWithAdmin(admin, testTopicName, brokers, controllerServers, numPartitions, numReplicationFactor,
      topicConfig = topicConfig)

    val configs = new util.HashMap[ConfigResource, util.Collection[AlterConfigOp]]()
    configs.put(new ConfigResource(ConfigResource.Type.TOPIC, testTopicName),
      util.Arrays.asList(
        new AlterConfigOp(new ConfigEntry(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "false"),
          AlterConfigOp.OpType.SET),
      ))
    assertThrowsException(classOf[InvalidConfigurationException],
      () => admin.incrementalAlterConfigs(configs).all().get(), "Disabling remote storage feature on the topic level is not supported.")
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testTopicDeletion(quorum: String): Unit = {
    MyRemoteStorageManager.deleteSegmentEventCounter.set(0)
    val numPartitions = 2
    val topicConfig = new Properties()
    topicConfig.put(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true")
    topicConfig.put(TopicConfig.RETENTION_MS_CONFIG, "200")
    topicConfig.put(TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG, "100")
    TestUtils.createTopicWithAdmin(createAdminClient(), testTopicName, brokers, controllerServers, numPartitions, brokerCount,
      topicConfig = topicConfig)
    TestUtils.deleteTopicWithAdmin(createAdminClient(), testTopicName, brokers, controllerServers)
    assertThrowsException(classOf[UnknownTopicOrPartitionException],
      () => TestUtils.describeTopic(createAdminClient(), testTopicName), "Topic should be deleted")
    TestUtils.waitUntilTrue(() =>
      numPartitions * MyRemoteLogMetadataManager.segmentCountPerPartition == MyRemoteStorageManager.deleteSegmentEventCounter.get(),
      "Remote log segments should be deleted only once by the leader")
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testClusterWideDisablementOfTieredStorageWithEnabledTieredTopic(quorum: String): Unit = {
    val topicConfig = new Properties()
    topicConfig.setProperty(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true")

    TestUtils.createTopicWithAdmin(createAdminClient(), testTopicName, brokers, controllerServers, numPartitions, brokerCount,
      topicConfig = topicConfig)

    val tsDisabledProps = TestUtils.createBrokerConfigs(1, null).head
    instanceConfigs = List(KafkaConfig.fromProps(tsDisabledProps))

    recreateBrokers(startup = true)
    assertTrue(faultHandler.firstException().getCause.isInstanceOf[ConfigException])
    // Normally the exception is thrown as part of the TearDown method of the parent class(es). We would like to not do this.
    faultHandler.setIgnore(true)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testClusterWithoutTieredStorageStartsSuccessfullyIfTopicWithTieringDisabled(quorum: String): Unit = {
    val topicConfig = new Properties()
    topicConfig.setProperty(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, false.toString)

    TestUtils.createTopicWithAdmin(createAdminClient(), testTopicName, brokers, controllerServers, numPartitions, brokerCount,
      topicConfig = topicConfig)

    val tsDisabledProps = TestUtils.createBrokerConfigs(1, null).head
    instanceConfigs = List(KafkaConfig.fromProps(tsDisabledProps))

    recreateBrokers(startup = true)
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
        if (topicConfig.contains(TopicConfig.REMOTE_LOG_COPY_DISABLE_CONFIG)) {
          result = result &&
            topicConfig.getProperty(TopicConfig.REMOTE_LOG_COPY_DISABLE_CONFIG).toBoolean ==
              logBuffer.head.config.remoteLogCopyDisable()
        }
        if (topicConfig.contains(TopicConfig.REMOTE_LOG_DELETE_ON_DISABLE_CONFIG)) {
          result = result &&
            topicConfig.getProperty(TopicConfig.REMOTE_LOG_DELETE_ON_DISABLE_CONFIG).toBoolean ==
              logBuffer.head.config.remoteLogDeleteOnDisable()
        }
      }
      result
    }, s"Failed to update topic config $topicConfig")
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

object MyRemoteStorageManager {
  val deleteSegmentEventCounter = new AtomicInteger(0)
}

class MyRemoteStorageManager extends NoOpRemoteStorageManager {
  import MyRemoteStorageManager._

  override def deleteLogSegmentData(remoteLogSegmentMetadata: RemoteLogSegmentMetadata): Unit = {
    deleteSegmentEventCounter.incrementAndGet()
  }
}

class MyRemoteLogMetadataManager extends NoOpRemoteLogMetadataManager {

  import MyRemoteLogMetadataManager._
  val time = new MockTime()

  override def listRemoteLogSegments(topicIdPartition: TopicIdPartition): util.Iterator[RemoteLogSegmentMetadata] = {
    val segmentMetadataList = new util.ArrayList[RemoteLogSegmentMetadata]()
    for (idx <- 0 until segmentCountPerPartition) {
      val timestamp = time.milliseconds()
      val startOffset = idx * recordsPerSegment
      val endOffset = startOffset + recordsPerSegment - 1
      val segmentLeaderEpochs: util.Map[Integer, java.lang.Long] = Collections.singletonMap(0, 0L)
      segmentMetadataList.add(new RemoteLogSegmentMetadata(new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid()), startOffset, endOffset, timestamp, 0, timestamp, segmentSize, Optional.empty(), RemoteLogSegmentState.COPY_SEGMENT_FINISHED, segmentLeaderEpochs))
    }
    segmentMetadataList.iterator()
  }
}

object MyRemoteLogMetadataManager {
  val segmentCountPerPartition = 10
  val recordsPerSegment = 100
  val segmentSize = 1024
}
