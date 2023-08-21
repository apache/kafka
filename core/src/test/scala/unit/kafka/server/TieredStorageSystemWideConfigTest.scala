package unit.kafka.server

import kafka.integration.KafkaServerTestHarness
import kafka.server.KafkaConfig
import kafka.utils.{TestInfoUtils, TestUtils}
import org.apache.kafka.common.config.{ConfigException, TopicConfig}
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig
import org.junit.jupiter.api.Assertions.{assertThrows, assertTrue}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.util.Properties

class TieredStorageSystemWideConfigTest extends KafkaServerTestHarness {

  override def generateConfigs: collection.Seq[KafkaConfig] = {
    val tsEnabledProps = TestUtils.createBrokerConfigs(1, zkConnectOrNull).head
    tsEnabledProps.put(RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, true.toString)
    tsEnabledProps.put(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP,
      "org.apache.kafka.server.log.remote.storage.NoOpRemoteLogMetadataManager")
    tsEnabledProps.put(RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CLASS_NAME_PROP,
      "org.apache.kafka.server.log.remote.storage.NoOpRemoteStorageManager")
    List(KafkaConfig.fromProps(tsEnabledProps))
  }

  /*
   * The controller in KRaft needs to be aware that tiered storage is enabled cluster-wide in order to carry
   * out validations correctly. As such, we need to provide the correct properties.
   */
  override def kraftControllerConfigs(): collection.Seq[Properties] = {
    val properties = new Properties()
    properties.setProperty(RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, true.toString)
    properties.setProperty(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP,
      "org.apache.kafka.server.log.remote.storage.NoOpRemoteLogMetadataManager")
    properties.setProperty(RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CLASS_NAME_PROP,
      "org.apache.kafka.server.log.remote.storage.NoOpRemoteStorageManager")
    Seq(properties)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk"))
  def testClusterWideDisablementOfTieredStorageWithEnabledTieredTopicZK(quorum: String): Unit = {
    val topicConfig = new Properties()
    topicConfig.setProperty(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, true.toString)

    this.createTopic("batman", topicConfig = topicConfig)

    val tsDisabledProps = TestUtils.createBrokerConfigs(1, zkConnectOrNull).head
    instanceConfigs = List(KafkaConfig.fromProps(tsDisabledProps))

    assertThrows(classOf[ConfigException], () => recreateBrokers(startup = true))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk"))
  def testClusterWithoutTieredStorageStartsSuccessfullyIfTopicWithTieringDisabledZK(quorum: String): Unit = {
    val topicConfig = new Properties()
    topicConfig.setProperty(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, false.toString)

    this.createTopic("batman", topicConfig = topicConfig)

    val tsDisabledProps = TestUtils.createBrokerConfigs(1, zkConnectOrNull).head
    instanceConfigs = List(KafkaConfig.fromProps(tsDisabledProps))

    recreateBrokers(startup = true)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("kraft"))
  def testClusterWideDisablementOfTieredStorageWithEnabledTieredTopicKRaft(quorum: String): Unit = {
    val topicConfig = new Properties()
    topicConfig.setProperty(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, true.toString)

    this.createTopic("batman", topicConfig = topicConfig)

    val tsDisabledProps = TestUtils.createBrokerConfigs(1, zkConnectOrNull).head
    instanceConfigs = List(KafkaConfig.fromProps(tsDisabledProps))

    recreateBrokers(startup = true)

    assertTrue(faultHandler.firstException().getCause.isInstanceOf[ConfigException])
    // Normally the exception is thrown as part of the TearDown method of the parent class(es). We would like to not do this.
    faultHandler.setIgnore(true)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("kraft"))
  def testClusterWithoutTieredStorageStartsSuccessfullyIfTopicWithTieringDisabledKRaft(quorum: String): Unit = {
    val topicConfig = new Properties()
    topicConfig.setProperty(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, false.toString)

    this.createTopic("batman", topicConfig = topicConfig)

    val tsDisabledProps = TestUtils.createBrokerConfigs(1, zkConnectOrNull).head
    instanceConfigs = List(KafkaConfig.fromProps(tsDisabledProps))

    recreateBrokers(startup = true)
  }

}
