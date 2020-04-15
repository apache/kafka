package integration.kafka.tiered.storage

import java.util.Properties

import integration.kafka.tiered.storage.TieredStorageTestCaseBuilder.newTestCase
import kafka.api.IntegrationTestHarness
import kafka.server.KafkaConfig
import kafka.utils.TestUtils.createBrokerConfigs
import org.apache.kafka.common.log.remote.metadata.storage.RLMMWithTopicStorage
import org.apache.kafka.common.log.remote.storage.LocalTieredStorage
import org.apache.kafka.common.log.remote.storage.LocalTieredStorage.{DELETE_ON_CLOSE_PROP, STORAGE_DIR_PROP}
import org.junit.{After, Test}

import scala.collection.Seq
import scala.collection.mutable

class TieredStorageTest extends IntegrationTestHarness {

  override def generateConfigs: Seq[KafkaConfig] = {
    val overridingProps = new Properties()
    overridingProps.setProperty(KafkaConfig.RemoteLogStorageEnableProp, true.toString)
    overridingProps.setProperty(KafkaConfig.RemoteLogStorageManagerProp, classOf[LocalTieredStorage].getName)
    overridingProps.setProperty(KafkaConfig.RemoteLogMetadataManagerProp, classOf[RLMMWithTopicStorage].getName)
    overridingProps.setProperty(KafkaConfig.RemoteLogManagerTaskIntervalMsProp, 1000.toString)
    overridingProps.setProperty(KafkaConfig.RemoteLogMetadataTopicReplicationFactorProp, 1.toString)

    overridingProps.setProperty(STORAGE_DIR_PROP, "tiered-storage-tests")
    overridingProps.setProperty(DELETE_ON_CLOSE_PROP, "true")

    createBrokerConfigs(numConfigs = 1, zkConnect).map(KafkaConfig.fromProps(_, overridingProps))
  }

  override protected def brokerCount: Int = 1

  private val testCases = mutable.Buffer[TieredStorageTestCase]()

  @Test
  def test(): Unit = {
    val topicA = "topicA"
    val topicB = "topicB"

    val remoteStorage = serverForId(0).get.remoteLogManager.get.storageManager().asInstanceOf[LocalTieredStorage]
    val kafkaStorageDirectory = configs(0).get(KafkaConfig.LogDirProp).asInstanceOf[String]

    val testCase = newTestCase(servers, zkClient, producerConfig, consumerConfig, remoteStorage, kafkaStorageDirectory)

      .withTopic(topicA, partitions = 1, segmentSize = 1)
      .producing(topicA, partition = 0, key = "k1", value = "v1")
      .producing(topicA, partition = 0, key = "k2", value = "v2")
      .producing(topicA, partition = 0, key = "k3", value = "v3")
      .expectingSegmentToBeOffloaded(topicA, partition = 0, baseOffset = 0, segmentSize = 1)
      .expectingSegmentToBeOffloaded(topicA, partition = 0, baseOffset = 1, segmentSize = 1)

      .withTopic(topicB, partitions = 1, segmentSize = 2)
      .producing(topicB, partition = 0, key = "k1", value = "v1")
      .producing(topicB, partition = 0, key = "k2", value = "v2")
      .producing(topicB, partition = 0, key = "k3", value = "v3")
      .expectingSegmentToBeOffloaded(topicB, partition = 0, baseOffset = 0, segmentSize = 2)

      .create()

    testCases += testCase

    testCase.execute()
    testCase.verify()

  }

  @After
  override def tearDown(): Unit = {
    testCases.foreach(_.tearDown())
    super.tearDown()
  }

}
