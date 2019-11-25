package integration.kafka.server

import kafka.api.IntegrationTestHarness
import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.junit.Assert.{assertFalse, assertTrue}
import org.junit.{Before, Test}

class StrayPartitionsIntegrationTest extends IntegrationTestHarness {
  override val brokerCount = 2
  private val validTopic = "foo"
  private val strayTopic = "bar"
  private val numPartitions = 1

  @Before
  override def setUp(): Unit = {
    super.setUp()
    createTopic(validTopic, numPartitions, brokerCount)
  }

  @Test
  def testStrayPartitionDeletion(): Unit = {
    val broker = servers.head

    // create stray partition
    val logManager = broker.logManager
    val config = logManager.currentDefaultConfig
    logManager.getOrCreateLog(new TopicPartition(strayTopic, 0), config)
    assertTrue(broker.logManager.getLog(new TopicPartition(validTopic, 0)).isDefined)
    assertTrue(broker.logManager.getLog(new TopicPartition(strayTopic, 0)).isDefined)

    // restart broker
    killBroker(broker.config.brokerId)
    restartDeadBrokers()

    // stray partition should now have been deleted
    TestUtils.waitUntilTrue(() => broker.replicaManager.partitionCount.value > 0, "Timed out waiting for LeaderAndIsrRequest")
    assertTrue(broker.logManager.getLog(new TopicPartition(validTopic, 0)).isDefined)
    assertFalse(broker.logManager.getLog(new TopicPartition(strayTopic, 0)).isDefined)
  }
}
