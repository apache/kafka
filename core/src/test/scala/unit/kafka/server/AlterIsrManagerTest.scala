package unit.kafka.server

import kafka.api.LeaderAndIsr
import kafka.server.{AlterIsrItem, AlterIsrManagerImpl, BrokerToControllerChannelManager}
import kafka.utils.{MockScheduler, MockTime}
import kafka.zk.KafkaZkClient
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.requests.{AbstractRequest, AlterIsrRequest}
import org.easymock.EasyMock
import org.junit.Assert._
import org.junit.{Before, Test}


class AlterIsrManagerTest {

  val topic = "test-topic"
  val time = new MockTime
  val metrics = new Metrics
  val brokerId = 1

  var kafkaZkClient: KafkaZkClient = _
  var brokerToController: BrokerToControllerChannelManager = _

  @Before
  def setup(): Unit = {
    kafkaZkClient = EasyMock.createMock(classOf[KafkaZkClient])
    EasyMock.expect(kafkaZkClient.getBrokerEpoch(EasyMock.anyInt())).andReturn(Some(4)).anyTimes()
    EasyMock.replay(kafkaZkClient)

    brokerToController = EasyMock.createMock(classOf[BrokerToControllerChannelManager])
    EasyMock.expect(brokerToController.start()).once()
    EasyMock.expect(brokerToController.shutdown()).once()

  }

  @Test
  def testBasic(): Unit = {
    EasyMock.expect(brokerToController.sendRequest(EasyMock.anyObject(), EasyMock.anyObject())).once()
    EasyMock.replay(brokerToController)

    val scheduler = new MockScheduler(time)
    val alterIsrManager = new AlterIsrManagerImpl(brokerToController, kafkaZkClient, scheduler, time, brokerId)
    alterIsrManager.startup()
    alterIsrManager.enqueueIsrUpdate(AlterIsrItem(new TopicPartition(topic, 1), new LeaderAndIsr(1, 1, List(1,2,3), 10), _ => {}))
    time.sleep(50)
    scheduler.tick()

    alterIsrManager.shutdown()
    EasyMock.verify(brokerToController)
  }

  @Test
  def testOverwriteWithinBatch(): Unit = {
    val capture = EasyMock.newCapture[AbstractRequest.Builder[AlterIsrRequest]]()
    EasyMock.expect(brokerToController.sendRequest(EasyMock.capture(capture), EasyMock.anyObject())).once()
    EasyMock.replay(brokerToController)

    val scheduler = new MockScheduler(time)
    val alterIsrManager = new AlterIsrManagerImpl(brokerToController, kafkaZkClient, scheduler, time, brokerId)
    alterIsrManager.startup()
    // Only send one ISR update for a given topic+partition
    alterIsrManager.enqueueIsrUpdate(AlterIsrItem(new TopicPartition(topic, 1), new LeaderAndIsr(1, 1, List(1,2,3), 10), _ => {}))
    alterIsrManager.enqueueIsrUpdate(AlterIsrItem(new TopicPartition(topic, 1), new LeaderAndIsr(1, 1, List(1,2), 10), _ => {}))

    time.sleep(50)
    scheduler.tick()

    alterIsrManager.shutdown()
    EasyMock.verify(brokerToController)

    val request = capture.getValue.build()
    assertEquals(request.data().topics().size(), 1)
    assertEquals(request.data().topics().get(0).partitions().get(0).newIsr().size(), 2)
  }

  @Test
  def testSingleBatch(): Unit = {
    val capture = EasyMock.newCapture[AbstractRequest.Builder[AlterIsrRequest]]()
    EasyMock.expect(brokerToController.sendRequest(EasyMock.capture(capture), EasyMock.anyObject())).once()
    EasyMock.replay(brokerToController)

    val scheduler = new MockScheduler(time)
    val alterIsrManager = new AlterIsrManagerImpl(brokerToController, kafkaZkClient, scheduler, time, brokerId)
    alterIsrManager.startup()

    for (i <- 0 to 9) {
      alterIsrManager.enqueueIsrUpdate(AlterIsrItem(new TopicPartition(topic, i), new LeaderAndIsr(1, 1, List(1,2,3), 10), _ => {}))
      time.sleep(1)
    }

    time.sleep(50)
    scheduler.tick()

    // This should not be included in the batch
    alterIsrManager.enqueueIsrUpdate(AlterIsrItem(new TopicPartition(topic, 10), new LeaderAndIsr(1, 1, List(1,2,3), 10), _ => {}))

    alterIsrManager.shutdown()
    EasyMock.verify(brokerToController)

    val request = capture.getValue.build()
    assertEquals(request.data().topics().size(), 1)
    assertEquals(request.data().topics().get(0).partitions().size(), 10)
  }

  @Test(expected = classOf[RuntimeException])
  def testNoBrokerEpoch(): Unit = {
    EasyMock.reset(kafkaZkClient)
    EasyMock.expect(kafkaZkClient.getBrokerEpoch(EasyMock.anyInt())).andReturn(None).anyTimes()
    EasyMock.replay(kafkaZkClient)

    val scheduler = new MockScheduler(time)
    val alterIsrManager = new AlterIsrManagerImpl(brokerToController, kafkaZkClient, scheduler, time, brokerId)
    alterIsrManager.startup()
    alterIsrManager.enqueueIsrUpdate(AlterIsrItem(new TopicPartition(topic, 1), new LeaderAndIsr(1, 1, List(1,2,3), 10), _ => {}))
    time.sleep(50) // throws
    scheduler.tick()

    alterIsrManager.shutdown()
    EasyMock.verify(brokerToController)
  }
}
