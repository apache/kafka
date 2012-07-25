package kafka.server

import org.scalatest.junit.JUnit3Suite
import org.junit.Assert._
import kafka.admin.CreateTopicCommand
import kafka.utils.TestUtils._
import kafka.utils.{Utils, TestUtils}
import kafka.zk.ZooKeeperTestHarness
import kafka.message.Message
import kafka.producer.{ProducerConfig, ProducerData, Producer}
import org.junit.Test

class LogRecoveryTest extends JUnit3Suite with ZooKeeperTestHarness {

  val configs = TestUtils.createBrokerConfigs(2).map(new KafkaConfig(_) {
    override val replicaMaxLagTimeMs = 5000L
    override val replicaMaxLagBytes = 10L
    override val flushInterval = 10
    override val replicaMinBytes = 20
  })
  val topic = "new-topic"
  val partitionId = 0

  val brokerId1 = 0
  val brokerId2 = 1

  val port1 = TestUtils.choosePort()
  val port2 = TestUtils.choosePort()

  var server1: KafkaServer = null
  var server2: KafkaServer = null

  val configProps1 = configs.head
  val configProps2 = configs.last

  val sent1 = List(new Message("hello".getBytes()), new Message("there".getBytes()))
  val sent2 = List( new Message("more".getBytes()), new Message("messages".getBytes()))

  var producer: Producer[Int, Message] = null
  var hwFile1: HighwaterMarkCheckpoint = new HighwaterMarkCheckpoint(configProps1.logDir)
  var hwFile2: HighwaterMarkCheckpoint = new HighwaterMarkCheckpoint(configProps2.logDir)
  var servers: Seq[KafkaServer] = Seq.empty[KafkaServer]

  @Test
  def testHWCheckpointNoFailuresSingleLogSegment {
    // start both servers
    server1 = TestUtils.createServer(configProps1)
    server2 = TestUtils.createServer(configProps2)
    servers ++= List(server1, server2)

    val producerProps = getProducerConfig(zkConnect, 64*1024, 100000, 10000)
    producerProps.put("producer.request.timeout.ms", "1000")
    producerProps.put("producer.request.required.acks", "-1")
    producer = new Producer[Int, Message](new ProducerConfig(producerProps))

    // create topic with 1 partition, 2 replicas, one on each broker
    CreateTopicCommand.createTopic(zkClient, topic, 1, 2, configs.map(_.brokerId).mkString(":"))

    // wait until leader is elected
    var leader = waitUntilLeaderIsElected(zkClient, topic, partitionId, 500)
    assertTrue("Leader should get elected", leader.isDefined)
    // NOTE: this is to avoid transient test failures
    assertTrue("Leader could be broker 0 or broker 1", (leader.getOrElse(-1) == 0) || (leader.getOrElse(-1) == 1))

    sendMessages(2)
    // don't wait for follower to read the leader's hw
    // shutdown the servers to allow the hw to be checkpointed
    servers.map(server => server.shutdown())
    producer.close()
    val leaderHW = hwFile1.read(topic, 0)
    assertEquals(60L, leaderHW)
    val followerHW = hwFile2.read(topic, 0)
    assertEquals(30L, followerHW)
    servers.map(server => Utils.rm(server.config.logDir))
  }

  def testHWCheckpointWithFailuresSingleLogSegment {
    // start both servers
    server1 = TestUtils.createServer(configProps1)
    server2 = TestUtils.createServer(configProps2)
    servers ++= List(server1, server2)

    val producerProps = getProducerConfig(zkConnect, 64*1024, 100000, 10000)
    producerProps.put("producer.request.timeout.ms", "1000")
    producerProps.put("producer.request.required.acks", "-1")
    producer = new Producer[Int, Message](new ProducerConfig(producerProps))

    // create topic with 1 partition, 2 replicas, one on each broker
    CreateTopicCommand.createTopic(zkClient, topic, 1, 2, configs.map(_.brokerId).mkString(":"))

    // wait until leader is elected
    var leader = waitUntilLeaderIsElected(zkClient, topic, partitionId, 500)
    assertTrue("Leader should get elected", leader.isDefined)
    // NOTE: this is to avoid transient test failures
    assertTrue("Leader could be broker 0 or broker 1", (leader.getOrElse(-1) == 0) || (leader.getOrElse(-1) == 1))

    assertEquals(0L, hwFile1.read(topic, 0))

    sendMessages()

    // kill the server hosting the preferred replica
    server1.shutdown()
    assertEquals(30L, hwFile1.read(topic, 0))

    // check if leader moves to the other server
    leader = waitUntilLeaderIsElected(zkClient, topic, partitionId, 500)
    assertEquals("Leader must move to broker 1", 1, leader.getOrElse(-1))

    // bring the preferred replica back
    server1.startup()

    leader = waitUntilLeaderIsElected(zkClient, topic, partitionId, 500)
    assertEquals("Leader must remain on broker 1", 1, leader.getOrElse(-1))

    assertEquals(30L, hwFile1.read(topic, 0))
    // since server 2 was never shut down, the hw value of 30 is probably not checkpointed to disk yet
    server2.shutdown()
    assertEquals(30L, hwFile2.read(topic, 0))

    server2.startup()
    leader = waitUntilLeaderIsElected(zkClient, topic, partitionId, 500)
    assertEquals("Leader must remain on broker 0", 0, leader.getOrElse(-1))

    sendMessages()
    // give some time for follower 1 to record leader HW of 60
    TestUtils.waitUntilTrue(() => server2.getReplica(topic, 0).get.highWatermark() == 60L, 500)

    // shutdown the servers to allow the hw to be checkpointed
    servers.map(server => server.shutdown())
    producer.close()
    assertEquals(60L, hwFile1.read(topic, 0))
    assertEquals(60L, hwFile2.read(topic, 0))
    servers.map(server => Utils.rm(server.config.logDir))
  }

  def testHWCheckpointNoFailuresMultipleLogSegments {
    val configs = TestUtils.createBrokerConfigs(2).map(new KafkaConfig(_) {
      override val replicaMaxLagTimeMs = 5000L
      override val replicaMaxLagBytes = 10L
      override val flushInterval = 10
      override val replicaMinBytes = 20
      override val logFileSize = 30
    })

    // start both servers
    server1 = TestUtils.createServer(configs.head)
    server2 = TestUtils.createServer(configs.last)
    servers ++= List(server1, server2)

    hwFile1 = new HighwaterMarkCheckpoint(server1.config.logDir)
    hwFile2 = new HighwaterMarkCheckpoint(server2.config.logDir)

    val producerProps = getProducerConfig(zkConnect, 64*1024, 100000, 10000)
    producerProps.put("producer.request.timeout.ms", "1000")
    producerProps.put("producer.request.required.acks", "-1")
    producer = new Producer[Int, Message](new ProducerConfig(producerProps))

    // create topic with 1 partition, 2 replicas, one on each broker
    CreateTopicCommand.createTopic(zkClient, topic, 1, 2, configs.map(_.brokerId).mkString(":"))

    // wait until leader is elected
    var leader = waitUntilLeaderIsElected(zkClient, topic, partitionId, 500)
    assertTrue("Leader should get elected", leader.isDefined)
    // NOTE: this is to avoid transient test failures
    assertTrue("Leader could be broker 0 or broker 1", (leader.getOrElse(-1) == 0) || (leader.getOrElse(-1) == 1))
    sendMessages(20)
    // give some time for follower 1 to record leader HW of 600
    TestUtils.waitUntilTrue(() => server2.getReplica(topic, 0).get.highWatermark() == 600L, 500)
    // shutdown the servers to allow the hw to be checkpointed
    servers.map(server => server.shutdown())
    producer.close()
    val leaderHW = hwFile1.read(topic, 0)
    assertEquals(600L, leaderHW)
    val followerHW = hwFile2.read(topic, 0)
    assertEquals(600L, followerHW)
    servers.map(server => Utils.rm(server.config.logDir))
  }

  def testHWCheckpointWithFailuresMultipleLogSegments {
    val configs = TestUtils.createBrokerConfigs(2).map(new KafkaConfig(_) {
      override val replicaMaxLagTimeMs = 5000L
      override val replicaMaxLagBytes = 10L
      override val flushInterval = 1000
      override val replicaMinBytes = 20
      override val logFileSize = 30
    })

    // start both servers
    server1 = TestUtils.createServer(configs.head)
    server2 = TestUtils.createServer(configs.last)
    servers ++= List(server1, server2)

    hwFile1 = new HighwaterMarkCheckpoint(server1.config.logDir)
    hwFile2 = new HighwaterMarkCheckpoint(server2.config.logDir)

    val producerProps = getProducerConfig(zkConnect, 64*1024, 100000, 10000)
    producerProps.put("producer.request.timeout.ms", "1000")
    producerProps.put("producer.request.required.acks", "-1")
    producer = new Producer[Int, Message](new ProducerConfig(producerProps))

    // create topic with 1 partition, 2 replicas, one on each broker
    CreateTopicCommand.createTopic(zkClient, topic, 1, 2, configs.map(_.brokerId).mkString(":"))

    // wait until leader is elected
    var leader = waitUntilLeaderIsElected(zkClient, topic, partitionId, 500)
    assertTrue("Leader should get elected", leader.isDefined)
    // NOTE: this is to avoid transient test failures
    assertTrue("Leader could be broker 0 or broker 1", (leader.getOrElse(-1) == 0) || (leader.getOrElse(-1) == 1))

    sendMessages(2)
    // allow some time for the follower to get the leader HW
    TestUtils.waitUntilTrue(() => server2.getReplica(topic, 0).get.highWatermark() == 60L, 1000)
    // kill the server hosting the preferred replica
    server1.shutdown()
    server2.shutdown()
    assertEquals(60L, hwFile1.read(topic, 0))
    assertEquals(60L, hwFile2.read(topic, 0))

    server2.startup()
    // check if leader moves to the other server
    leader = waitUntilLeaderIsElected(zkClient, topic, partitionId, 500)
    assertEquals("Leader must move to broker 1", 1, leader.getOrElse(-1))

    assertEquals(60L, hwFile1.read(topic, 0))

    // bring the preferred replica back
    server1.startup()

    assertEquals(60L, hwFile1.read(topic, 0))
    assertEquals(60L, hwFile2.read(topic, 0))

    sendMessages(2)
    // allow some time for the follower to get the leader HW
    TestUtils.waitUntilTrue(() => server1.getReplica(topic, 0).get.highWatermark() == 120L, 1000)
    // shutdown the servers to allow the hw to be checkpointed
    servers.map(server => server.shutdown())
    producer.close()
    assertEquals(120L, hwFile1.read(topic, 0))
    assertEquals(120L, hwFile2.read(topic, 0))
    servers.map(server => Utils.rm(server.config.logDir))
  }

  private def sendMessages(numMessages: Int = 1) {
    for(i <- 0 until numMessages) {
      producer.send(new ProducerData[Int, Message](topic, 0, sent1))
    }
  }
}