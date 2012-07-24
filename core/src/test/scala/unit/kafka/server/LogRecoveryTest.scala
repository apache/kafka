package kafka.server

import org.scalatest.junit.JUnit3Suite
import org.junit.Assert._
import kafka.admin.CreateTopicCommand
import kafka.utils.TestUtils._
import kafka.utils.{Utils, TestUtils}
import kafka.zk.ZooKeeperTestHarness
import kafka.message.Message
import java.io.RandomAccessFile
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

  val server1HWFile = configProps1.logDir + "/" + topic + "-0/highwatermark"
  val server2HWFile = configProps2.logDir + "/" + topic + "-0/highwatermark"

  val sent1 = List(new Message("hello".getBytes()), new Message("there".getBytes()))
  val sent2 = List( new Message("more".getBytes()), new Message("messages".getBytes()))

  var producer: Producer[Int, Message] = null
  var hwFile1: RandomAccessFile = null
  var hwFile2: RandomAccessFile = null
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

    sendMessages()

    hwFile1 = new RandomAccessFile(server1HWFile, "r")
    hwFile2 = new RandomAccessFile(server2HWFile, "r")

    sendMessages()
    // don't wait for follower to read the leader's hw
    // shutdown the servers to allow the hw to be checkpointed
    servers.map(server => server.shutdown())
    producer.close()
    val leaderHW = readHW(hwFile1)
    assertEquals(60L, leaderHW)
    val followerHW = readHW(hwFile2)
    assertEquals(30L, followerHW)
    hwFile1.close()
    hwFile2.close()
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

    hwFile1 = new RandomAccessFile(server1HWFile, "r")
    hwFile2 = new RandomAccessFile(server2HWFile, "r")

    assertEquals(0L, readHW(hwFile1))

    sendMessages()

    // kill the server hosting the preferred replica
    server1.shutdown()
    assertEquals(30L, readHW(hwFile1))

    // check if leader moves to the other server
    leader = waitUntilLeaderIsElected(zkClient, topic, partitionId, 500)
    assertEquals("Leader must move to broker 1", 1, leader.getOrElse(-1))

    // bring the preferred replica back
    server1.startup()

    leader = waitUntilLeaderIsElected(zkClient, topic, partitionId, 500)
    assertEquals("Leader must remain on broker 1", 1, leader.getOrElse(-1))

    assertEquals(30L, readHW(hwFile1))
    // since server 2 was never shut down, the hw value of 30 is probably not checkpointed to disk yet
    server2.shutdown()
    assertEquals(30L, readHW(hwFile2))

    server2.startup()
    leader = waitUntilLeaderIsElected(zkClient, topic, partitionId, 500)
    assertEquals("Leader must remain on broker 0", 0, leader.getOrElse(-1))

    sendMessages()
    // give some time for follower 1 to record leader HW of 60
    Thread.sleep(500)
    // shutdown the servers to allow the hw to be checkpointed
    servers.map(server => server.shutdown())
    Thread.sleep(200)
    producer.close()
    assert(hwFile1.length() > 0)
    assert(hwFile2.length() > 0)
    assertEquals(60L, readHW(hwFile1))
    assertEquals(60L, readHW(hwFile2))
    hwFile1.close()
    hwFile2.close()
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

    val server1HWFile = configs.head.logDir + "/" + topic + "-0/highwatermark"
    val server2HWFile = configs.last.logDir + "/" + topic + "-0/highwatermark"

    // start both servers
    server1 = TestUtils.createServer(configs.head)
    server2 = TestUtils.createServer(configs.last)
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

    sendMessages(10)

    hwFile1 = new RandomAccessFile(server1HWFile, "r")
    hwFile2 = new RandomAccessFile(server2HWFile, "r")

    sendMessages(10)

    // give some time for follower 1 to record leader HW of 600
    Thread.sleep(500)
    // shutdown the servers to allow the hw to be checkpointed
    servers.map(server => server.shutdown())
    producer.close()
    val leaderHW = readHW(hwFile1)
    assertEquals(600L, leaderHW)
    val followerHW = readHW(hwFile2)
    assertEquals(600L, followerHW)
    hwFile1.close()
    hwFile2.close()
    servers.map(server => Utils.rm(server.config.logDir))
  }

  def testHWCheckpointWithFailuresMultipleLogSegments {
    val configs = TestUtils.createBrokerConfigs(2).map(new KafkaConfig(_) {
      override val replicaMaxLagTimeMs = 5000L
      override val replicaMaxLagBytes = 10L
      override val flushInterval = 1000
      override val flushSchedulerThreadRate = 10
      override val replicaMinBytes = 20
      override val logFileSize = 30
    })

    val server1HWFile = configs.head.logDir + "/" + topic + "-0/highwatermark"
    val server2HWFile = configs.last.logDir + "/" + topic + "-0/highwatermark"

    // start both servers
    server1 = TestUtils.createServer(configs.head)
    server2 = TestUtils.createServer(configs.last)
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

    val hwFile1 = new RandomAccessFile(server1HWFile, "r")
    val hwFile2 = new RandomAccessFile(server2HWFile, "r")

    sendMessages(2)
    // allow some time for the follower to get the leader HW
    Thread.sleep(1000)
    // kill the server hosting the preferred replica
    server1.shutdown()
    server2.shutdown()
    assertEquals(60L, readHW(hwFile1))
    assertEquals(60L, readHW(hwFile2))

    server2.startup()
    // check if leader moves to the other server
    leader = waitUntilLeaderIsElected(zkClient, topic, partitionId, 500)
    assertEquals("Leader must move to broker 1", 1, leader.getOrElse(-1))

    assertEquals(60L, readHW(hwFile1))

    // bring the preferred replica back
    server1.startup()

    assertEquals(60L, readHW(hwFile1))
    assertEquals(60L, readHW(hwFile2))

    sendMessages(2)
    // allow some time for the follower to get the leader HW
    Thread.sleep(1000)
    // shutdown the servers to allow the hw to be checkpointed
    servers.map(server => server.shutdown())
    producer.close()
    assert(hwFile1.length() > 0)
    assert(hwFile2.length() > 0)
    assertEquals(120L, readHW(hwFile1))
    assertEquals(120L, readHW(hwFile2))
    hwFile1.close()
    hwFile2.close()
    servers.map(server => Utils.rm(server.config.logDir))
  }

  private def sendMessages(numMessages: Int = 1) {
    for(i <- 0 until numMessages) {
      producer.send(new ProducerData[Int, Message](topic, 0, sent1))
    }
  }

  private def readHW(hwFile: RandomAccessFile): Long = {
    hwFile.seek(0)
    hwFile.readLong()
  }
}