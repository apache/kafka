/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package kafka.api

import java.util.concurrent._
import java.util.{Collection, Collections, Properties}

import kafka.server.{BaseRequestTest, KafkaConfig}
import kafka.utils.{CoreUtils, Logging, ShutdownableThread, TestUtils}
import org.apache.kafka.clients.consumer._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.{FindCoordinatorRequest, FindCoordinatorResponse}
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.junit.Assert._
import org.junit.{After, Before, Ignore, Test}

import scala.collection.JavaConverters._
import scala.collection.mutable.Buffer


/**
 * Integration tests for the consumer that cover basic usage as well as server failures
 */
class ConsumerBounceTest extends BaseRequestTest with Logging {

  override def numBrokers: Int = 3

  val producerCount = 1
  val consumerCount = 2

  val consumers = Buffer[KafkaConsumer[Array[Byte], Array[Byte]]]()
  val producers = Buffer[KafkaProducer[Array[Byte], Array[Byte]]]()

  val topic = "topic"
  val part = 0
  val tp = new TopicPartition(topic, part)

  // Time to process commit and leave group requests in tests when brokers are available
  val gracefulCloseTimeMs = 1000
  val executor = Executors.newScheduledThreadPool(2)

  val producerConfig = new Properties
  val consumerConfig = new Properties
  this.producerConfig.setProperty(ProducerConfig.ACKS_CONFIG, "all")
  this.consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-test")
  this.consumerConfig.setProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 4096.toString)
  this.consumerConfig.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000")
  this.consumerConfig.setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "3000")
  this.consumerConfig.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  def serverConfig(): Properties = {
    val properties = new Properties
    properties.put(KafkaConfig.OffsetsTopicReplicationFactorProp, "3") // don't want to lose offset
    properties.put(KafkaConfig.OffsetsTopicPartitionsProp, "1")
    properties.put(KafkaConfig.GroupMinSessionTimeoutMsProp, "10") // set small enough session timeout
    properties.put(KafkaConfig.GroupInitialRebalanceDelayMsProp, "0")
    properties.put(KafkaConfig.UncleanLeaderElectionEnableProp, "true")
    properties.put(KafkaConfig.AutoCreateTopicsEnableProp, "false")
    properties
  }

  override def generateConfigs = {
    FixedPortTestUtils.createBrokerConfigs(numBrokers, zkConnect, enableControlledShutdown = false)
      .map(KafkaConfig.fromProps(_, serverConfig))
  }

  @Before
  override def setUp() {
    super.setUp()

    for (_ <- 0 until producerCount)
      producers += createProducer

    for (_ <- 0 until consumerCount)
      consumers += createConsumer

    // create the test topic with all the brokers as replicas
    createTopic(topic, 1, numBrokers)
  }

  def createProducer: KafkaProducer[Array[Byte], Array[Byte]] = {
    TestUtils.createProducer(TestUtils.getBrokerListStrFromServers(servers),
        securityProtocol = SecurityProtocol.PLAINTEXT,
        props = Some(producerConfig))
  }

  def createConsumer: KafkaConsumer[Array[Byte], Array[Byte]] = {
    TestUtils.createConsumer(TestUtils.getBrokerListStrFromServers(servers),
        securityProtocol = SecurityProtocol.PLAINTEXT,
        props = Some(consumerConfig))
  }

  @After
  override def tearDown() {
    try {
      executor.shutdownNow()
      // Wait for any active tasks to terminate to ensure consumer is not closed while being used from another thread
      assertTrue("Executor did not terminate", executor.awaitTermination(5000, TimeUnit.MILLISECONDS))
      producers.foreach(_.close())
      consumers.foreach(_.close())
    } finally {
      super.tearDown()
    }
  }

  @Test
  @Ignore // To be re-enabled once we can make it less flaky (KAFKA-4801)
  def testConsumptionWithBrokerFailures() = consumeWithBrokerFailures(10)

  /*
   * 1. Produce a bunch of messages
   * 2. Then consume the messages while killing and restarting brokers at random
   */
  def consumeWithBrokerFailures(numIters: Int) {
    val numRecords = 1000
    sendRecords(numRecords)
    this.producers.foreach(_.close)

    var consumed = 0L
    val consumer = this.consumers.head

    consumer.subscribe(Collections.singletonList(topic))

    val scheduler = new BounceBrokerScheduler(numIters)
    scheduler.start()

    while (scheduler.isRunning) {
      val records = consumer.poll(100).asScala
      assertEquals(Set(tp), consumer.assignment.asScala)

      for (record <- records) {
        assertEquals(consumed, record.offset())
        consumed += 1
      }

      if (records.nonEmpty) {
        consumer.commitSync()
        assertEquals(consumer.position(tp), consumer.committed(tp).offset)

        if (consumer.position(tp) == numRecords) {
          consumer.seekToBeginning(Collections.emptyList())
          consumed = 0
        }
      }
    }
    scheduler.shutdown()
  }

  @Test
  def testSeekAndCommitWithBrokerFailures() = seekAndCommitWithBrokerFailures(5)

  def seekAndCommitWithBrokerFailures(numIters: Int) {
    val numRecords = 1000
    sendRecords(numRecords)
    this.producers.foreach(_.close)

    val consumer = this.consumers.head
    consumer.assign(Collections.singletonList(tp))
    consumer.seek(tp, 0)

    // wait until all the followers have synced the last HW with leader
    TestUtils.waitUntilTrue(() => servers.forall(server =>
      server.replicaManager.getReplica(tp).get.highWatermark.messageOffset == numRecords
    ), "Failed to update high watermark for followers after timeout")

    val scheduler = new BounceBrokerScheduler(numIters)
    scheduler.start()

    while(scheduler.isRunning) {
      val coin = TestUtils.random.nextInt(3)
      if (coin == 0) {
        info("Seeking to end of log")
        consumer.seekToEnd(Collections.emptyList())
        assertEquals(numRecords.toLong, consumer.position(tp))
      } else if (coin == 1) {
        val pos = TestUtils.random.nextInt(numRecords).toLong
        info("Seeking to " + pos)
        consumer.seek(tp, pos)
        assertEquals(pos, consumer.position(tp))
      } else if (coin == 2) {
        info("Committing offset.")
        consumer.commitSync()
        assertEquals(consumer.position(tp), consumer.committed(tp).offset)
      }
    }
  }

  @Test
  def testSubscribeWhenTopicUnavailable() {
    val numRecords = 1000
    val newtopic = "newtopic"

    val consumer = this.consumers.head
    consumer.subscribe(Collections.singleton(newtopic))
    executor.schedule(new Runnable {
        def run() = createTopic(newtopic, numPartitions = numBrokers, replicationFactor = numBrokers)
      }, 2, TimeUnit.SECONDS)
    consumer.poll(0)

    def sendRecords(numRecords: Int, topic: String) {
      var remainingRecords = numRecords
      val endTimeMs = System.currentTimeMillis + 20000
      while (remainingRecords > 0 && System.currentTimeMillis < endTimeMs) {
        val futures = (0 until remainingRecords).map { i =>
          this.producers.head.send(new ProducerRecord(topic, part, i.toString.getBytes, i.toString.getBytes))
        }
        futures.map { future =>
          try {
            future.get
            remainingRecords -= 1
          } catch {
            case _: Exception =>
          }
        }
      }
      assertEquals(0, remainingRecords)
    }

    sendRecords(numRecords, newtopic)
    receiveRecords(consumer, numRecords, newtopic, 10000)

    servers.foreach(server => killBroker(server.config.brokerId))
    Thread.sleep(500)
    restartDeadBrokers()

    val future = executor.submit(new Runnable {
      def run() = receiveRecords(consumer, numRecords, newtopic, 10000)
    })
    sendRecords(numRecords, newtopic)
    future.get
  }


  @Test
  def testClose() {
    val numRecords = 10
    sendRecords(numRecords)

    checkCloseGoodPath(numRecords, "group1")
    checkCloseWithCoordinatorFailure(numRecords, "group2", "group3")
    checkCloseWithClusterFailure(numRecords, "group4", "group5")
  }

  /**
   * Consumer is closed while cluster is healthy. Consumer should complete pending offset commits
   * and leave group. New consumer instance should be able join group and start consuming from
   * last committed offset.
   */
  private def checkCloseGoodPath(numRecords: Int, groupId: String) {
    val consumer = createConsumerAndReceive(groupId, false, numRecords)
    val future = submitCloseAndValidate(consumer, Long.MaxValue, None, Some(gracefulCloseTimeMs))
    future.get
    checkClosedState(groupId, numRecords)
  }

  /**
   * Consumer closed while coordinator is unavailable. Close of consumers using group
   * management should complete after commit attempt even though commits fail due to rebalance.
   * Close of consumers using manual assignment should complete with successful commits since a
   * broker is available.
   */
  private def checkCloseWithCoordinatorFailure(numRecords: Int, dynamicGroup: String, manualGroup: String) {
    val consumer1 = createConsumerAndReceive(dynamicGroup, false, numRecords)
    val consumer2 = createConsumerAndReceive(manualGroup, true, numRecords)

    killBroker(findCoordinator(dynamicGroup))
    killBroker(findCoordinator(manualGroup))

    val future1 = submitCloseAndValidate(consumer1, Long.MaxValue, None, Some(gracefulCloseTimeMs))
    val future2 = submitCloseAndValidate(consumer2, Long.MaxValue, None, Some(gracefulCloseTimeMs))
    future1.get
    future2.get

    restartDeadBrokers()
    checkClosedState(dynamicGroup, 0)
    checkClosedState(manualGroup, numRecords)
  }

  private def findCoordinator(group: String) : Int = {
    val request = new FindCoordinatorRequest.Builder(FindCoordinatorRequest.CoordinatorType.GROUP, group).build()
    val resp = connectAndSend(request, ApiKeys.FIND_COORDINATOR)
    val response = FindCoordinatorResponse.parse(resp, ApiKeys.FIND_COORDINATOR.latestVersion())
    response.node().id()
  }


  /**
   * Consumer is closed while all brokers are unavailable. Cannot rebalance or commit offsets since
   * there is no coordinator, but close should timeout and return. If close is invoked with a very
   * large timeout, close should timeout after request timeout.
   */
  private def checkCloseWithClusterFailure(numRecords: Int, group1: String, group2: String) {
    val consumer1 = createConsumerAndReceive(group1, false, numRecords)

    val requestTimeout = 6000
    this.consumerConfig.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "5000")
    this.consumerConfig.setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "1000")
    this.consumerConfig.setProperty(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeout.toString)
    val consumer2 = createConsumerAndReceive(group2, true, numRecords)

    servers.foreach(server => killBroker(server.config.brokerId))
    val closeTimeout = 2000
    val future1 = submitCloseAndValidate(consumer1, closeTimeout, Some(closeTimeout), Some(closeTimeout))
    val future2 = submitCloseAndValidate(consumer2, Long.MaxValue, Some(requestTimeout), Some(requestTimeout))
    future1.get
    future2.get
  }

  /**
   * Consumer is closed during rebalance. Close should leave group and close
   * immediately if rebalance is in progress. If brokers are not available,
   * close should terminate immediately without sending leave group.
   */
  @Test
  def testCloseDuringRebalance() {
    val topic = "closetest"
    createTopic(topic, 10, numBrokers)
    this.consumerConfig.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "60000")
    this.consumerConfig.setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "1000")
    this.consumerConfig.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    checkCloseDuringRebalance("group1", topic, executor, true)
  }

  private def checkCloseDuringRebalance(groupId: String, topic: String, executor: ExecutorService, brokersAvailableDuringClose: Boolean) {

    def subscribeAndPoll(consumer: KafkaConsumer[Array[Byte], Array[Byte]], revokeSemaphore: Option[Semaphore] = None): Future[Any] = {
      executor.submit(CoreUtils.runnable {
          consumer.subscribe(Collections.singletonList(topic), new ConsumerRebalanceListener {
            def onPartitionsAssigned(partitions: Collection[TopicPartition]) {
            }
            def onPartitionsRevoked(partitions: Collection[TopicPartition]) {
              revokeSemaphore.foreach(s => s.release())
            }
          })
          consumer.poll(0)
        }, 0)
    }

    def waitForRebalance(timeoutMs: Long, future: Future[Any], otherConsumers: KafkaConsumer[Array[Byte], Array[Byte]]*) {
      val startMs = System.currentTimeMillis
      while (System.currentTimeMillis < startMs + timeoutMs && !future.isDone)
          otherConsumers.foreach(consumer => consumer.poll(100))
      assertTrue("Rebalance did not complete in time", future.isDone)
    }

    def createConsumerToRebalance(): Future[Any] = {
      val consumer = createConsumer(groupId)
      val rebalanceSemaphore = new Semaphore(0)
      val future = subscribeAndPoll(consumer, Some(rebalanceSemaphore))
      // Wait for consumer to poll and trigger rebalance
      assertTrue("Rebalance not triggered", rebalanceSemaphore.tryAcquire(2000, TimeUnit.MILLISECONDS))
      // Rebalance is blocked by other consumers not polling
      assertFalse("Rebalance completed too early", future.isDone)
      future
    }

    val consumer1 = createConsumer(groupId)
    waitForRebalance(2000, subscribeAndPoll(consumer1))
    val consumer2 = createConsumer(groupId)
    waitForRebalance(2000, subscribeAndPoll(consumer2), consumer1)
    val rebalanceFuture = createConsumerToRebalance()

    // consumer1 should leave group and close immediately even though rebalance is in progress
    val closeFuture1 = submitCloseAndValidate(consumer1, Long.MaxValue, None, Some(gracefulCloseTimeMs))

    // Rebalance should complete without waiting for consumer1 to timeout since consumer1 has left the group
    waitForRebalance(2000, rebalanceFuture, consumer2)

    // Trigger another rebalance and shutdown all brokers
    // This consumer poll() doesn't complete and `tearDown` shuts down the executor and closes the consumer
    createConsumerToRebalance()
    servers.foreach(server => killBroker(server.config.brokerId))

    // consumer2 should close immediately without LeaveGroup request since there are no brokers available
    val closeFuture2 = submitCloseAndValidate(consumer2, Long.MaxValue, None, Some(0))

    // Ensure futures complete to avoid concurrent shutdown attempt during test cleanup
    closeFuture1.get(2000, TimeUnit.MILLISECONDS)
    closeFuture2.get(2000, TimeUnit.MILLISECONDS)
  }

  private def createConsumer(groupId: String) : KafkaConsumer[Array[Byte], Array[Byte]] = {
    this.consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    val consumer = createConsumer
    consumers += consumer
    consumer
  }

  private def createConsumerAndReceive(groupId: String, manualAssign: Boolean, numRecords: Int) : KafkaConsumer[Array[Byte], Array[Byte]] = {
    val consumer = createConsumer(groupId)
    if (manualAssign)
      consumer.assign(Collections.singleton(tp))
    else
      consumer.subscribe(Collections.singleton(topic))
    receiveRecords(consumer, numRecords)
    consumer
  }

  private def receiveRecords(consumer: KafkaConsumer[Array[Byte], Array[Byte]], numRecords: Int, topic: String = this.topic, timeoutMs: Long = 60000) {
    var received = 0L
    val endTimeMs = System.currentTimeMillis + timeoutMs
    while (received < numRecords && System.currentTimeMillis < endTimeMs)
      received += consumer.poll(1000).count()
    assertEquals(numRecords, received)
  }

  private def submitCloseAndValidate(consumer: KafkaConsumer[Array[Byte], Array[Byte]],
      closeTimeoutMs: Long, minCloseTimeMs: Option[Long], maxCloseTimeMs: Option[Long]): Future[Any] = {
    executor.submit(CoreUtils.runnable {
      val closeGraceTimeMs = 2000
      val startNanos = System.nanoTime
      info("Closing consumer with timeout " + closeTimeoutMs + " ms.")
      consumer.close(closeTimeoutMs, TimeUnit.MILLISECONDS)
      val timeTakenMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime - startNanos)
      maxCloseTimeMs.foreach { ms =>
        assertTrue("Close took too long " + timeTakenMs, timeTakenMs < ms + closeGraceTimeMs)
      }
      minCloseTimeMs.foreach { ms =>
        assertTrue("Close finished too quickly " + timeTakenMs, timeTakenMs >= ms)
      }
      info("consumer.close() completed in " + timeTakenMs + " ms.")
    }, 0)
  }

  private def checkClosedState(groupId: String, committedRecords: Int) {
    // Check that close was graceful with offsets committed and leave group sent.
    // New instance of consumer should be assigned partitions immediately and should see committed offsets.
    val assignSemaphore = new Semaphore(0)
    val consumer = createConsumer(groupId)
    consumer.subscribe(Collections.singletonList(topic),  new ConsumerRebalanceListener {
      def onPartitionsAssigned(partitions: Collection[TopicPartition]) {
        assignSemaphore.release()
      }
      def onPartitionsRevoked(partitions: Collection[TopicPartition]) {
      }})
    consumer.poll(3000)
    assertTrue("Assigment did not complete on time", assignSemaphore.tryAcquire(1, TimeUnit.SECONDS))
    if (committedRecords > 0)
      assertEquals(committedRecords, consumer.committed(tp).offset)
    consumer.close()
  }

  private class BounceBrokerScheduler(val numIters: Int) extends ShutdownableThread("daemon-bounce-broker", false)
  {
    var iter: Int = 0

    override def doWork(): Unit = {
      killRandomBroker()
      Thread.sleep(500)
      restartDeadBrokers()

      iter += 1
      if (iter == numIters)
        initiateShutdown()
      else
        Thread.sleep(500)
    }
  }

  private def sendRecords(numRecords: Int, topic: String = this.topic) {
    val futures = (0 until numRecords).map { i =>
      this.producers.head.send(new ProducerRecord(topic, part, i.toString.getBytes, i.toString.getBytes))
    }
    futures.map(_.get)
  }


}
