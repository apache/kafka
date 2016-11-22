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

import java.util.{Collection, Collections}
import java.util.concurrent.{Callable, Executors, ExecutorService, Future, Semaphore, TimeUnit}

import kafka.admin.AdminClient
import kafka.server.KafkaConfig
import kafka.utils.{Logging, ShutdownableThread, TestUtils}
import org.apache.kafka.clients.consumer._
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.junit.Assert._
import org.junit.{Before, Test}

import scala.collection.JavaConverters._


/**
 * Integration tests for the new consumer that cover basic usage as well as server failures
 */
class ConsumerBounceTest extends IntegrationTestHarness with Logging {

  val producerCount = 1
  val consumerCount = 2
  val serverCount = 3

  val topic = "topic"
  val part = 0
  val tp = new TopicPartition(topic, part)

  // configure the servers and clients
  this.serverConfig.setProperty(KafkaConfig.ControlledShutdownEnableProp, "false") // speed up shutdown
  this.serverConfig.setProperty(KafkaConfig.OffsetsTopicReplicationFactorProp, "3") // don't want to lose offset
  this.serverConfig.setProperty(KafkaConfig.OffsetsTopicPartitionsProp, "1")
  this.serverConfig.setProperty(KafkaConfig.GroupMinSessionTimeoutMsProp, "10") // set small enough session timeout
  this.producerConfig.setProperty(ProducerConfig.ACKS_CONFIG, "all")
  this.consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-test")
  this.consumerConfig.setProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 4096.toString)
  this.consumerConfig.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000")
  this.consumerConfig.setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "3000")
  this.consumerConfig.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  override def generateConfigs() = {
    FixedPortTestUtils.createBrokerConfigs(serverCount, zkConnect,enableControlledShutdown = false)
      .map(KafkaConfig.fromProps(_, serverConfig))
  }

  @Before
  override def setUp() {
    super.setUp()

    // create the test topic with all the brokers as replicas
    TestUtils.createTopic(this.zkUtils, topic, 1, serverCount, this.servers)
  }

  @Test
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

    while (scheduler.isRunning.get()) {
      for (record <- consumer.poll(100).asScala) {
        assertEquals(consumed, record.offset())
        consumed += 1
      }

      try {
        consumer.commitSync()
        assertEquals(consumer.position(tp), consumer.committed(tp).offset)

        if (consumer.position(tp) == numRecords) {
          consumer.seekToBeginning(Collections.emptyList())
          consumed = 0
        }
      } catch {
        // TODO: should be no need to catch these exceptions once KAFKA-2017 is
        // merged since coordinator fail-over will not cause a rebalance
        case _: CommitFailedException =>
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

    while(scheduler.isRunning.get()) {
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

  /**
   * Consumer is closed while cluster is healthy. Consumer should complete pending offset commits
   * and leave group. New consumer instance should be able join group and start consuming from
   * last committed offset.
   */
  @Test
  def testClose() {
    val numRecords = 10
    sendRecords(numRecords)
    val consumer = this.consumers.remove(0)
    consumer.subscribe(Collections.singletonList(topic))
    receiveRecords(consumer, numRecords)
    closeAndValidate(consumer, Long.MaxValue, None, Some(1000))
    checkClosedState(consumers.head, numRecords)
  }

  /**
   * Consumer using group management is closed while coordinator is unavailable.
   * Close should complete after commit attempt even though commits fail due to rebalance.
   */
  @Test
  def testCloseWithCoordinatorFailureUsingGroupManagement() {
    val numRecords = 10
    sendRecords(numRecords)
    val consumer = this.consumers.remove(0)
    consumer.assign(Collections.singletonList(tp))
    receiveRecords(consumer, numRecords)
    val adminClient = AdminClient.createSimplePlaintext(this.brokerList)
    val coordinator = adminClient.findCoordinator("my-test").id
    killBroker(coordinator)
    closeAndValidate(consumer, Long.MaxValue, None, Some(1000))
    checkClosedState(consumers.head, 0)
  }

  /**
   * Consumer using manual partition assignment is closed while coordinator is unavailable,
   * but other brokers are available. Close should complete gracefully with successful commits
   * for manual partition assignment.
   */
  @Test
  def testCloseWithCoordinatorFailureUsingManualAssignment() {
    val numRecords = 10
    sendRecords(numRecords)
    val consumer = this.consumers.remove(0)
    consumer.assign(Collections.singletonList(tp))
    receiveRecords(consumer, numRecords)
    val adminClient = AdminClient.createSimplePlaintext(this.brokerList)
    val coordinator = adminClient.findCoordinator("my-test").id
    killBroker(coordinator)
    closeAndValidate(consumer, Long.MaxValue, None, Some(1000))
    checkClosedState(consumers.head, numRecords)
  }

  /**
   * Consumer is closed with a small timeout while all brokers are unavailable.
   * Cannot rebalance or commit offsets since there is no coordinator, but close should
   * timeout and return.
   */
  @Test
  def testCloseTimeoutWithClusterFailure() {
    val numRecords = 10
    sendRecords(numRecords)
    val consumer = this.consumers.remove(0)
    consumer.subscribe(Collections.singletonList(topic))
    receiveRecords(consumer, numRecords)
    servers.foreach(server => killBroker(server.config.brokerId))
    val closeTimeout = 2000
    closeAndValidate(consumer, closeTimeout, Some(closeTimeout), Some(closeTimeout))
  }

  /**
   * Consumer is closed without a timeout while all brokers are unavailable.
   * Cannot commit offsets since there are no brokers, but close should
   * terminate on request timeout after attempting to commit offsets even
   * though offsets cannot be successfully committed.
   */
  @Test
  def testCloseNoTimeoutWithClusterFailure() {
    val numRecords = 10
    sendRecords(numRecords)
    val requestTimeout = 6000
    this.consumerConfig.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "5000")
    this.consumerConfig.setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "1000")
    this.consumerConfig.setProperty(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeout.toString)
    val consumer = createNewConsumer
    consumer.assign(Collections.singletonList(tp))
    receiveRecords(consumer, numRecords)
    servers.foreach(server => killBroker(server.config.brokerId))
    closeAndValidate(consumer, Long.MaxValue, Some(requestTimeout), Some(requestTimeout))
  }

  /**
   * Consumer is closed during rebalance. Close should leave group and close
   * immediately if rebalance is in progress.
   */
  @Test
  def testCloseDuringRebalance() {
    val executor = Executors.newSingleThreadExecutor
    try {
      closeDuringRebalance(executor, true)
    } finally {
      executor.shutdownNow()
    }
  }

  /**
   * Consumer closed during rebalance when brokers are not available.
   * Close should terminate immediately since coordinator not known.
   */
  @Test
  def testCloseDuringRebalanceBrokersUnavailable() {
    val executor = Executors.newSingleThreadExecutor
    try {
      closeDuringRebalance(executor, true)
    } finally {
      executor.shutdownNow()
    }
  }

  private def closeDuringRebalance(executor: ExecutorService, brokersAvailableDuringClose: Boolean) {
    val topic = "closetest"
    TestUtils.createTopic(this.zkUtils, topic, 10, serverCount, this.servers)
    this.consumerConfig.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "60000")
    this.consumerConfig.setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "1000")

    def subscribeAndPoll(consumer: KafkaConsumer[Array[Byte], Array[Byte]]): Future[Any] = {
      executor.submit(new Runnable() {
        def run() {
          consumer.subscribe(Collections.singletonList(topic))
          consumer.poll(0)
        }}, 0)
    }

    def waitForRebalance(consumer: KafkaConsumer[Array[Byte], Array[Byte]], future: Future[Any], timeoutMs: Long) {
      val startMs1 = System.currentTimeMillis
      while (System.currentTimeMillis < startMs1 + timeoutMs && !future.isDone)
          consumer.poll(1000)
      assertTrue("Rebalance did not complete in time", future.isDone)
    }

    val consumer1 = createNewConsumer
    val future1 = subscribeAndPoll(consumer1)
    future1.get(2000, TimeUnit.MILLISECONDS)

    val consumer2 = createNewConsumer
    val future2 = subscribeAndPoll(consumer2)
    waitForRebalance(consumer1, future2, 2000)

    val consumer3 = createNewConsumer
    val future3 = subscribeAndPoll(consumer3)
    // Wait for consumer3 to poll and trigger rebalance
    Thread.sleep(2000)
    // Rebalance is blocked by consumer2 not polling
    assertFalse(future3.isDone)

    if (!brokersAvailableDuringClose)
      servers.foreach(server => killBroker(server.config.brokerId))

    // consumer1 should leave group and close immediately even though rebalance is in progress
    closeAndValidate(consumer1, Long.MaxValue, None, Some(1000))
    restartDeadBrokers()

    // Rebalance should complete without waiting for consumer1 to timeout since consumer1 has left the group
    waitForRebalance(consumer2, future3, 2000)
  }

  /**
   * Close consumer while brokers are being shutdown and restarted:
   * Close must complete or timeout
   */
  @Test
  def testCloseWithRandomBrokerFailures() {
    val scheduler = new BounceBrokerScheduler(10)
    scheduler.start()
    sendRecords(10)

    while(scheduler.isRunning.get()) {
      val consumer = createNewConsumer
      consumer.subscribe(Collections.singletonList(topic))
      consumer.poll(100)
      closeAndValidate(consumer, 1000, None, Some(1000))
    }
  }

  private def receiveRecords(consumer: KafkaConsumer[Array[Byte], Array[Byte]], numRecords: Int) {
    var received = 0
    while (received < numRecords)
      received += consumer.poll(1000).count()
  }

  private def closeAndValidate(consumer: KafkaConsumer[Array[Byte], Array[Byte]],
      closeTimeoutMs: Long, minCloseTimeMs: Option[Long], maxCloseTimeMs: Option[Long]) {
    val closeGraceTimeMs = 2000
    val startNanos = System.nanoTime
    info("Closing consumer with timeout " + closeTimeoutMs + " ms.")
    consumer.close(closeTimeoutMs, TimeUnit.MILLISECONDS)
    val timeTakenMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime - startNanos)
    maxCloseTimeMs match {
      case Some(ms) => assertTrue("Close took too long " + timeTakenMs, timeTakenMs < ms + closeGraceTimeMs)
      case None =>
    }
    minCloseTimeMs match {
      case Some(ms) => assertTrue("Close finished too quickly " + timeTakenMs, timeTakenMs >= ms)
      case None =>
    }
    info("consumer.close() completed in " + timeTakenMs + " ms.")
  }

  private def checkClosedState(consumer: KafkaConsumer[Array[Byte], Array[Byte]], committedRecords: Int) {
    // Check that close was graceful with offsets committed and leave group sent.
    // New instance of consumer should be assigned partitions immediately and should see committed offsets.
    val assignSemaphore = new Semaphore(0)
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

  private def sendRecords(numRecords: Int) {
    val futures = (0 until numRecords).map { i =>
      this.producers.head.send(new ProducerRecord(topic, part, i.toString.getBytes, i.toString.getBytes))
    }
    futures.map(_.get)
  }


}
