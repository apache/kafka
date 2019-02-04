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

import java.time
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock
import java.util.{Collection, Collections, Properties}

import util.control.Breaks._
import kafka.server.{BaseRequestTest, KafkaConfig}
import kafka.utils.{CoreUtils, Logging, ShutdownableThread, TestUtils}
import org.apache.kafka.clients.consumer._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.GroupMaxSizeReachedException
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.{FindCoordinatorRequest, FindCoordinatorResponse}
import org.junit.Assert._
import org.junit.{After, Before, Ignore, Test}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future => SFuture}

/**
 * Integration tests for the consumer that cover basic usage as well as server failures
 */
class ConsumerBounceTest extends BaseRequestTest with Logging {
  val topic = "topic"
  val part = 0
  val tp = new TopicPartition(topic, part)
  val maxGroupSize = 5

  // Time to process commit and leave group requests in tests when brokers are available
  val gracefulCloseTimeMs = 1000
  val executor = Executors.newScheduledThreadPool(2)

  override def generateConfigs = {
    generateKafkaConfigs()
  }

  private def generateKafkaConfigs(maxGroupSize: String = maxGroupSize.toString): Seq[KafkaConfig] = {
    val properties = new Properties
    properties.put(KafkaConfig.OffsetsTopicReplicationFactorProp, "3") // don't want to lose offset
    properties.put(KafkaConfig.OffsetsTopicPartitionsProp, "1")
    properties.put(KafkaConfig.GroupMinSessionTimeoutMsProp, "10") // set small enough session timeout
    properties.put(KafkaConfig.GroupInitialRebalanceDelayMsProp, "0")
    properties.put(KafkaConfig.GroupMaxSizeProp, maxGroupSize)
    properties.put(KafkaConfig.UncleanLeaderElectionEnableProp, "true")
    properties.put(KafkaConfig.AutoCreateTopicsEnableProp, "false")

    FixedPortTestUtils.createBrokerConfigs(numBrokers, zkConnect, enableControlledShutdown = false)
      .map(KafkaConfig.fromProps(_, properties))
  }

  @Before
  override def setUp() {
    super.setUp()

    // create the test topic with all the brokers as replicas
    createTopic(topic, 1, numBrokers)
  }

  @After
  override def tearDown() {
    try {
      executor.shutdownNow()
      // Wait for any active tasks to terminate to ensure consumer is not closed while being used from another thread
      assertTrue("Executor did not terminate", executor.awaitTermination(5000, TimeUnit.MILLISECONDS))
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
    val producer = createProducer()
    sendRecords(producer, numRecords)

    var consumed = 0L
    val consumer = createConsumer()

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
    val producer = createProducer()
    sendRecords(producer, numRecords)

    val consumer = createConsumer()
    consumer.assign(Collections.singletonList(tp))
    consumer.seek(tp, 0)

    // wait until all the followers have synced the last HW with leader
    TestUtils.waitUntilTrue(() => servers.forall(server =>
      server.replicaManager.localReplica(tp).get.highWatermark.messageOffset == numRecords
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

    val consumer = createConsumer()
    consumer.subscribe(Collections.singleton(newtopic))
    executor.schedule(new Runnable {
        def run() = createTopic(newtopic, numPartitions = numBrokers, replicationFactor = numBrokers)
      }, 2, TimeUnit.SECONDS)
    consumer.poll(0)

    val producer = createProducer()

    def sendRecords(numRecords: Int, topic: String) {
      var remainingRecords = numRecords
      val endTimeMs = System.currentTimeMillis + 20000
      while (remainingRecords > 0 && System.currentTimeMillis < endTimeMs) {
        val futures = (0 until remainingRecords).map { i =>
          producer.send(new ProducerRecord(topic, part, i.toString.getBytes, i.toString.getBytes))
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
    receiveRecords(consumer, numRecords, 10000)

    servers.foreach(server => killBroker(server.config.brokerId))
    Thread.sleep(500)
    restartDeadBrokers()

    val future = executor.submit(new Runnable {
      def run() = receiveRecords(consumer, numRecords, 10000)
    })
    sendRecords(numRecords, newtopic)
    future.get
  }

  @Test
  def testClose() {
    val numRecords = 10
    val producer = createProducer()
    sendRecords(producer, numRecords)

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
    * If we have a running consumer group of size N, configure consumer.group.max.size = N-1 and restart all brokers,
    * the group should be forced to rebalance when it becomes hosted on a Coordinator with the new config.
    * Then, 1 consumer should be left out of the group.
    */
  @Test
  def testRollingBrokerRestartsWithSmallerMaxGroupSizeConfigDisruptsBigGroup(): Unit = {
    val topic = "group-max-size-test"
    val maxGroupSize = 2
    val consumerCount = maxGroupSize + 1
    var recordsProduced = maxGroupSize * 100
    val partitionCount = consumerCount * 2
    if (recordsProduced % partitionCount != 0) {
      // ensure even record distribution per partition
      recordsProduced += partitionCount - recordsProduced % partitionCount
    }
    val executor = Executors.newScheduledThreadPool(consumerCount * 2)
    this.consumerConfig.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "60000")
    this.consumerConfig.setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "1000")
    this.consumerConfig.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    val producer = createProducer()
    createTopic(topic, numPartitions = partitionCount, replicationFactor = numBrokers)
    val stableConsumers = createConsumersWithGroupId("group2", consumerCount, executor, topic = topic)

    // assert group is stable and working
    sendRecords(producer, recordsProduced, topic, numPartitions = Some(partitionCount))
    stableConsumers.foreach { cons => {
      receiveAndCommit(cons, recordsProduced / consumerCount, 10000)
    }}

    // roll all brokers with a lesser max group size to make sure coordinator has the new config
    val newConfigs = generateKafkaConfigs(maxGroupSize.toString)
    val kickedConsumerOut = new AtomicBoolean(false)
    var kickedOutConsumerIdx: Option[Int] = None
    val lock = new ReentrantLock
    // restart brokers until the group moves to a Coordinator with the new config
    breakable { for (broker <- servers.indices) {
      killBroker(broker)
      sendRecords(producer, recordsProduced, topic, numPartitions = Some(partitionCount))

      var successfulConsumes = 0

      // compute consumptions in a non-blocking way in order to account for the rebalance once the group.size takes effect
      val consumeFutures = new ArrayBuffer[SFuture[Any]]
      implicit val executorContext: ExecutionContextExecutor = ExecutionContext.fromExecutor(executor)
      stableConsumers.indices.foreach(idx => {
        val currentConsumer = stableConsumers(idx)
        val consumeFuture = SFuture {
          try {
            receiveAndCommit(currentConsumer, recordsProduced / consumerCount, 10000)
            CoreUtils.inLock(lock) { successfulConsumes += 1 }
          } catch {
            case e: Throwable =>
              if (!e.isInstanceOf[GroupMaxSizeReachedException]) {
                throw e
              }
              if (!kickedConsumerOut.compareAndSet(false, true)) {
                fail(s"Received more than one ${classOf[GroupMaxSizeReachedException]}")
              }
              kickedOutConsumerIdx = Some(idx)
          }
        }

        consumeFutures += consumeFuture
      })
      Await.result(SFuture.sequence(consumeFutures), Duration("12sec"))

      if (kickedConsumerOut.get()) {
        // validate the rest N-1 consumers consumed successfully
        assertEquals(maxGroupSize, successfulConsumes)
        break
      }

      val config = newConfigs(broker)
      servers(broker) = TestUtils.createServer(config, time = brokerTime(config.brokerId))
      restartDeadBrokers()
    }}
    if (!kickedConsumerOut.get())
      fail(s"Should have received an ${classOf[GroupMaxSizeReachedException]} during the cluster roll")

    // assert that the group has gone through a rebalance and shed off one consumer
    stableConsumers.remove(kickedOutConsumerIdx.get)
    sendRecords(producer, recordsProduced, topic, numPartitions = Some(partitionCount))
    // should be only maxGroupSize consumers left in the group
    stableConsumers.foreach { cons => {
      receiveAndCommit(cons, recordsProduced / maxGroupSize, 10000)
    }}
  }

  /**
    * When we have the consumer group max size configured to X, the X+1th consumer trying to join should receive a fatal exception
    */
  @Test
  def testConsumerReceivesFatalExceptionWhenGroupPassesMaxSize(): Unit = {
    val topic = "group-max-size-test"
    val groupId = "group1"
    val executor = Executors.newScheduledThreadPool(maxGroupSize * 2)
    createTopic(topic, maxGroupSize, numBrokers)
    this.consumerConfig.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "60000")
    this.consumerConfig.setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "1000")
    this.consumerConfig.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")

    // Create N+1 consumers in the same consumer group and assert that the N+1th consumer receives a fatal error when it tries to join the group
    val stableConsumers = createConsumersWithGroupId(groupId, maxGroupSize, executor, topic)
    val newConsumer = createConsumerWithGroupId(groupId)
    var failedRebalance = false
    var exception: Exception = null
    waitForRebalance(5000, subscribeAndPoll(newConsumer, executor = executor, onException = e => {failedRebalance = true; exception = e}),
      executor = executor, stableConsumers:_*)
    assertTrue("Rebalance did not fail as expected", failedRebalance)
    assertTrue(exception.isInstanceOf[GroupMaxSizeReachedException])

    // assert group continues to live
    val producer = createProducer()
    sendRecords(producer, maxGroupSize * 100, topic, numPartitions = Some(maxGroupSize))
    stableConsumers.foreach { cons => {
        receiveExactRecords(cons, 100, 10000)
    }}
  }

  /**
    * Creates N consumers with the same group ID and ensures the group rebalances properly at each step
    */
  private def createConsumersWithGroupId(groupId: String, consumerCount: Int, executor: ExecutorService, topic: String = topic): ArrayBuffer[KafkaConsumer[Array[Byte], Array[Byte]]] = {
    val stableConsumers = ArrayBuffer[KafkaConsumer[Array[Byte], Array[Byte]]]()
    for (_ <- 1.to(consumerCount)) {
      val newConsumer = createConsumerWithGroupId(groupId)
      waitForRebalance(5000, subscribeAndPoll(newConsumer, executor = executor, topic = topic),
        executor = executor, stableConsumers:_*)
      stableConsumers += newConsumer
    }
    stableConsumers
  }

  def subscribeAndPoll(consumer: KafkaConsumer[Array[Byte], Array[Byte]], executor: ExecutorService, revokeSemaphore: Option[Semaphore] = None,
                       onException: Exception => Unit = e => { throw e }, topic: String = topic, pollTimeout: Int = 1000): Future[Any] = {
    executor.submit(CoreUtils.runnable {
      try {
        consumer.subscribe(Collections.singletonList(topic))
        consumer.poll(java.time.Duration.ofMillis(pollTimeout))
      } catch {
        case e: Exception => onException.apply(e)
      }
    }, 0)
  }

  def waitForRebalance(timeoutMs: Long, future: Future[Any], executor: ExecutorService, otherConsumers: KafkaConsumer[Array[Byte], Array[Byte]]*) {
    val startMs = System.currentTimeMillis
    implicit val executorContext: ExecutionContextExecutor = ExecutionContext.fromExecutor(executor)

    while (System.currentTimeMillis < startMs + timeoutMs && !future.isDone) {
      val consumeFutures = otherConsumers.map(consumer => SFuture {
        consumer.poll(time.Duration.ofMillis(1000))
      })
      Await.result(SFuture.sequence(consumeFutures), Duration("1500ms"))
    }

    assertTrue("Rebalance did not complete in time", future.isDone)
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
      val consumer = createConsumerWithGroupId(groupId)
      val rebalanceSemaphore = new Semaphore(0)
      val future = subscribeAndPoll(consumer, Some(rebalanceSemaphore))
      // Wait for consumer to poll and trigger rebalance
      assertTrue("Rebalance not triggered", rebalanceSemaphore.tryAcquire(2000, TimeUnit.MILLISECONDS))
      // Rebalance is blocked by other consumers not polling
      assertFalse("Rebalance completed too early", future.isDone)
      future
    }
    val consumer1 = createConsumerWithGroupId(groupId)
    waitForRebalance(2000, subscribeAndPoll(consumer1))
    val consumer2 = createConsumerWithGroupId(groupId)
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

  private def createConsumerWithGroupId(groupId: String): KafkaConsumer[Array[Byte], Array[Byte]] = {
    consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    createConsumer()
  }

  private def createConsumerAndReceive(groupId: String, manualAssign: Boolean, numRecords: Int): KafkaConsumer[Array[Byte], Array[Byte]] = {
    val consumer = createConsumerWithGroupId(groupId)
    if (manualAssign)
      consumer.assign(Collections.singleton(tp))
    else
      consumer.subscribe(Collections.singleton(topic))
    receiveExactRecords(consumer, numRecords)
    consumer
  }

  private def receiveRecords(consumer: KafkaConsumer[Array[Byte], Array[Byte]], numRecords: Int, timeoutMs: Long = 60000): Long = {
    var received = 0L
    val endTimeMs = System.currentTimeMillis + timeoutMs
    while (received < numRecords && System.currentTimeMillis < endTimeMs)
      received += consumer.poll(time.Duration.ofMillis(100)).count()

    received
  }

  private def receiveExactRecords(consumer: KafkaConsumer[Array[Byte], Array[Byte]], numRecords: Int, timeoutMs: Long = 60000): Unit = {
    val received = receiveRecords(consumer, numRecords, timeoutMs)
    assertEquals(numRecords, received)
  }

  @throws(classOf[CommitFailedException])
  private def receiveAndCommit(consumer: KafkaConsumer[Array[Byte], Array[Byte]], numRecords: Int, timeoutMs: Long): Unit = {
    val received = receiveRecords(consumer, numRecords, timeoutMs)
    assertTrue(s"Received $received, expected at least $numRecords", numRecords <= received)
    consumer.commitSync()
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
    val consumer = createConsumerWithGroupId(groupId)
    consumer.subscribe(Collections.singletonList(topic),  new ConsumerRebalanceListener {
      def onPartitionsAssigned(partitions: Collection[TopicPartition]) {
        assignSemaphore.release()
      }
      def onPartitionsRevoked(partitions: Collection[TopicPartition]) {
      }})
    consumer.poll(3000)
    assertTrue("Assignment did not complete on time", assignSemaphore.tryAcquire(1, TimeUnit.SECONDS))
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

  private def sendRecords(producer: KafkaProducer[Array[Byte], Array[Byte]],
                          numRecords: Int,
                          topic: String = this.topic,
                          numPartitions: Option[Int] = None) {
    var partitionIndex = 0
    def getPartition: Int = {
      numPartitions match {
        case Some(partitions) =>
          val nextPart = partitionIndex % partitions
          partitionIndex += 1
          nextPart
        case None => part
      }
    }

    val futures = (0 until numRecords).map { i =>
      producer.send(new ProducerRecord(topic, getPartition, i.toString.getBytes, i.toString.getBytes))
    }
    futures.map(_.get)
  }

}
