/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import java.io.File
import java.util.{Optional, Properties}
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import kafka.log.{Log, LogConfig, LogManager, ProducerStateManager}
import kafka.utils.{MockScheduler, MockTime, TestUtils}
import TestUtils.createBroker
import kafka.cluster.BrokerEndPoint
import kafka.server.epoch.util.ReplicaFetcherMockBlockingSend
import kafka.utils.timer.MockTimer
import kafka.zk.KafkaZkClient
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record._
import org.apache.kafka.common.requests.{EpochEndOffset, IsolationLevel, LeaderAndIsrRequest}
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.requests.FetchRequest.PartitionData
import org.apache.kafka.common.requests.FetchResponse.AbortedTransaction
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.{Node, TopicPartition}
import org.apache.zookeeper.data.Stat
import org.easymock.EasyMock
import org.junit.Assert._
import org.junit.{After, Before, Test}

import scala.collection.JavaConverters._
import scala.collection.Map

class ReplicaManagerTest {

  val topic = "test-topic"
  val time = new MockTime
  val metrics = new Metrics
  var zkClient: ZkClient = _
  var kafkaZkClient: KafkaZkClient = _

  // Constants defined for readability
  val zkVersion = 0
  val correlationId = 0
  var controllerEpoch = 0
  val brokerEpoch = 0L

  @Before
  def setUp() {
    zkClient = EasyMock.createMock(classOf[ZkClient])
    kafkaZkClient = EasyMock.createMock(classOf[KafkaZkClient])
    EasyMock.expect(kafkaZkClient.getEntityConfigs(EasyMock.anyString(), EasyMock.anyString())).andReturn(new Properties()).anyTimes()
    EasyMock.replay(kafkaZkClient)
    EasyMock.expect(zkClient.readData(EasyMock.anyString(), EasyMock.anyObject[Stat])).andReturn(null).anyTimes()
    EasyMock.replay(zkClient)
  }

  @After
  def tearDown() {
    metrics.close()
  }

  @Test
  def testHighWaterMarkDirectoryMapping() {
    val props = TestUtils.createBrokerConfig(1, TestUtils.MockZkConnect)
    val config = KafkaConfig.fromProps(props)
    val mockLogMgr = TestUtils.createLogManager(config.logDirs.map(new File(_)))
    val rm = new ReplicaManager(config, metrics, time, kafkaZkClient, new MockScheduler(time), mockLogMgr,
      new AtomicBoolean(false), QuotaFactory.instantiate(config, metrics, time, ""), new BrokerTopicStats,
      new MetadataCache(config.brokerId), new LogDirFailureChannel(config.logDirs.size))
    try {
      val partition = rm.getOrCreatePartition(new TopicPartition(topic, 1))
      partition.getOrCreateReplica(1)
      rm.checkpointHighWatermarks()
    } finally {
      // shutdown the replica manager upon test completion
      rm.shutdown(false)
    }
  }

  @Test
  def testHighwaterMarkRelativeDirectoryMapping() {
    val props = TestUtils.createBrokerConfig(1, TestUtils.MockZkConnect)
    props.put("log.dir", TestUtils.tempRelativeDir("data").getAbsolutePath)
    val config = KafkaConfig.fromProps(props)
    val mockLogMgr = TestUtils.createLogManager(config.logDirs.map(new File(_)))
    val rm = new ReplicaManager(config, metrics, time, kafkaZkClient, new MockScheduler(time), mockLogMgr,
      new AtomicBoolean(false), QuotaFactory.instantiate(config, metrics, time, ""), new BrokerTopicStats,
      new MetadataCache(config.brokerId), new LogDirFailureChannel(config.logDirs.size))
    try {
      val partition = rm.getOrCreatePartition(new TopicPartition(topic, 1))
      partition.getOrCreateReplica(1)
      rm.checkpointHighWatermarks()
    } finally {
      // shutdown the replica manager upon test completion
      rm.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testIllegalRequiredAcks() {
    val props = TestUtils.createBrokerConfig(1, TestUtils.MockZkConnect)
    val config = KafkaConfig.fromProps(props)
    val mockLogMgr = TestUtils.createLogManager(config.logDirs.map(new File(_)))
    val rm = new ReplicaManager(config, metrics, time, kafkaZkClient, new MockScheduler(time), mockLogMgr,
      new AtomicBoolean(false), QuotaFactory.instantiate(config, metrics, time, ""), new BrokerTopicStats,
      new MetadataCache(config.brokerId), new LogDirFailureChannel(config.logDirs.size), Option(this.getClass.getName))
    try {
      def callback(responseStatus: Map[TopicPartition, PartitionResponse]) = {
        assert(responseStatus.values.head.error == Errors.INVALID_REQUIRED_ACKS)
      }
      rm.appendRecords(
        timeout = 0,
        requiredAcks = 3,
        internalTopicsAllowed = false,
        isFromClient = true,
        entriesPerPartition = Map(new TopicPartition("test1", 0) -> MemoryRecords.withRecords(CompressionType.NONE,
          new SimpleRecord("first message".getBytes))),
        responseCallback = callback)
    } finally {
      rm.shutdown(checkpointHW = false)
    }

    TestUtils.verifyNonDaemonThreadsStatus(this.getClass.getName)
  }

  @Test
  def testClearPurgatoryOnBecomingFollower() {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect)
    props.put("log.dir", TestUtils.tempRelativeDir("data").getAbsolutePath)
    val config = KafkaConfig.fromProps(props)
    val logProps = new Properties()
    val mockLogMgr = TestUtils.createLogManager(config.logDirs.map(new File(_)), LogConfig(logProps))
    val aliveBrokers = Seq(createBroker(0, "host0", 0), createBroker(1, "host1", 1))
    val metadataCache: MetadataCache = EasyMock.createMock(classOf[MetadataCache])
    EasyMock.expect(metadataCache.getAliveBrokers).andReturn(aliveBrokers).anyTimes()
    EasyMock.replay(metadataCache)
    val rm = new ReplicaManager(config, metrics, time, kafkaZkClient, new MockScheduler(time), mockLogMgr,
      new AtomicBoolean(false), QuotaFactory.instantiate(config, metrics, time, ""), new BrokerTopicStats,
      metadataCache, new LogDirFailureChannel(config.logDirs.size))

    try {
      val brokerList = Seq[Integer](0, 1).asJava

      val partition = rm.getOrCreatePartition(new TopicPartition(topic, 0))
      partition.getOrCreateReplica(0)
      // Make this replica the leader.
      val leaderAndIsrRequest1 = new LeaderAndIsrRequest.Builder(ApiKeys.LEADER_AND_ISR.latestVersion, 0, 0, brokerEpoch,
        collection.immutable.Map(new TopicPartition(topic, 0) ->
          new LeaderAndIsrRequest.PartitionState(0, 0, 0, brokerList, 0, brokerList, false)).asJava,
        Set(new Node(0, "host1", 0), new Node(1, "host2", 1)).asJava).build()
      rm.becomeLeaderOrFollower(0, leaderAndIsrRequest1, (_, _) => ())
      rm.getPartitionOrException(new TopicPartition(topic, 0), expectLeader = true)
          .localReplicaOrException

      val records = MemoryRecords.withRecords(CompressionType.NONE, new SimpleRecord("first message".getBytes()))
      val appendResult = appendRecords(rm, new TopicPartition(topic, 0), records).onFire { response =>
        assertEquals(Errors.NOT_LEADER_FOR_PARTITION, response.error)
      }

      // Fetch some messages
      val fetchResult = fetchAsConsumer(rm, new TopicPartition(topic, 0),
        new PartitionData(0, 0, 100000, Optional.empty()),
        minBytes = 100000)
      assertFalse(fetchResult.isFired)

      // Make this replica the follower
      val leaderAndIsrRequest2 = new LeaderAndIsrRequest.Builder(ApiKeys.LEADER_AND_ISR.latestVersion, 0, 0, brokerEpoch,
        collection.immutable.Map(new TopicPartition(topic, 0) ->
          new LeaderAndIsrRequest.PartitionState(0, 1, 1, brokerList, 0, brokerList, false)).asJava,
        Set(new Node(0, "host1", 0), new Node(1, "host2", 1)).asJava).build()
      rm.becomeLeaderOrFollower(1, leaderAndIsrRequest2, (_, _) => ())

      assertTrue(appendResult.isFired)
      assertTrue(fetchResult.isFired)
    } finally {
      rm.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testReceiveOutOfOrderSequenceExceptionWithLogStartOffset(): Unit = {
    val timer = new MockTimer
    val replicaManager = setupReplicaManagerWithMockedPurgatories(timer)

    try {
      val brokerList = Seq[Integer](0, 1).asJava

      val partition = replicaManager.getOrCreatePartition(new TopicPartition(topic, 0))
      partition.getOrCreateReplica(0)

      // Make this replica the leader.
      val leaderAndIsrRequest1 = new LeaderAndIsrRequest.Builder(ApiKeys.LEADER_AND_ISR.latestVersion, 0, 0, brokerEpoch,
        collection.immutable.Map(new TopicPartition(topic, 0) ->
          new LeaderAndIsrRequest.PartitionState(0, 0, 0, brokerList, 0, brokerList, true)).asJava,
        Set(new Node(0, "host1", 0), new Node(1, "host2", 1)).asJava).build()
      replicaManager.becomeLeaderOrFollower(0, leaderAndIsrRequest1, (_, _) => ())
      replicaManager.getPartitionOrException(new TopicPartition(topic, 0), expectLeader = true)
        .localReplicaOrException

      val producerId = 234L
      val epoch = 5.toShort

      // write a few batches as part of a transaction
      val numRecords = 3
      for (sequence <- 0 until numRecords) {
        val records = MemoryRecords.withIdempotentRecords(CompressionType.NONE, producerId, epoch, sequence,
          new SimpleRecord(s"message $sequence".getBytes))
        appendRecords(replicaManager, new TopicPartition(topic, 0), records).onFire { response =>
          assertEquals(Errors.NONE, response.error)
        }
      }

      assertEquals(0, partition.logStartOffset)

      // Append a record with an out of range sequence. We should get the OutOfOrderSequence error code with the log
      // start offset set.
      val outOfRangeSequence = numRecords + 10
      val record = MemoryRecords.withIdempotentRecords(CompressionType.NONE, producerId, epoch, outOfRangeSequence,
        new SimpleRecord(s"message: $outOfRangeSequence".getBytes))
      appendRecords(replicaManager, new TopicPartition(topic, 0), record).onFire { response =>
        assertEquals(Errors.OUT_OF_ORDER_SEQUENCE_NUMBER, response.error)
        assertEquals(0, response.logStartOffset)
      }

    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }

  }

  @Test
  def testReadCommittedFetchLimitedAtLSO(): Unit = {
    val timer = new MockTimer
    val replicaManager = setupReplicaManagerWithMockedPurgatories(timer)

    try {
      val brokerList = Seq[Integer](0, 1).asJava

      val partition = replicaManager.getOrCreatePartition(new TopicPartition(topic, 0))
      partition.getOrCreateReplica(0)

      // Make this replica the leader.
      val leaderAndIsrRequest1 = new LeaderAndIsrRequest.Builder(ApiKeys.LEADER_AND_ISR.latestVersion, 0, 0, brokerEpoch,
        collection.immutable.Map(new TopicPartition(topic, 0) ->
          new LeaderAndIsrRequest.PartitionState(0, 0, 0, brokerList, 0, brokerList, true)).asJava,
        Set(new Node(0, "host1", 0), new Node(1, "host2", 1)).asJava).build()
      replicaManager.becomeLeaderOrFollower(0, leaderAndIsrRequest1, (_, _) => ())
      replicaManager.getPartitionOrException(new TopicPartition(topic, 0), expectLeader = true)
        .localReplicaOrException

      val producerId = 234L
      val epoch = 5.toShort

      // write a few batches as part of a transaction
      val numRecords = 3
      for (sequence <- 0 until numRecords) {
        val records = MemoryRecords.withTransactionalRecords(CompressionType.NONE, producerId, epoch, sequence,
          new SimpleRecord(s"message $sequence".getBytes))
        appendRecords(replicaManager, new TopicPartition(topic, 0), records).onFire { response =>
          assertEquals(Errors.NONE, response.error)
        }
      }

      // fetch as follower to advance the high watermark
      fetchAsFollower(replicaManager, new TopicPartition(topic, 0),
        new PartitionData(numRecords, 0, 100000, Optional.empty()),
        isolationLevel = IsolationLevel.READ_UNCOMMITTED)

      // fetch should return empty since LSO should be stuck at 0
      var consumerFetchResult = fetchAsConsumer(replicaManager, new TopicPartition(topic, 0),
        new PartitionData(0, 0, 100000, Optional.empty()),
        isolationLevel = IsolationLevel.READ_COMMITTED)
      var fetchData = consumerFetchResult.assertFired
      assertEquals(Errors.NONE, fetchData.error)
      assertTrue(fetchData.records.batches.asScala.isEmpty)
      assertEquals(Some(0), fetchData.lastStableOffset)
      assertEquals(Some(List.empty[AbortedTransaction]), fetchData.abortedTransactions)

      // delayed fetch should timeout and return nothing
      consumerFetchResult = fetchAsConsumer(replicaManager, new TopicPartition(topic, 0),
        new PartitionData(0, 0, 100000, Optional.empty()),
        isolationLevel = IsolationLevel.READ_COMMITTED, minBytes = 1000)
      assertFalse(consumerFetchResult.isFired)
      timer.advanceClock(1001)

      fetchData = consumerFetchResult.assertFired
      assertEquals(Errors.NONE, fetchData.error)
      assertTrue(fetchData.records.batches.asScala.isEmpty)
      assertEquals(Some(0), fetchData.lastStableOffset)
      assertEquals(Some(List.empty[AbortedTransaction]), fetchData.abortedTransactions)

      // now commit the transaction
      val endTxnMarker = new EndTransactionMarker(ControlRecordType.COMMIT, 0)
      val commitRecordBatch = MemoryRecords.withEndTransactionMarker(producerId, epoch, endTxnMarker)
      appendRecords(replicaManager, new TopicPartition(topic, 0), commitRecordBatch, isFromClient = false)
        .onFire { response => assertEquals(Errors.NONE, response.error) }

      // the LSO has advanced, but the appended commit marker has not been replicated, so
      // none of the data from the transaction should be visible yet
      consumerFetchResult = fetchAsConsumer(replicaManager, new TopicPartition(topic, 0),
        new PartitionData(0, 0, 100000, Optional.empty()),
        isolationLevel = IsolationLevel.READ_COMMITTED)

      fetchData = consumerFetchResult.assertFired
      assertEquals(Errors.NONE, fetchData.error)
      assertTrue(fetchData.records.batches.asScala.isEmpty)

      // fetch as follower to advance the high watermark
      fetchAsFollower(replicaManager, new TopicPartition(topic, 0),
        new PartitionData(numRecords + 1, 0, 100000, Optional.empty()),
        isolationLevel = IsolationLevel.READ_UNCOMMITTED)

      // now all of the records should be fetchable
      consumerFetchResult = fetchAsConsumer(replicaManager, new TopicPartition(topic, 0),
        new PartitionData(0, 0, 100000, Optional.empty()),
        isolationLevel = IsolationLevel.READ_COMMITTED)

      fetchData = consumerFetchResult.assertFired
      assertEquals(Errors.NONE, fetchData.error)
      assertEquals(Some(numRecords + 1), fetchData.lastStableOffset)
      assertEquals(Some(List.empty[AbortedTransaction]), fetchData.abortedTransactions)
      assertEquals(numRecords + 1, fetchData.records.batches.asScala.size)
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testDelayedFetchIncludesAbortedTransactions(): Unit = {
    val timer = new MockTimer
    val replicaManager = setupReplicaManagerWithMockedPurgatories(timer)

    try {
      val brokerList = Seq[Integer](0, 1).asJava
      val partition = replicaManager.getOrCreatePartition(new TopicPartition(topic, 0))
      partition.getOrCreateReplica(0)

      // Make this replica the leader.
      val leaderAndIsrRequest1 = new LeaderAndIsrRequest.Builder(ApiKeys.LEADER_AND_ISR.latestVersion, 0, 0, brokerEpoch,
        collection.immutable.Map(new TopicPartition(topic, 0) ->
          new LeaderAndIsrRequest.PartitionState(0, 0, 0, brokerList, 0, brokerList, true)).asJava,
        Set(new Node(0, "host1", 0), new Node(1, "host2", 1)).asJava).build()
      replicaManager.becomeLeaderOrFollower(0, leaderAndIsrRequest1, (_, _) => ())
      replicaManager.getPartitionOrException(new TopicPartition(topic, 0), expectLeader = true)
        .localReplicaOrException

      val producerId = 234L
      val epoch = 5.toShort

      // write a few batches as part of a transaction
      val numRecords = 3
      for (sequence <- 0 until numRecords) {
        val records = MemoryRecords.withTransactionalRecords(CompressionType.NONE, producerId, epoch, sequence,
          new SimpleRecord(s"message $sequence".getBytes))
        appendRecords(replicaManager, new TopicPartition(topic, 0), records).onFire { response =>
          assertEquals(Errors.NONE, response.error)
        }
      }

      // now abort the transaction
      val endTxnMarker = new EndTransactionMarker(ControlRecordType.ABORT, 0)
      val abortRecordBatch = MemoryRecords.withEndTransactionMarker(producerId, epoch, endTxnMarker)
      appendRecords(replicaManager, new TopicPartition(topic, 0), abortRecordBatch, isFromClient = false)
        .onFire { response => assertEquals(Errors.NONE, response.error) }

      // fetch as follower to advance the high watermark
      fetchAsFollower(replicaManager, new TopicPartition(topic, 0),
        new PartitionData(numRecords + 1, 0, 100000, Optional.empty()),
        isolationLevel = IsolationLevel.READ_UNCOMMITTED)

      // Set the minBytes in order force this request to enter purgatory. When it returns, we should still
      // see the newly aborted transaction.
      val fetchResult = fetchAsConsumer(replicaManager, new TopicPartition(topic, 0),
        new PartitionData(0, 0, 100000, Optional.empty()),
        isolationLevel = IsolationLevel.READ_COMMITTED, minBytes = 10000)
      assertFalse(fetchResult.isFired)

      timer.advanceClock(1001)
      val fetchData = fetchResult.assertFired

      assertEquals(Errors.NONE, fetchData.error)
      assertEquals(Some(numRecords + 1), fetchData.lastStableOffset)
      assertEquals(numRecords + 1, fetchData.records.records.asScala.size)
      assertTrue(fetchData.abortedTransactions.isDefined)
      assertEquals(1, fetchData.abortedTransactions.get.size)

      val abortedTransaction = fetchData.abortedTransactions.get.head
      assertEquals(0L, abortedTransaction.firstOffset)
      assertEquals(producerId, abortedTransaction.producerId)
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testFetchBeyondHighWatermarkReturnEmptyResponse() {
    val rm = setupReplicaManagerWithMockedPurgatories(new MockTimer, aliveBrokerIds = Seq(0, 1, 2))
    try {
      val brokerList = Seq[Integer](0, 1, 2).asJava

      val partition = rm.getOrCreatePartition(new TopicPartition(topic, 0))
      partition.getOrCreateReplica(0)

      // Make this replica the leader.
      val leaderAndIsrRequest1 = new LeaderAndIsrRequest.Builder(ApiKeys.LEADER_AND_ISR.latestVersion, 0, 0, brokerEpoch,
        collection.immutable.Map(new TopicPartition(topic, 0) ->
          new LeaderAndIsrRequest.PartitionState(0, 0, 0, brokerList, 0, brokerList, false)).asJava,
        Set(new Node(0, "host1", 0), new Node(1, "host2", 1), new Node(2, "host2", 2)).asJava).build()
      rm.becomeLeaderOrFollower(0, leaderAndIsrRequest1, (_, _) => ())
      rm.getPartitionOrException(new TopicPartition(topic, 0), expectLeader = true)
        .localReplicaOrException

      // Append a couple of messages.
      for(i <- 1 to 2) {
        val records = TestUtils.singletonRecords(s"message $i".getBytes)
        appendRecords(rm, new TopicPartition(topic, 0), records).onFire { response =>
          assertEquals(Errors.NONE, response.error)
        }
      }

      // Fetch a message above the high watermark as a follower
      val followerFetchResult = fetchAsFollower(rm, new TopicPartition(topic, 0),
        new PartitionData(1, 0, 100000, Optional.empty()))
      val followerFetchData = followerFetchResult.assertFired
      assertEquals("Should not give an exception", Errors.NONE, followerFetchData.error)
      assertTrue("Should return some data", followerFetchData.records.batches.iterator.hasNext)

      // Fetch a message above the high watermark as a consumer
      val consumerFetchResult = fetchAsConsumer(rm, new TopicPartition(topic, 0),
        new PartitionData(1, 0, 100000, Optional.empty()))
      val consumerFetchData = consumerFetchResult.assertFired
      assertEquals("Should not give an exception", Errors.NONE, consumerFetchData.error)
      assertEquals("Should return empty response", MemoryRecords.EMPTY, consumerFetchData.records)
    } finally {
      rm.shutdown(checkpointHW = false)
    }
  }

  /**
   * If a follower sends a fetch request for 2 partitions and it's no longer the follower for one of them, the other
   * partition should not be affected.
   */
  @Test
  def testFetchMessagesWhenNotFollowerForOnePartition() {
    val replicaManager = setupReplicaManagerWithMockedPurgatories(new MockTimer, aliveBrokerIds = Seq(0, 1, 2))

    try {
      // Create 2 partitions, assign replica 0 as the leader for both a different follower (1 and 2) for each
      val tp0 = new TopicPartition(topic, 0)
      val tp1 = new TopicPartition(topic, 1)
      replicaManager.getOrCreatePartition(tp0).getOrCreateReplica(0)
      replicaManager.getOrCreatePartition(tp1).getOrCreateReplica(0)
      val partition0Replicas = Seq[Integer](0, 1).asJava
      val partition1Replicas = Seq[Integer](0, 2).asJava
      val leaderAndIsrRequest = new LeaderAndIsrRequest.Builder(ApiKeys.LEADER_AND_ISR.latestVersion, 0, 0, brokerEpoch,
        collection.immutable.Map(
          tp0 -> new LeaderAndIsrRequest.PartitionState(0, 0, 0, partition0Replicas, 0, partition0Replicas, true),
          tp1 -> new LeaderAndIsrRequest.PartitionState(0, 0, 0, partition1Replicas, 0, partition1Replicas, true)
        ).asJava,
        Set(new Node(0, "host1", 0), new Node(1, "host2", 1)).asJava).build()
      replicaManager.becomeLeaderOrFollower(0, leaderAndIsrRequest, (_, _) => ())

      // Append a couple of messages.
      for (i <- 1 to 2) {
        appendRecords(replicaManager, tp0, TestUtils.singletonRecords(s"message $i".getBytes)).onFire { response =>
          assertEquals(Errors.NONE, response.error)
        }
        appendRecords(replicaManager, tp1, TestUtils.singletonRecords(s"message $i".getBytes)).onFire { response =>
          assertEquals(Errors.NONE, response.error)
        }
      }

      def fetchCallback(responseStatus: Seq[(TopicPartition, FetchPartitionData)]) = {
        val responseStatusMap = responseStatus.toMap
        assertEquals(2, responseStatus.size)
        assertEquals(Set(tp0, tp1), responseStatusMap.keySet)

        val tp0Status = responseStatusMap.get(tp0)
        assertTrue(tp0Status.isDefined)
        // the response contains high watermark on the leader before it is updated based
        // on this fetch request
        assertEquals(0, tp0Status.get.highWatermark)
        assertEquals(Some(0), tp0Status.get.lastStableOffset)
        assertEquals(Errors.NONE, tp0Status.get.error)
        assertTrue(tp0Status.get.records.batches.iterator.hasNext)

        val tp1Status = responseStatusMap.get(tp1)
        assertTrue(tp1Status.isDefined)
        assertEquals(0, tp1Status.get.highWatermark)
        assertEquals(Some(0), tp0Status.get.lastStableOffset)
        assertEquals(Errors.NONE, tp1Status.get.error)
        assertFalse(tp1Status.get.records.batches.iterator.hasNext)
      }

      replicaManager.fetchMessages(
        timeout = 1000,
        replicaId = 1,
        fetchMinBytes = 0,
        fetchMaxBytes = Int.MaxValue,
        hardMaxBytesLimit = false,
        fetchInfos = Seq(
          tp0 -> new PartitionData(1, 0, 100000, Optional.empty()),
          tp1 -> new PartitionData(1, 0, 100000, Optional.empty())),
        responseCallback = fetchCallback,
        isolationLevel = IsolationLevel.READ_UNCOMMITTED
      )
      val tp0Replica = replicaManager.localReplica(tp0)
      assertTrue(tp0Replica.isDefined)
      assertEquals("hw should be incremented", 1, tp0Replica.get.highWatermark.messageOffset)

      replicaManager.localReplica(tp1)
      val tp1Replica = replicaManager.localReplica(tp1)
      assertTrue(tp1Replica.isDefined)
      assertEquals("hw should not be incremented", 0, tp1Replica.get.highWatermark.messageOffset)

    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  /**
    * If a partition becomes a follower and the leader is unchanged it should check for truncation
    * if the epoch has increased by more than one (which suggests it has missed an update)
    */
  @Test
  def testBecomeFollowerWhenLeaderIsUnchangedButMissedLeaderUpdate() {
    val topicPartition = 0
    val followerBrokerId = 0
    val leaderBrokerId = 1
    val controllerId = 0
    val controllerEpoch = 0
    var leaderEpoch = 1
    val leaderEpochIncrement = 2
    val aliveBrokerIds = Seq[Integer] (followerBrokerId, leaderBrokerId)
    val countDownLatch = new CountDownLatch(1)

    // Prepare the mocked components for the test
    val (replicaManager, mockLogMgr) = prepareReplicaManagerAndLogManager(
      topicPartition, leaderEpoch + leaderEpochIncrement, followerBrokerId, leaderBrokerId, countDownLatch, expectTruncation = true)

    // Initialize partition state to follower, with leader = 1, leaderEpoch = 1
    val partition = replicaManager.getOrCreatePartition(new TopicPartition(topic, topicPartition))
    partition.getOrCreateReplica(followerBrokerId)
    partition.makeFollower(controllerId,
      leaderAndIsrPartitionState(leaderEpoch, leaderBrokerId, aliveBrokerIds),
      correlationId)

    // Make local partition a follower - because epoch increased by more than 1, truncation should
    // trigger even though leader does not change
    leaderEpoch += leaderEpochIncrement
    val leaderAndIsrRequest0 = new LeaderAndIsrRequest.Builder(ApiKeys.LEADER_AND_ISR.latestVersion,
      controllerId, controllerEpoch, brokerEpoch,
      collection.immutable.Map(new TopicPartition(topic, topicPartition) ->
        leaderAndIsrPartitionState(leaderEpoch, leaderBrokerId, aliveBrokerIds)).asJava,
      Set(new Node(followerBrokerId, "host1", 0),
        new Node(leaderBrokerId, "host2", 1)).asJava).build()
    replicaManager.becomeLeaderOrFollower(correlationId, leaderAndIsrRequest0,
      (_, followers) => assertEquals(followerBrokerId, followers.head.partitionId))
    assertTrue(countDownLatch.await(1000L, TimeUnit.MILLISECONDS))

    // Truncation should have happened once
    EasyMock.verify(mockLogMgr)
  }

  /**
   * This method assumes that the test using created ReplicaManager calls
   * ReplicaManager.becomeLeaderOrFollower() once with LeaderAndIsrRequest containing
   * 'leaderEpochInLeaderAndIsr' leader epoch for partition 'topicPartition'.
   */
  private def prepareReplicaManagerAndLogManager(topicPartition: Int,
                                                 leaderEpochInLeaderAndIsr: Int,
                                                 followerBrokerId: Int,
                                                 leaderBrokerId: Int,
                                                 countDownLatch: CountDownLatch,
                                                 expectTruncation: Boolean) : (ReplicaManager, LogManager) = {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect)
    props.put("log.dir", TestUtils.tempRelativeDir("data").getAbsolutePath)
    val config = KafkaConfig.fromProps(props)

    // Setup mock local log to have leader epoch of 3 and offset of 10
    val localLogOffset = 10
    val offsetFromLeader = 5
    val leaderEpochFromLeader = 3
    val mockScheduler = new MockScheduler(time)
    val mockBrokerTopicStats = new BrokerTopicStats
    val mockLogDirFailureChannel = new LogDirFailureChannel(config.logDirs.size)
    val mockLog = new Log(
      dir = new File(new File(config.logDirs.head), s"$topic-0"),
      config = LogConfig(),
      logStartOffset = 0L,
      recoveryPoint = 0L,
      scheduler = mockScheduler,
      brokerTopicStats = mockBrokerTopicStats,
      time = time,
      maxProducerIdExpirationMs = 30000,
      producerIdExpirationCheckIntervalMs = 30000,
      topicPartition = new TopicPartition(topic, topicPartition),
      producerStateManager = new ProducerStateManager(new TopicPartition(topic, topicPartition),
        new File(new File(config.logDirs.head), s"$topic-$topicPartition"), 30000),
      logDirFailureChannel = mockLogDirFailureChannel) {

      override def endOffsetForEpoch(leaderEpoch: Int): Option[OffsetAndEpoch] = {
        assertEquals(leaderEpoch, leaderEpochFromLeader)
        Some(OffsetAndEpoch(localLogOffset, leaderEpochFromLeader))
      }

      override def latestEpoch: Option[Int] = Some(leaderEpochFromLeader)

      override def logEndOffsetMetadata = LogOffsetMetadata(localLogOffset)
    }

    // Expect to call LogManager.truncateTo exactly once
    val mockLogMgr: LogManager = EasyMock.createMock(classOf[LogManager])
    EasyMock.expect(mockLogMgr.liveLogDirs).andReturn(config.logDirs.map(new File(_).getAbsoluteFile)).anyTimes
    EasyMock.expect(mockLogMgr.currentDefaultConfig).andReturn(LogConfig())
    EasyMock.expect(mockLogMgr.getOrCreateLog(new TopicPartition(topic, topicPartition),
      LogConfig(), isNew = false, isFuture = false)).andReturn(mockLog).anyTimes
    if (expectTruncation) {
      EasyMock.expect(mockLogMgr.truncateTo(Map(new TopicPartition(topic, topicPartition) -> offsetFromLeader),
        isFuture = false)).once
    }
    EasyMock.replay(mockLogMgr)

    val aliveBrokerIds = Seq[Integer](followerBrokerId, leaderBrokerId)
    val aliveBrokers = aliveBrokerIds.map(brokerId => createBroker(brokerId, s"host$brokerId", brokerId))

    val metadataCache: MetadataCache = EasyMock.createMock(classOf[MetadataCache])
    EasyMock.expect(metadataCache.getAliveBrokers).andReturn(aliveBrokers).anyTimes
    aliveBrokerIds.foreach { brokerId =>
      EasyMock.expect(metadataCache.isBrokerAlive(EasyMock.eq(brokerId))).andReturn(true).anyTimes
    }
    EasyMock.replay(metadataCache)


    val timer = new MockTimer
    val mockProducePurgatory = new DelayedOperationPurgatory[DelayedProduce](
      purgatoryName = "Produce", timer, reaperEnabled = false)
    val mockFetchPurgatory = new DelayedOperationPurgatory[DelayedFetch](
      purgatoryName = "Fetch", timer, reaperEnabled = false)
    val mockDeleteRecordsPurgatory = new DelayedOperationPurgatory[DelayedDeleteRecords](
      purgatoryName = "DeleteRecords", timer, reaperEnabled = false)
    val mockElectPreferredLeaderPurgatory = new DelayedOperationPurgatory[DelayedElectPreferredLeader](
      purgatoryName = "ElectPreferredLeader", timer, reaperEnabled = false)

    // Mock network client to show leader offset of 5
    val quota = QuotaFactory.instantiate(config, metrics, time, "")
    val blockingSend = new ReplicaFetcherMockBlockingSend(Map(new TopicPartition(topic, topicPartition) ->
      new EpochEndOffset(leaderEpochFromLeader, offsetFromLeader)).asJava, BrokerEndPoint(1, "host1" ,1), time)
    val replicaManager = new ReplicaManager(config, metrics, time, kafkaZkClient, mockScheduler, mockLogMgr,
      new AtomicBoolean(false), quota, mockBrokerTopicStats,
      metadataCache, mockLogDirFailureChannel, mockProducePurgatory, mockFetchPurgatory,
      mockDeleteRecordsPurgatory, mockElectPreferredLeaderPurgatory, Option(this.getClass.getName)) {

      override protected def createReplicaFetcherManager(metrics: Metrics,
                                                     time: Time,
                                                     threadNamePrefix: Option[String],
                                                     quotaManager: ReplicationQuotaManager): ReplicaFetcherManager = {
        new ReplicaFetcherManager(config, this, metrics, time, threadNamePrefix, quotaManager) {

          override def createFetcherThread(fetcherId: Int, sourceBroker: BrokerEndPoint): ReplicaFetcherThread = {
            new ReplicaFetcherThread(s"ReplicaFetcherThread-$fetcherId", fetcherId,
              sourceBroker, config, replicaManager, metrics, time, quota.follower, Some(blockingSend)) {

              override def doWork() = {
                // In case the thread starts before the partition is added by AbstractFetcherManager,
                // add it here (it's a no-op if already added)
                val initialOffset = OffsetAndEpoch(offset = 0L, leaderEpoch = leaderEpochInLeaderAndIsr)
                addPartitions(Map(new TopicPartition(topic, topicPartition) -> initialOffset))
                super.doWork()

                // Shut the thread down after one iteration to avoid double-counting truncations
                initiateShutdown()
                countDownLatch.countDown()
              }
            }
          }
        }
      }
    }

    (replicaManager, mockLogMgr)
  }

  private def leaderAndIsrPartitionState(leaderEpoch: Int,
                                         leaderBrokerId: Int,
                                         aliveBrokerIds: Seq[Integer]) : LeaderAndIsrRequest.PartitionState = {
    new LeaderAndIsrRequest.PartitionState(controllerEpoch, leaderBrokerId, leaderEpoch, aliveBrokerIds.asJava,
      zkVersion, aliveBrokerIds.asJava, false)
  }

  private class CallbackResult[T] {
    private var value: Option[T] = None
    private var fun: Option[T => Unit] = None

    def assertFired: T = {
      assertTrue("Callback has not been fired", isFired)
      value.get
    }

    def isFired: Boolean = {
      value.isDefined
    }

    def fire(value: T): Unit = {
      this.value = Some(value)
      fun.foreach(f => f(value))
    }

    def onFire(fun: T => Unit): CallbackResult[T] = {
      this.fun = Some(fun)
      if (this.isFired) fire(value.get)
      this
    }
  }

  private def appendRecords(replicaManager: ReplicaManager,
                            partition: TopicPartition,
                            records: MemoryRecords,
                            isFromClient: Boolean = true,
                            requiredAcks: Short = -1): CallbackResult[PartitionResponse] = {
    val result = new CallbackResult[PartitionResponse]()
    def appendCallback(responses: Map[TopicPartition, PartitionResponse]): Unit = {
      val response = responses.get(partition)
      assertTrue(response.isDefined)
      result.fire(response.get)
    }

    replicaManager.appendRecords(
      timeout = 1000,
      requiredAcks = requiredAcks,
      internalTopicsAllowed = false,
      isFromClient = isFromClient,
      entriesPerPartition = Map(partition -> records),
      responseCallback = appendCallback)

    result
  }

  private def fetchAsConsumer(replicaManager: ReplicaManager,
                              partition: TopicPartition,
                              partitionData: PartitionData,
                              minBytes: Int = 0,
                              isolationLevel: IsolationLevel = IsolationLevel.READ_UNCOMMITTED): CallbackResult[FetchPartitionData] = {
    fetchMessages(replicaManager, replicaId = -1, partition, partitionData, minBytes, isolationLevel)
  }

  private def fetchAsFollower(replicaManager: ReplicaManager,
                              partition: TopicPartition,
                              partitionData: PartitionData,
                              minBytes: Int = 0,
                              isolationLevel: IsolationLevel = IsolationLevel.READ_UNCOMMITTED): CallbackResult[FetchPartitionData] = {
    fetchMessages(replicaManager, replicaId = 1, partition, partitionData, minBytes, isolationLevel)
  }

  private def fetchMessages(replicaManager: ReplicaManager,
                            replicaId: Int,
                            partition: TopicPartition,
                            partitionData: PartitionData,
                            minBytes: Int,
                            isolationLevel: IsolationLevel): CallbackResult[FetchPartitionData] = {
    val result = new CallbackResult[FetchPartitionData]()
    def fetchCallback(responseStatus: Seq[(TopicPartition, FetchPartitionData)]) = {
      assertEquals(1, responseStatus.size)
      val (topicPartition, fetchData) = responseStatus.head
      assertEquals(partition, topicPartition)
      result.fire(fetchData)
    }

    replicaManager.fetchMessages(
      timeout = 1000,
      replicaId = replicaId,
      fetchMinBytes = minBytes,
      fetchMaxBytes = Int.MaxValue,
      hardMaxBytesLimit = false,
      fetchInfos = Seq(partition -> partitionData),
      responseCallback = fetchCallback,
      isolationLevel = isolationLevel)

    result
  }

  private def setupReplicaManagerWithMockedPurgatories(timer: MockTimer, aliveBrokerIds: Seq[Int] = Seq(0, 1)): ReplicaManager = {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect)
    props.put("log.dir", TestUtils.tempRelativeDir("data").getAbsolutePath)
    val config = KafkaConfig.fromProps(props)
    val logProps = new Properties()
    val mockLogMgr = TestUtils.createLogManager(config.logDirs.map(new File(_)), LogConfig(logProps))
    val aliveBrokers = aliveBrokerIds.map(brokerId => createBroker(brokerId, s"host$brokerId", brokerId))
    val metadataCache: MetadataCache = EasyMock.createMock(classOf[MetadataCache])
    EasyMock.expect(metadataCache.getAliveBrokers).andReturn(aliveBrokers).anyTimes()
    aliveBrokerIds.foreach { brokerId =>
      EasyMock.expect(metadataCache.isBrokerAlive(EasyMock.eq(brokerId))).andReturn(true).anyTimes()
    }
    EasyMock.replay(metadataCache)

    val mockProducePurgatory = new DelayedOperationPurgatory[DelayedProduce](
      purgatoryName = "Produce", timer, reaperEnabled = false)
    val mockFetchPurgatory = new DelayedOperationPurgatory[DelayedFetch](
      purgatoryName = "Fetch", timer, reaperEnabled = false)
    val mockDeleteRecordsPurgatory = new DelayedOperationPurgatory[DelayedDeleteRecords](
      purgatoryName = "DeleteRecords", timer, reaperEnabled = false)
    val mockDelayedElectPreferredLeaderPurgatory = new DelayedOperationPurgatory[DelayedElectPreferredLeader](
      purgatoryName = "DelayedElectPreferredLeader", timer, reaperEnabled = false)

    new ReplicaManager(config, metrics, time, kafkaZkClient, new MockScheduler(time), mockLogMgr,
      new AtomicBoolean(false), QuotaFactory.instantiate(config, metrics, time, ""), new BrokerTopicStats,
      metadataCache, new LogDirFailureChannel(config.logDirs.size), mockProducePurgatory, mockFetchPurgatory,
      mockDeleteRecordsPurgatory, mockDelayedElectPreferredLeaderPurgatory, Option(this.getClass.getName))
  }

}
