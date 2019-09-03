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
import java.net.InetAddress
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.{Optional, Properties}

import kafka.api.Request
import kafka.cluster.BrokerEndPoint
import kafka.log.{Log, LogConfig, LogManager, ProducerStateManager}
import kafka.utils.{MockScheduler, MockTime, TestUtils}
import TestUtils.createBroker
import kafka.cluster.BrokerEndPoint
import kafka.server.QuotaFactory.UnboundedQuota
import kafka.server.checkpoints.LazyOffsetCheckpoints
import kafka.server.epoch.util.ReplicaFetcherMockBlockingSend
import kafka.utils.TestUtils.createBroker
import kafka.utils.timer.MockTimer
import kafka.utils.{MockScheduler, MockTime, TestUtils}
import kafka.zk.KafkaZkClient
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record._
import org.apache.kafka.common.replica.ClientMetadata.DefaultClientMetadata
import org.apache.kafka.common.replica.ClientMetadata
import org.apache.kafka.common.requests.FetchRequest
import org.apache.kafka.common.requests.FetchRequest.PartitionData
import org.apache.kafka.common.requests.FetchResponse.AbortedTransaction
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.requests.{EpochEndOffset, IsolationLevel, LeaderAndIsrRequest}
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.{Node, TopicPartition}
import org.easymock.EasyMock
import org.junit.Assert._
import org.junit.{After, Before, Test}

import scala.collection.JavaConverters._
import scala.collection.{Map, Seq}

class ReplicaManagerTest {

  val topic = "test-topic"
  val time = new MockTime
  val metrics = new Metrics
  var kafkaZkClient: KafkaZkClient = _

  // Constants defined for readability
  val zkVersion = 0
  val correlationId = 0
  var controllerEpoch = 0
  val brokerEpoch = 0L

  @Before
  def setUp(): Unit = {
    kafkaZkClient = EasyMock.createMock(classOf[KafkaZkClient])
    EasyMock.expect(kafkaZkClient.getEntityConfigs(EasyMock.anyString(), EasyMock.anyString())).andReturn(new Properties()).anyTimes()
    EasyMock.replay(kafkaZkClient)
  }

  @After
  def tearDown(): Unit = {
    metrics.close()
  }

  @Test
  def testHighWaterMarkDirectoryMapping(): Unit = {
    val props = TestUtils.createBrokerConfig(1, TestUtils.MockZkConnect)
    val config = KafkaConfig.fromProps(props)
    val mockLogMgr = TestUtils.createLogManager(config.logDirs.map(new File(_)))
    val rm = new ReplicaManager(config, metrics, time, kafkaZkClient, new MockScheduler(time), mockLogMgr,
      new AtomicBoolean(false), QuotaFactory.instantiate(config, metrics, time, ""), new BrokerTopicStats,
      new MetadataCache(config.brokerId), new LogDirFailureChannel(config.logDirs.size))
    try {
      val partition = rm.createPartition(new TopicPartition(topic, 1))
      partition.createLogIfNotExists(1, isNew = false, isFutureReplica = false,
        new LazyOffsetCheckpoints(rm.highWatermarkCheckpoints))
      rm.checkpointHighWatermarks()
    } finally {
      // shutdown the replica manager upon test completion
      rm.shutdown(false)
    }
  }

  @Test
  def testHighwaterMarkRelativeDirectoryMapping(): Unit = {
    val props = TestUtils.createBrokerConfig(1, TestUtils.MockZkConnect)
    props.put("log.dir", TestUtils.tempRelativeDir("data").getAbsolutePath)
    val config = KafkaConfig.fromProps(props)
    val mockLogMgr = TestUtils.createLogManager(config.logDirs.map(new File(_)))
    val rm = new ReplicaManager(config, metrics, time, kafkaZkClient, new MockScheduler(time), mockLogMgr,
      new AtomicBoolean(false), QuotaFactory.instantiate(config, metrics, time, ""), new BrokerTopicStats,
      new MetadataCache(config.brokerId), new LogDirFailureChannel(config.logDirs.size))
    try {
      val partition = rm.createPartition(new TopicPartition(topic, 1))
      partition.createLogIfNotExists(1, isNew = false, isFutureReplica = false,
        new LazyOffsetCheckpoints(rm.highWatermarkCheckpoints))
      rm.checkpointHighWatermarks()
    } finally {
      // shutdown the replica manager upon test completion
      rm.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testIllegalRequiredAcks(): Unit = {
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

    TestUtils.assertNoNonDaemonThreads(this.getClass.getName)
  }

  @Test
  def testClearPurgatoryOnBecomingFollower(): Unit = {
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

      val partition = rm.createPartition(new TopicPartition(topic, 0))
      partition.createLogIfNotExists(0, isNew = false, isFutureReplica = false,
        new LazyOffsetCheckpoints(rm.highWatermarkCheckpoints))
      // Make this replica the leader.
      val leaderAndIsrRequest1 = new LeaderAndIsrRequest.Builder(ApiKeys.LEADER_AND_ISR.latestVersion, 0, 0, brokerEpoch,
        collection.immutable.Map(new TopicPartition(topic, 0) ->
          new LeaderAndIsrRequest.PartitionState(0, 0, 0, brokerList, 0, brokerList, false)).asJava,
        Set(new Node(0, "host1", 0), new Node(1, "host2", 1)).asJava).build()
      rm.becomeLeaderOrFollower(0, leaderAndIsrRequest1, (_, _) => ())
      rm.getPartitionOrException(new TopicPartition(topic, 0), expectLeader = true)
          .localLogOrException

      val records = MemoryRecords.withRecords(CompressionType.NONE, new SimpleRecord("first message".getBytes()))
      val appendResult = appendRecords(rm, new TopicPartition(topic, 0), records).onFire { response =>
        assertEquals(Errors.NOT_LEADER_FOR_PARTITION, response.error)
      }

      // Make this replica the follower
      val leaderAndIsrRequest2 = new LeaderAndIsrRequest.Builder(ApiKeys.LEADER_AND_ISR.latestVersion, 0, 0, brokerEpoch,
        collection.immutable.Map(new TopicPartition(topic, 0) ->
          new LeaderAndIsrRequest.PartitionState(0, 1, 1, brokerList, 0, brokerList, false)).asJava,
        Set(new Node(0, "host1", 0), new Node(1, "host2", 1)).asJava).build()
      rm.becomeLeaderOrFollower(1, leaderAndIsrRequest2, (_, _) => ())

      assertTrue(appendResult.isFired)
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

      val partition = replicaManager.createPartition(new TopicPartition(topic, 0))
      partition.createLogIfNotExists(0, isNew = false, isFutureReplica = false,
        new LazyOffsetCheckpoints(replicaManager.highWatermarkCheckpoints))

      // Make this replica the leader.
      val leaderAndIsrRequest1 = new LeaderAndIsrRequest.Builder(ApiKeys.LEADER_AND_ISR.latestVersion, 0, 0, brokerEpoch,
        collection.immutable.Map(new TopicPartition(topic, 0) ->
          new LeaderAndIsrRequest.PartitionState(0, 0, 0, brokerList, 0, brokerList, true)).asJava,
        Set(new Node(0, "host1", 0), new Node(1, "host2", 1)).asJava).build()
      replicaManager.becomeLeaderOrFollower(0, leaderAndIsrRequest1, (_, _) => ())
      replicaManager.getPartitionOrException(new TopicPartition(topic, 0), expectLeader = true)
        .localLogOrException

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

      val partition = replicaManager.createPartition(new TopicPartition(topic, 0))
      partition.createLogIfNotExists(0, isNew = false, isFutureReplica = false,
        new LazyOffsetCheckpoints(replicaManager.highWatermarkCheckpoints))

      // Make this replica the leader.
      val leaderAndIsrRequest1 = new LeaderAndIsrRequest.Builder(ApiKeys.LEADER_AND_ISR.latestVersion, 0, 0, brokerEpoch,
        collection.immutable.Map(new TopicPartition(topic, 0) ->
          new LeaderAndIsrRequest.PartitionState(0, 0, 0, brokerList, 0, brokerList, true)).asJava,
        Set(new Node(0, "host1", 0), new Node(1, "host2", 1)).asJava).build()
      replicaManager.becomeLeaderOrFollower(0, leaderAndIsrRequest1, (_, _) => ())
      replicaManager.getPartitionOrException(new TopicPartition(topic, 0), expectLeader = true)
        .localLogOrException

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
      val partition = replicaManager.createPartition(new TopicPartition(topic, 0))
      partition.createLogIfNotExists(0, isNew = false, isFutureReplica = false,
        new LazyOffsetCheckpoints(replicaManager.highWatermarkCheckpoints))

      // Make this replica the leader.
      val leaderAndIsrRequest1 = new LeaderAndIsrRequest.Builder(ApiKeys.LEADER_AND_ISR.latestVersion, 0, 0, brokerEpoch,
        collection.immutable.Map(new TopicPartition(topic, 0) ->
          new LeaderAndIsrRequest.PartitionState(0, 0, 0, brokerList, 0, brokerList, true)).asJava,
        Set(new Node(0, "host1", 0), new Node(1, "host2", 1)).asJava).build()
      replicaManager.becomeLeaderOrFollower(0, leaderAndIsrRequest1, (_, _) => ())
      replicaManager.getPartitionOrException(new TopicPartition(topic, 0), expectLeader = true)
        .localLogOrException

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
  def testFetchBeyondHighWatermarkReturnEmptyResponse(): Unit = {
    val rm = setupReplicaManagerWithMockedPurgatories(new MockTimer, aliveBrokerIds = Seq(0, 1, 2))
    try {
      val brokerList = Seq[Integer](0, 1, 2).asJava

      val partition = rm.createPartition(new TopicPartition(topic, 0))
      partition.createLogIfNotExists(0, isNew = false, isFutureReplica = false,
        new LazyOffsetCheckpoints(rm.highWatermarkCheckpoints))

      // Make this replica the leader.
      val leaderAndIsrRequest1 = new LeaderAndIsrRequest.Builder(ApiKeys.LEADER_AND_ISR.latestVersion, 0, 0, brokerEpoch,
        collection.immutable.Map(new TopicPartition(topic, 0) ->
          new LeaderAndIsrRequest.PartitionState(0, 0, 0, brokerList, 0, brokerList, false)).asJava,
        Set(new Node(0, "host1", 0), new Node(1, "host2", 1), new Node(2, "host2", 2)).asJava).build()
      rm.becomeLeaderOrFollower(0, leaderAndIsrRequest1, (_, _) => ())
      rm.getPartitionOrException(new TopicPartition(topic, 0), expectLeader = true)
        .localLogOrException

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

  @Test
  def testFollowerStateNotUpdatedIfLogReadFails(): Unit = {
    val maxFetchBytes = 1024 * 1024
    val aliveBrokersIds = Seq(0, 1)
    val leaderEpoch = 5
    val replicaManager = setupReplicaManagerWithMockedPurgatories(new MockTimer, aliveBrokersIds)
    try {
      val tp = new TopicPartition(topic, 0)
      val replicas = aliveBrokersIds.toList.map(Int.box).asJava

      // Broker 0 becomes leader of the partition
      val leaderAndIsrPartitionState = new LeaderAndIsrRequest.PartitionState(0, 0, leaderEpoch,
        replicas, 0, replicas, true)
      val leaderAndIsrRequest = new LeaderAndIsrRequest.Builder(ApiKeys.LEADER_AND_ISR.latestVersion, 0, 0, brokerEpoch,
        Map(tp -> leaderAndIsrPartitionState).asJava,
        Set(new Node(0, "host1", 0), new Node(1, "host2", 1)).asJava).build()
      val leaderAndIsrResponse = replicaManager.becomeLeaderOrFollower(0, leaderAndIsrRequest, (_, _) => ())
      assertEquals(Errors.NONE, leaderAndIsrResponse.error)

      // Follower replica state is initialized, but initial state is not known
      assertTrue(replicaManager.nonOfflinePartition(tp).isDefined)
      val partition = replicaManager.nonOfflinePartition(tp).get

      assertTrue(partition.getReplica(1).isDefined)
      val followerReplica = partition.getReplica(1).get
      assertEquals(-1L, followerReplica.logStartOffset)
      assertEquals(-1L, followerReplica.logEndOffset)

      // Leader appends some data
      for (i <- 1 to 5) {
        appendRecords(replicaManager, tp, TestUtils.singletonRecords(s"message $i".getBytes)).onFire { response =>
          assertEquals(Errors.NONE, response.error)
        }
      }

      // We receive one valid request from the follower and replica state is updated
      var successfulFetch: Option[FetchPartitionData] = None
      def callback(response: Seq[(TopicPartition, FetchPartitionData)]): Unit = {
        successfulFetch = response.headOption.filter(_._1 == tp).map(_._2)
      }

      val validFetchPartitionData = new FetchRequest.PartitionData(0L, 0L, maxFetchBytes,
        Optional.of(leaderEpoch))

      replicaManager.fetchMessages(
        timeout = 0L,
        replicaId = 1,
        fetchMinBytes = 1,
        fetchMaxBytes = maxFetchBytes,
        hardMaxBytesLimit = false,
        fetchInfos = Seq(tp -> validFetchPartitionData),
        quota = UnboundedQuota,
        isolationLevel = IsolationLevel.READ_UNCOMMITTED,
        responseCallback = callback,
        clientMetadata = None
      )

      assertTrue(successfulFetch.isDefined)
      assertEquals(0L, followerReplica.logStartOffset)
      assertEquals(0L, followerReplica.logEndOffset)


      // Next we receive an invalid request with a higher fetch offset, but an old epoch.
      // We expect that the replica state does not get updated.
      val invalidFetchPartitionData = new FetchRequest.PartitionData(3L, 0L, maxFetchBytes,
        Optional.of(leaderEpoch - 1))

      replicaManager.fetchMessages(
        timeout = 0L,
        replicaId = 1,
        fetchMinBytes = 1,
        fetchMaxBytes = maxFetchBytes,
        hardMaxBytesLimit = false,
        fetchInfos = Seq(tp -> invalidFetchPartitionData),
        quota = UnboundedQuota,
        isolationLevel = IsolationLevel.READ_UNCOMMITTED,
        responseCallback = callback,
        clientMetadata = None
      )

      assertTrue(successfulFetch.isDefined)
      assertEquals(0L, followerReplica.logStartOffset)
      assertEquals(0L, followerReplica.logEndOffset)

    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  /**
   * If a follower sends a fetch request for 2 partitions and it's no longer the follower for one of them, the other
   * partition should not be affected.
   */
  @Test
  def testFetchMessagesWhenNotFollowerForOnePartition(): Unit = {
    val replicaManager = setupReplicaManagerWithMockedPurgatories(new MockTimer, aliveBrokerIds = Seq(0, 1, 2))

    try {
      // Create 2 partitions, assign replica 0 as the leader for both a different follower (1 and 2) for each
      val tp0 = new TopicPartition(topic, 0)
      val tp1 = new TopicPartition(topic, 1)
      val offsetCheckpoints = new LazyOffsetCheckpoints(replicaManager.highWatermarkCheckpoints)
      replicaManager.createPartition(tp0).createLogIfNotExists(0, isNew = false, isFutureReplica = false, offsetCheckpoints)
      replicaManager.createPartition(tp1).createLogIfNotExists(0, isNew = false, isFutureReplica = false, offsetCheckpoints)
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
        quota = UnboundedQuota,
        responseCallback = fetchCallback,
        isolationLevel = IsolationLevel.READ_UNCOMMITTED,
        clientMetadata = None
      )
      val tp0Log = replicaManager.localLog(tp0)
      assertTrue(tp0Log.isDefined)
      assertEquals("hw should be incremented", 1, tp0Log.get.highWatermark)

      replicaManager.localLog(tp1)
      val tp1Replica = replicaManager.localLog(tp1)
      assertTrue(tp1Replica.isDefined)
      assertEquals("hw should not be incremented", 0, tp1Replica.get.highWatermark)

    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  /**
    * If a partition becomes a follower and the leader is unchanged it should check for truncation
    * if the epoch has increased by more than one (which suggests it has missed an update)
    */
  @Test
  def testBecomeFollowerWhenLeaderIsUnchangedButMissedLeaderUpdate(): Unit = {
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
    val partition = replicaManager.createPartition(new TopicPartition(topic, topicPartition))
    val offsetCheckpoints = new LazyOffsetCheckpoints(replicaManager.highWatermarkCheckpoints)
    partition.createLogIfNotExists(followerBrokerId, isNew = false, isFutureReplica = false, offsetCheckpoints)
    partition.makeFollower(controllerId,
      leaderAndIsrPartitionState(leaderEpoch, leaderBrokerId, aliveBrokerIds),
      correlationId, offsetCheckpoints)

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

  @Test
  def testReplicaSelector(): Unit = {
    val topicPartition = 0
    val followerBrokerId = 0
    val leaderBrokerId = 1
    val controllerId = 0
    val leaderEpoch = 1
    val leaderEpochIncrement = 2
    val aliveBrokerIds = Seq[Integer] (followerBrokerId, leaderBrokerId)
    val countDownLatch = new CountDownLatch(1)

    // Prepare the mocked components for the test
    val (replicaManager, mockLogMgr) = prepareReplicaManagerAndLogManager(
      topicPartition, leaderEpoch + leaderEpochIncrement, followerBrokerId,
      leaderBrokerId, countDownLatch, expectTruncation = true)

    val partition = replicaManager.createPartition(new TopicPartition(topic, topicPartition))

    val offsetCheckpoints = new LazyOffsetCheckpoints(replicaManager.highWatermarkCheckpoints)
    partition.createLogIfNotExists(leaderBrokerId, isNew = false, isFutureReplica = false, offsetCheckpoints)
    partition.makeLeader(
      controllerId,
      leaderAndIsrPartitionState(leaderEpoch, leaderBrokerId, aliveBrokerIds),
      correlationId,
      offsetCheckpoints
    )

    val tp0 = new TopicPartition(topic, 0)

    val metadata: ClientMetadata = new DefaultClientMetadata("rack-a", "client-id",
      InetAddress.getByName("localhost"), KafkaPrincipal.ANONYMOUS, "default")

    // We expect to select the leader, which means we return None
    val preferredReadReplica: Option[Int] = replicaManager.findPreferredReadReplica(
      tp0, metadata, Request.OrdinaryConsumerId, 1L, System.currentTimeMillis)
    assertFalse(preferredReadReplica.isDefined)
  }

  @Test
  def testPreferredReplicaAsFollower(): Unit = {
    val topicPartition = 0
    val followerBrokerId = 0
    val leaderBrokerId = 1
    val leaderEpoch = 1
    val leaderEpochIncrement = 2
    val countDownLatch = new CountDownLatch(1)

    // Prepare the mocked components for the test
    val (replicaManager, mockLogMgr) = prepareReplicaManagerAndLogManager(
      topicPartition, leaderEpoch + leaderEpochIncrement, followerBrokerId,
      leaderBrokerId, countDownLatch, expectTruncation = true)

    val brokerList = Seq[Integer](0, 1).asJava

    val tp0 = new TopicPartition(topic, 0)

    replicaManager.createPartition(new TopicPartition(topic, 0))

    // Make this replica the follower
    val leaderAndIsrRequest2 = new LeaderAndIsrRequest.Builder(ApiKeys.LEADER_AND_ISR.latestVersion, 0, 0, brokerEpoch,
      collection.immutable.Map(new TopicPartition(topic, 0) ->
        new LeaderAndIsrRequest.PartitionState(0, 1, 1, brokerList, 0, brokerList, false)).asJava,
      Set(new Node(0, "host1", 0), new Node(1, "host2", 1)).asJava).build()
    replicaManager.becomeLeaderOrFollower(1, leaderAndIsrRequest2, (_, _) => ())

    val metadata: ClientMetadata = new DefaultClientMetadata("rack-a", "client-id",
      InetAddress.getByName("localhost"), KafkaPrincipal.ANONYMOUS, "default")

    val consumerResult = fetchAsConsumer(replicaManager, tp0,
      new PartitionData(0, 0, 100000, Optional.empty()),
      clientMetadata = Some(metadata))

    // Fetch from follower succeeds
    assertTrue(consumerResult.isFired)

    // But only leader will compute preferred replica
    assertTrue(consumerResult.assertFired.preferredReadReplica.isEmpty)
  }

  @Test
  def testPreferredReplicaAsLeader(): Unit = {
    val topicPartition = 0
    val followerBrokerId = 0
    val leaderBrokerId = 1
    val leaderEpoch = 1
    val leaderEpochIncrement = 2
    val countDownLatch = new CountDownLatch(1)

    // Prepare the mocked components for the test
    val (replicaManager, mockLogMgr) = prepareReplicaManagerAndLogManager(
      topicPartition, leaderEpoch + leaderEpochIncrement, followerBrokerId,
      leaderBrokerId, countDownLatch, expectTruncation = true)

    val brokerList = Seq[Integer](0, 1).asJava

    val tp0 = new TopicPartition(topic, 0)

    replicaManager.createPartition(new TopicPartition(topic, 0))

    // Make this replica the follower
    val leaderAndIsrRequest2 = new LeaderAndIsrRequest.Builder(ApiKeys.LEADER_AND_ISR.latestVersion, 0, 0, brokerEpoch,
      collection.immutable.Map(new TopicPartition(topic, 0) ->
        new LeaderAndIsrRequest.PartitionState(0, 0, 1, brokerList, 0, brokerList, false)).asJava,
      Set(new Node(0, "host1", 0), new Node(1, "host2", 1)).asJava).build()
    replicaManager.becomeLeaderOrFollower(1, leaderAndIsrRequest2, (_, _) => ())

    val metadata: ClientMetadata = new DefaultClientMetadata("rack-a", "client-id",
      InetAddress.getByName("localhost"), KafkaPrincipal.ANONYMOUS, "default")

    val consumerResult = fetchAsConsumer(replicaManager, tp0,
      new PartitionData(0, 0, 100000, Optional.empty()),
      clientMetadata = Some(metadata))

    // Fetch from follower succeeds
    assertTrue(consumerResult.isFired)

    // Returns a preferred replica (should just be the leader, which is None)
    assertFalse(consumerResult.assertFired.preferredReadReplica.isDefined)
  }

  @Test(expected = classOf[ClassNotFoundException])
  def testUnknownReplicaSelector(): Unit = {
    val topicPartition = 0
    val followerBrokerId = 0
    val leaderBrokerId = 1
    val leaderEpoch = 1
    val leaderEpochIncrement = 2
    val countDownLatch = new CountDownLatch(1)

    val props = new Properties()
    props.put(KafkaConfig.ReplicaSelectorClassProp, "non-a-class")
    val (replicaManager, mockLogMgr) = prepareReplicaManagerAndLogManager(
      topicPartition, leaderEpoch + leaderEpochIncrement, followerBrokerId,
      leaderBrokerId, countDownLatch, expectTruncation = true, extraProps = props)
  }

  @Test
  def testDefaultReplicaSelector(): Unit = {
    val topicPartition = 0
    val followerBrokerId = 0
    val leaderBrokerId = 1
    val leaderEpoch = 1
    val leaderEpochIncrement = 2
    val countDownLatch = new CountDownLatch(1)

    val (replicaManager, mockLogMgr) = prepareReplicaManagerAndLogManager(
      topicPartition, leaderEpoch + leaderEpochIncrement, followerBrokerId,
      leaderBrokerId, countDownLatch, expectTruncation = true)
    assertFalse(replicaManager.replicaSelectorOpt.isDefined)
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
                                                 expectTruncation: Boolean,
                                                 extraProps: Properties = new Properties()) : (ReplicaManager, LogManager) = {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect)
    props.put("log.dir", TestUtils.tempRelativeDir("data").getAbsolutePath)
    props.asScala ++= extraProps.asScala
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

      override def logEndOffset: Long = localLogOffset
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
      EasyMock.expect(metadataCache.getAliveBroker(EasyMock.eq(brokerId)))
        .andReturn(Option(createBroker(brokerId, s"host$brokerId", brokerId)))
        .anyTimes
    }
    EasyMock
      .expect(metadataCache.getPartitionReplicaEndpoints(
        EasyMock.anyObject(), EasyMock.anyObject()))
      .andReturn(Map(
        leaderBrokerId -> new Node(leaderBrokerId, "host1", 9092, "rack-a"),
        followerBrokerId -> new Node(followerBrokerId, "host2", 9092, "rack-b")).toMap
      )
      .anyTimes()
    EasyMock.replay(metadataCache)

    val timer = new MockTimer
    val mockProducePurgatory = new DelayedOperationPurgatory[DelayedProduce](
      purgatoryName = "Produce", timer, reaperEnabled = false)
    val mockFetchPurgatory = new DelayedOperationPurgatory[DelayedFetch](
      purgatoryName = "Fetch", timer, reaperEnabled = false)
    val mockDeleteRecordsPurgatory = new DelayedOperationPurgatory[DelayedDeleteRecords](
      purgatoryName = "DeleteRecords", timer, reaperEnabled = false)
    val mockElectLeaderPurgatory = new DelayedOperationPurgatory[DelayedElectLeader](
      purgatoryName = "ElectLeader", timer, reaperEnabled = false)

    // Mock network client to show leader offset of 5
    val quota = QuotaFactory.instantiate(config, metrics, time, "")
    val blockingSend = new ReplicaFetcherMockBlockingSend(Map(new TopicPartition(topic, topicPartition) ->
      new EpochEndOffset(leaderEpochFromLeader, offsetFromLeader)).asJava, BrokerEndPoint(1, "host1" ,1), time)
    val replicaManager = new ReplicaManager(config, metrics, time, kafkaZkClient, mockScheduler, mockLogMgr,
      new AtomicBoolean(false), quota, mockBrokerTopicStats,
      metadataCache, mockLogDirFailureChannel, mockProducePurgatory, mockFetchPurgatory,
      mockDeleteRecordsPurgatory, mockElectLeaderPurgatory, Option(this.getClass.getName)) {

      override protected def createReplicaFetcherManager(metrics: Metrics,
                                                     time: Time,
                                                     threadNamePrefix: Option[String],
                                                     quotaManager: ReplicationQuotaManager): ReplicaFetcherManager = {
        new ReplicaFetcherManager(config, this, metrics, time, threadNamePrefix, quotaManager) {

          override def createFetcherThread(fetcherId: Int, sourceBroker: BrokerEndPoint): ReplicaFetcherThread = {
            new ReplicaFetcherThread(s"ReplicaFetcherThread-$fetcherId", fetcherId,
              sourceBroker, config, failedPartitions, replicaManager, metrics, time, quota.follower, Some(blockingSend)) {

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
                              isolationLevel: IsolationLevel = IsolationLevel.READ_UNCOMMITTED,
                              clientMetadata: Option[ClientMetadata] = None): CallbackResult[FetchPartitionData] = {
    fetchMessages(replicaManager, replicaId = -1, partition, partitionData, minBytes, isolationLevel, clientMetadata)
  }

  private def fetchAsFollower(replicaManager: ReplicaManager,
                              partition: TopicPartition,
                              partitionData: PartitionData,
                              minBytes: Int = 0,
                              isolationLevel: IsolationLevel = IsolationLevel.READ_UNCOMMITTED,
                              clientMetadata: Option[ClientMetadata] = None): CallbackResult[FetchPartitionData] = {
    fetchMessages(replicaManager, replicaId = 1, partition, partitionData, minBytes, isolationLevel, clientMetadata)
  }

  private def fetchMessages(replicaManager: ReplicaManager,
                            replicaId: Int,
                            partition: TopicPartition,
                            partitionData: PartitionData,
                            minBytes: Int,
                            isolationLevel: IsolationLevel,
                            clientMetadata: Option[ClientMetadata]): CallbackResult[FetchPartitionData] = {
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
      quota = UnboundedQuota,
      responseCallback = fetchCallback,
      isolationLevel = isolationLevel,
      clientMetadata = clientMetadata
    )

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
      EasyMock.expect(metadataCache.getAliveBroker(EasyMock.eq(brokerId)))
        .andReturn(Option(createBroker(brokerId, s"host$brokerId", brokerId)))
        .anyTimes()
    }
    EasyMock.replay(metadataCache)

    val mockProducePurgatory = new DelayedOperationPurgatory[DelayedProduce](
      purgatoryName = "Produce", timer, reaperEnabled = false)
    val mockFetchPurgatory = new DelayedOperationPurgatory[DelayedFetch](
      purgatoryName = "Fetch", timer, reaperEnabled = false)
    val mockDeleteRecordsPurgatory = new DelayedOperationPurgatory[DelayedDeleteRecords](
      purgatoryName = "DeleteRecords", timer, reaperEnabled = false)
    val mockDelayedElectLeaderPurgatory = new DelayedOperationPurgatory[DelayedElectLeader](
      purgatoryName = "DelayedElectLeader", timer, reaperEnabled = false)

    new ReplicaManager(config, metrics, time, kafkaZkClient, new MockScheduler(time), mockLogMgr,
      new AtomicBoolean(false), QuotaFactory.instantiate(config, metrics, time, ""), new BrokerTopicStats,
      metadataCache, new LogDirFailureChannel(config.logDirs.size), mockProducePurgatory, mockFetchPurgatory,
      mockDeleteRecordsPurgatory, mockDelayedElectLeaderPurgatory, Option(this.getClass.getName))
  }

  @Test
  def testOldLeaderLosesMetricsWhenReassignPartitions(): Unit = {
    val controllerEpoch = 0
    val leaderEpoch = 0
    val leaderEpochIncrement = 1
    val correlationId = 0
    val controllerId = 0
    val (rm0, rm1, _, mockTopicStats1) = prepareDifferentReplicaManagersWithMockedBrokerTopicStats()

    EasyMock.expect(mockTopicStats1.removeOldLeaderMetrics(topic)).andVoid.once
    EasyMock.replay(mockTopicStats1)

    try {
      // make broker 0 the leader of partition 0 and
      // make broker 1 the leader of partition 1
      val tp0 = new TopicPartition(topic, 0)
      val tp1 = new TopicPartition(topic, 1)
      val partition0Replicas = Seq[Integer](0, 1).asJava
      val partition1Replicas = Seq[Integer](1, 0).asJava

      val leaderAndIsrRequest1 = new LeaderAndIsrRequest.Builder(ApiKeys.LEADER_AND_ISR.latestVersion,
        controllerId, 0, brokerEpoch,
        collection.immutable.Map(
          tp0 -> new LeaderAndIsrRequest.PartitionState(controllerEpoch, 0, leaderEpoch,
            partition0Replicas, 0, partition0Replicas, true),
          tp1 -> new LeaderAndIsrRequest.PartitionState(controllerEpoch, 1, leaderEpoch,
            partition1Replicas, 0, partition1Replicas, true)
        ).asJava,
        Set(new Node(0, "host0", 0), new Node(1, "host1", 1)).asJava).build()

      rm0.becomeLeaderOrFollower(correlationId, leaderAndIsrRequest1, (_, _) => ())
      rm1.becomeLeaderOrFollower(correlationId, leaderAndIsrRequest1, (_, _) => ())

      // make broker 0 the leader of partition 1 so broker 1 loses its leadership position
      val leaderAndIsrRequest2 = new LeaderAndIsrRequest.Builder(ApiKeys.LEADER_AND_ISR.latestVersion, controllerId,
        controllerEpoch, brokerEpoch,
        collection.immutable.Map(
          tp0 -> new LeaderAndIsrRequest.PartitionState(controllerEpoch, 0, leaderEpoch + leaderEpochIncrement,
            partition0Replicas, 0, partition0Replicas, true),
          tp1 -> new LeaderAndIsrRequest.PartitionState(controllerEpoch, 0, leaderEpoch + leaderEpochIncrement,
            partition1Replicas, 0, partition1Replicas, true)
        ).asJava,
        Set(new Node(0, "host0", 0), new Node(1, "host1", 1)).asJava).build()

      rm0.becomeLeaderOrFollower(correlationId, leaderAndIsrRequest2, (_, _) => ())
      rm1.becomeLeaderOrFollower(correlationId, leaderAndIsrRequest2, (_, _) => ())
    } finally {
      rm0.shutdown()
      rm1.shutdown()
    }

    // verify that broker 1 did remove its metrics when no longer being the leader of partition 1
    EasyMock.verify(mockTopicStats1)
  }

  @Test
  def testOldFollowerLosesMetricsWhenReassignPartitions(): Unit = {
    val controllerEpoch = 0
    val leaderEpoch = 0
    val leaderEpochIncrement = 1
    val correlationId = 0
    val controllerId = 0
    val (rm0, rm1, _, mockTopicStats1) = prepareDifferentReplicaManagersWithMockedBrokerTopicStats()

    EasyMock.expect(mockTopicStats1.removeOldLeaderMetrics(topic)).andVoid.once
    EasyMock.expect(mockTopicStats1.removeOldFollowerMetrics(topic)).andVoid.once
    EasyMock.replay(mockTopicStats1)

    try {
      // make broker 0 the leader of partition 0 and
      // make broker 1 the leader of partition 1
      val tp0 = new TopicPartition(topic, 0)
      val tp1 = new TopicPartition(topic, 1)
      val partition0Replicas = Seq[Integer](1, 0).asJava
      val partition1Replicas = Seq[Integer](1, 0).asJava

      val leaderAndIsrRequest1 = new LeaderAndIsrRequest.Builder(ApiKeys.LEADER_AND_ISR.latestVersion,
        controllerId, 0, brokerEpoch,
        collection.immutable.Map(
          tp0 -> new LeaderAndIsrRequest.PartitionState(controllerEpoch, 1, leaderEpoch,
            partition0Replicas, 0, partition0Replicas, true),
          tp1 -> new LeaderAndIsrRequest.PartitionState(controllerEpoch, 1, leaderEpoch,
            partition1Replicas, 0, partition1Replicas, true)
        ).asJava,
        Set(new Node(0, "host0", 0), new Node(1, "host1", 1)).asJava).build()

      rm0.becomeLeaderOrFollower(correlationId, leaderAndIsrRequest1, (_, _) => ())
      rm1.becomeLeaderOrFollower(correlationId, leaderAndIsrRequest1, (_, _) => ())

      // make broker 0 the leader of partition 1 so broker 1 loses its leadership position
      val leaderAndIsrRequest2 = new LeaderAndIsrRequest.Builder(ApiKeys.LEADER_AND_ISR.latestVersion, controllerId,
        controllerEpoch, brokerEpoch,
        collection.immutable.Map(
          tp0 -> new LeaderAndIsrRequest.PartitionState(controllerEpoch, 0, leaderEpoch + leaderEpochIncrement,
            partition0Replicas, 0, partition0Replicas, true),
          tp1 -> new LeaderAndIsrRequest.PartitionState(controllerEpoch, 0, leaderEpoch + leaderEpochIncrement,
            partition1Replicas, 0, partition1Replicas, true)
        ).asJava,
        Set(new Node(0, "host0", 0), new Node(1, "host1", 1)).asJava).build()

      rm0.becomeLeaderOrFollower(correlationId, leaderAndIsrRequest2, (_, _) => ())
      rm1.becomeLeaderOrFollower(correlationId, leaderAndIsrRequest2, (_, _) => ())
    } finally {
      rm0.shutdown()
      rm1.shutdown()
    }

    // verify that broker 1 did remove its metrics when no longer being the leader of partition 1
    EasyMock.verify(mockTopicStats1)
  }

  private def prepareDifferentReplicaManagersWithMockedBrokerTopicStats(): (ReplicaManager, ReplicaManager, BrokerTopicStats, BrokerTopicStats) = {
    val props0 = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect)
    val props1 = TestUtils.createBrokerConfig(1, TestUtils.MockZkConnect)

    props0.put("log0.dir", TestUtils.tempRelativeDir("data").getAbsolutePath)
    props1.put("log1.dir", TestUtils.tempRelativeDir("data").getAbsolutePath)

    val config0 = KafkaConfig.fromProps(props0)
    val config1 = KafkaConfig.fromProps(props1)

    val mockLogMgr0 = TestUtils.createLogManager(config0.logDirs.map(new File(_)))
    val mockLogMgr1 = TestUtils.createLogManager(config1.logDirs.map(new File(_)))

    val mockTopicStats0: BrokerTopicStats = EasyMock.createMock(classOf[BrokerTopicStats])
    val mockTopicStats1: BrokerTopicStats = EasyMock.createMock(classOf[BrokerTopicStats])

    val metadataCache0: MetadataCache = EasyMock.createMock(classOf[MetadataCache])
    val metadataCache1: MetadataCache = EasyMock.createMock(classOf[MetadataCache])

    val aliveBrokers = Seq(createBroker(0, "host0", 0), createBroker(1, "host1", 1))

    EasyMock.expect(metadataCache0.getAliveBrokers).andReturn(aliveBrokers).anyTimes()
    EasyMock.replay(metadataCache0)
    EasyMock.expect(metadataCache1.getAliveBrokers).andReturn(aliveBrokers).anyTimes()
    EasyMock.replay(metadataCache1)

    // each replica manager is for a broker
    val rm0 = new ReplicaManager(config0, metrics, time, kafkaZkClient, new MockScheduler(time), mockLogMgr0,
      new AtomicBoolean(false), QuotaFactory.instantiate(config0, metrics, time, ""),
      mockTopicStats0, metadataCache0, new LogDirFailureChannel(config0.logDirs.size))
    val rm1 = new ReplicaManager(config1, metrics, time, kafkaZkClient, new MockScheduler(time), mockLogMgr1,
      new AtomicBoolean(false), QuotaFactory.instantiate(config1, metrics, time, ""),
      mockTopicStats1, metadataCache1, new LogDirFailureChannel(config1.logDirs.size))

    (rm0, rm1, mockTopicStats0, mockTopicStats1)
  }

}
