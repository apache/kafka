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

import kafka.api._
import kafka.cluster.{BrokerEndPoint, Partition}
import kafka.log._
import kafka.server.QuotaFactory.{QuotaManagers, UnboundedQuota}
import kafka.server.checkpoints.{LazyOffsetCheckpoints, OffsetCheckpointFile}
import kafka.server.epoch.util.ReplicaFetcherMockBlockingSend
import kafka.server.metadata.CachedConfigRepository
import kafka.utils.TestUtils.createBroker
import kafka.utils.timer.MockTimer
import kafka.utils.{MockScheduler, MockTime, TestUtils}
import org.apache.kafka.common.message.FetchResponseData
import org.apache.kafka.common.message.LeaderAndIsrRequestData
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.EpochEndOffset
import org.apache.kafka.common.message.StopReplicaRequestData.StopReplicaPartitionState
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record._
import org.apache.kafka.common.replica.ClientMetadata
import org.apache.kafka.common.replica.ClientMetadata.DefaultClientMetadata
import org.apache.kafka.common.requests.FetchRequest.PartitionData
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.requests._
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.utils.{Time, Utils}
import org.apache.kafka.common.{IsolationLevel, Node, TopicPartition, Uuid}
import org.easymock.EasyMock
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.mockito.Mockito

import java.io.File
import java.net.InetAddress
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.{Collections, Optional, Properties}
import scala.collection.{Map, Seq, mutable}
import scala.jdk.CollectionConverters._

class ReplicaManagerTest {

  val topic = "test-topic"
  val time = new MockTime
  val scheduler = new MockScheduler(time)
  val metrics = new Metrics
  val configRepository = new CachedConfigRepository()
  var alterIsrManager: AlterIsrManager = _
  var config: KafkaConfig = _
  var quotaManager: QuotaManagers = _

  // Constants defined for readability
  val zkVersion = 0
  val correlationId = 0
  var controllerEpoch = 0
  val brokerEpoch = 0L

  @BeforeEach
  def setUp(): Unit = {
    val props = TestUtils.createBrokerConfig(1, TestUtils.MockZkConnect)
    config = KafkaConfig.fromProps(props)
    alterIsrManager = EasyMock.createMock(classOf[AlterIsrManager])
    quotaManager = QuotaFactory.instantiate(config, metrics, time, "")
  }

  @AfterEach
  def tearDown(): Unit = {
    TestUtils.clearYammerMetrics()
    Option(quotaManager).foreach(_.shutdown())
    metrics.close()
  }

  @Test
  def testHighWaterMarkDirectoryMapping(): Unit = {
    val mockLogMgr = TestUtils.createLogManager(config.logDirs.map(new File(_)))
    val rm = new ReplicaManager(config, metrics, time, None, new MockScheduler(time), mockLogMgr,
      new AtomicBoolean(false), quotaManager, new BrokerTopicStats,
      MetadataCache.zkMetadataCache(config.brokerId), new LogDirFailureChannel(config.logDirs.size), alterIsrManager, configRepository)
    try {
      val partition = rm.createPartition(new TopicPartition(topic, 1))
      partition.createLogIfNotExists(isNew = false, isFutureReplica = false,
        new LazyOffsetCheckpoints(rm.highWatermarkCheckpoints), None)
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
    val rm = new ReplicaManager(config, metrics, time, None, new MockScheduler(time), mockLogMgr,
      new AtomicBoolean(false), quotaManager, new BrokerTopicStats,
      MetadataCache.zkMetadataCache(config.brokerId), new LogDirFailureChannel(config.logDirs.size), alterIsrManager, configRepository)
    try {
      val partition = rm.createPartition(new TopicPartition(topic, 1))
      partition.createLogIfNotExists(isNew = false, isFutureReplica = false,
        new LazyOffsetCheckpoints(rm.highWatermarkCheckpoints), None)
      rm.checkpointHighWatermarks()
    } finally {
      // shutdown the replica manager upon test completion
      rm.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testIllegalRequiredAcks(): Unit = {
    val mockLogMgr = TestUtils.createLogManager(config.logDirs.map(new File(_)))
    val rm = new ReplicaManager(config, metrics, time, None, new MockScheduler(time), mockLogMgr,
      new AtomicBoolean(false), quotaManager, new BrokerTopicStats,
      MetadataCache.zkMetadataCache(config.brokerId), new LogDirFailureChannel(config.logDirs.size), alterIsrManager, configRepository, Option(this.getClass.getName))
    try {
      def callback(responseStatus: Map[TopicPartition, PartitionResponse]) = {
        assert(responseStatus.values.head.error == Errors.INVALID_REQUIRED_ACKS)
      }
      rm.appendRecords(
        timeout = 0,
        requiredAcks = 3,
        internalTopicsAllowed = false,
        origin = AppendOrigin.Client,
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
    val rm = new ReplicaManager(config, metrics, time, None, new MockScheduler(time), mockLogMgr,
      new AtomicBoolean(false), quotaManager, new BrokerTopicStats,
      metadataCache, new LogDirFailureChannel(config.logDirs.size), alterIsrManager, configRepository)

    try {
      val brokerList = Seq[Integer](0, 1).asJava
      val topicIds = Collections.singletonMap(topic, Uuid.randomUuid())

      val partition = rm.createPartition(new TopicPartition(topic, 0))
      partition.createLogIfNotExists(isNew = false, isFutureReplica = false,
        new LazyOffsetCheckpoints(rm.highWatermarkCheckpoints), None)
      // Make this replica the leader.
      val leaderAndIsrRequest1 = new LeaderAndIsrRequest.Builder(ApiKeys.LEADER_AND_ISR.latestVersion, 0, 0, brokerEpoch,
        Seq(new LeaderAndIsrPartitionState()
          .setTopicName(topic)
          .setPartitionIndex(0)
          .setControllerEpoch(0)
          .setLeader(0)
          .setLeaderEpoch(0)
          .setIsr(brokerList)
          .setZkVersion(0)
          .setReplicas(brokerList)
          .setIsNew(false)).asJava,
        topicIds,
        Set(new Node(0, "host1", 0), new Node(1, "host2", 1)).asJava).build()
      rm.becomeLeaderOrFollower(0, leaderAndIsrRequest1, (_, _) => ())
      rm.getPartitionOrException(new TopicPartition(topic, 0))
          .localLogOrException

      val records = MemoryRecords.withRecords(CompressionType.NONE, new SimpleRecord("first message".getBytes()))
      val appendResult = appendRecords(rm, new TopicPartition(topic, 0), records).onFire { response =>
        assertEquals(Errors.NOT_LEADER_OR_FOLLOWER, response.error)
      }

      // Make this replica the follower
      val leaderAndIsrRequest2 = new LeaderAndIsrRequest.Builder(ApiKeys.LEADER_AND_ISR.latestVersion, 0, 0, brokerEpoch,
        Seq(new LeaderAndIsrPartitionState()
          .setTopicName(topic)
          .setPartitionIndex(0)
          .setControllerEpoch(0)
          .setLeader(1)
          .setLeaderEpoch(1)
          .setIsr(brokerList)
          .setZkVersion(0)
          .setReplicas(brokerList)
          .setIsNew(false)).asJava,
        topicIds,
        Set(new Node(0, "host1", 0), new Node(1, "host2", 1)).asJava).build()
      rm.becomeLeaderOrFollower(1, leaderAndIsrRequest2, (_, _) => ())

      assertTrue(appendResult.isFired)
    } finally {
      rm.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testFencedErrorCausedByBecomeLeader(): Unit = {
    testFencedErrorCausedByBecomeLeader(0)
    testFencedErrorCausedByBecomeLeader(1)
    testFencedErrorCausedByBecomeLeader(10)
  }

  private[this] def testFencedErrorCausedByBecomeLeader(loopEpochChange: Int): Unit = {
    val replicaManager = setupReplicaManagerWithMockedPurgatories(new MockTimer(time))
    try {
      val brokerList = Seq[Integer](0, 1).asJava
      val topicPartition = new TopicPartition(topic, 0)
      replicaManager.createPartition(topicPartition)
        .createLogIfNotExists(isNew = false, isFutureReplica = false,
          new LazyOffsetCheckpoints(replicaManager.highWatermarkCheckpoints), None)
      val topicIds = Collections.singletonMap(topic, Uuid.randomUuid())

      def leaderAndIsrRequest(epoch: Int): LeaderAndIsrRequest = new LeaderAndIsrRequest.Builder(ApiKeys.LEADER_AND_ISR.latestVersion, 0, 0, brokerEpoch,
        Seq(new LeaderAndIsrPartitionState()
          .setTopicName(topic)
          .setPartitionIndex(0)
          .setControllerEpoch(0)
          .setLeader(0)
          .setLeaderEpoch(epoch)
          .setIsr(brokerList)
          .setZkVersion(0)
          .setReplicas(brokerList)
          .setIsNew(true)).asJava,
        topicIds,
        Set(new Node(0, "host1", 0), new Node(1, "host2", 1)).asJava).build()

      replicaManager.becomeLeaderOrFollower(0, leaderAndIsrRequest(0), (_, _) => ())
      val partition = replicaManager.getPartitionOrException(new TopicPartition(topic, 0))
      assertEquals(1, replicaManager.logManager.liveLogDirs.filterNot(_ == partition.log.get.dir.getParentFile).size)

      val previousReplicaFolder = partition.log.get.dir.getParentFile
      // find the live and different folder
      val newReplicaFolder = replicaManager.logManager.liveLogDirs.filterNot(_ == partition.log.get.dir.getParentFile).head
      assertEquals(0, replicaManager.replicaAlterLogDirsManager.fetcherThreadMap.size)
      replicaManager.alterReplicaLogDirs(Map(topicPartition -> newReplicaFolder.getAbsolutePath))
      // make sure the future log is created
      replicaManager.futureLocalLogOrException(topicPartition)
      assertEquals(1, replicaManager.replicaAlterLogDirsManager.fetcherThreadMap.size)
      (1 to loopEpochChange).foreach(epoch => replicaManager.becomeLeaderOrFollower(0, leaderAndIsrRequest(epoch), (_, _) => ()))
      // wait for the ReplicaAlterLogDirsThread to complete
      TestUtils.waitUntilTrue(() => {
        replicaManager.replicaAlterLogDirsManager.shutdownIdleFetcherThreads()
        replicaManager.replicaAlterLogDirsManager.fetcherThreadMap.isEmpty
      }, s"ReplicaAlterLogDirsThread should be gone")

      // the fenced error should be recoverable
      assertEquals(0, replicaManager.replicaAlterLogDirsManager.failedPartitions.size)
      // the replica change is completed after retrying
      assertTrue(partition.futureLog.isEmpty)
      assertEquals(newReplicaFolder.getAbsolutePath, partition.log.get.dir.getParent)
      // change the replica folder again
      val response = replicaManager.alterReplicaLogDirs(Map(topicPartition -> previousReplicaFolder.getAbsolutePath))
      assertNotEquals(0, response.size)
      response.values.foreach(assertEquals(Errors.NONE, _))
      // should succeed to invoke ReplicaAlterLogDirsThread again
      assertEquals(1, replicaManager.replicaAlterLogDirsManager.fetcherThreadMap.size)
    } finally replicaManager.shutdown(checkpointHW = false)
  }

  @Test
  def testReceiveOutOfOrderSequenceExceptionWithLogStartOffset(): Unit = {
    val timer = new MockTimer(time)
    val replicaManager = setupReplicaManagerWithMockedPurgatories(timer)

    try {
      val brokerList = Seq[Integer](0, 1).asJava

      val partition = replicaManager.createPartition(new TopicPartition(topic, 0))
      partition.createLogIfNotExists(isNew = false, isFutureReplica = false,
        new LazyOffsetCheckpoints(replicaManager.highWatermarkCheckpoints), None)

      // Make this replica the leader.
      val leaderAndIsrRequest1 = new LeaderAndIsrRequest.Builder(ApiKeys.LEADER_AND_ISR.latestVersion, 0, 0, brokerEpoch,
        Seq(new LeaderAndIsrPartitionState()
          .setTopicName(topic)
          .setPartitionIndex(0)
          .setControllerEpoch(0)
          .setLeader(0)
          .setLeaderEpoch(0)
          .setIsr(brokerList)
          .setZkVersion(0)
          .setReplicas(brokerList)
          .setIsNew(true)).asJava,
        Collections.singletonMap(topic, Uuid.randomUuid()),
        Set(new Node(0, "host1", 0), new Node(1, "host2", 1)).asJava).build()
      replicaManager.becomeLeaderOrFollower(0, leaderAndIsrRequest1, (_, _) => ())
      replicaManager.getPartitionOrException(new TopicPartition(topic, 0))
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
    val timer = new MockTimer(time)
    val replicaManager = setupReplicaManagerWithMockedPurgatories(timer)

    try {
      val brokerList = Seq[Integer](0, 1).asJava

      val partition = replicaManager.createPartition(new TopicPartition(topic, 0))
      partition.createLogIfNotExists(isNew = false, isFutureReplica = false,
        new LazyOffsetCheckpoints(replicaManager.highWatermarkCheckpoints), None)

      // Make this replica the leader.
      val leaderAndIsrRequest1 = new LeaderAndIsrRequest.Builder(ApiKeys.LEADER_AND_ISR.latestVersion, 0, 0, brokerEpoch,
        Seq(new LeaderAndIsrPartitionState()
          .setTopicName(topic)
          .setPartitionIndex(0)
          .setControllerEpoch(0)
          .setLeader(0)
          .setLeaderEpoch(0)
          .setIsr(brokerList)
          .setZkVersion(0)
          .setReplicas(brokerList)
          .setIsNew(true)).asJava,
        Collections.singletonMap(topic, Uuid.randomUuid()),
        Set(new Node(0, "host1", 0), new Node(1, "host2", 1)).asJava).build()
      replicaManager.becomeLeaderOrFollower(0, leaderAndIsrRequest1, (_, _) => ())
      replicaManager.getPartitionOrException(new TopicPartition(topic, 0))
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
      assertEquals(Some(List.empty[FetchResponseData.AbortedTransaction]), fetchData.abortedTransactions)

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
      assertEquals(Some(List.empty[FetchResponseData.AbortedTransaction]), fetchData.abortedTransactions)

      // now commit the transaction
      val endTxnMarker = new EndTransactionMarker(ControlRecordType.COMMIT, 0)
      val commitRecordBatch = MemoryRecords.withEndTransactionMarker(producerId, epoch, endTxnMarker)
      appendRecords(replicaManager, new TopicPartition(topic, 0), commitRecordBatch,
        origin = AppendOrigin.Coordinator)
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
      assertEquals(Some(List.empty[FetchResponseData.AbortedTransaction]), fetchData.abortedTransactions)
      assertEquals(numRecords + 1, fetchData.records.batches.asScala.size)
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testDelayedFetchIncludesAbortedTransactions(): Unit = {
    val timer = new MockTimer(time)
    val replicaManager = setupReplicaManagerWithMockedPurgatories(timer)

    try {
      val brokerList = Seq[Integer](0, 1).asJava
      val partition = replicaManager.createPartition(new TopicPartition(topic, 0))
      partition.createLogIfNotExists(isNew = false, isFutureReplica = false,
        new LazyOffsetCheckpoints(replicaManager.highWatermarkCheckpoints), None)

      // Make this replica the leader.
      val leaderAndIsrRequest1 = new LeaderAndIsrRequest.Builder(ApiKeys.LEADER_AND_ISR.latestVersion, 0, 0, brokerEpoch,
        Seq(new LeaderAndIsrPartitionState()
          .setTopicName(topic)
          .setPartitionIndex(0)
          .setControllerEpoch(0)
          .setLeader(0)
          .setLeaderEpoch(0)
          .setIsr(brokerList)
          .setZkVersion(0)
          .setReplicas(brokerList)
          .setIsNew(true)).asJava,
        Collections.singletonMap(topic, Uuid.randomUuid()),
        Set(new Node(0, "host1", 0), new Node(1, "host2", 1)).asJava).build()
      replicaManager.becomeLeaderOrFollower(0, leaderAndIsrRequest1, (_, _) => ())
      replicaManager.getPartitionOrException(new TopicPartition(topic, 0))
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
      appendRecords(replicaManager, new TopicPartition(topic, 0), abortRecordBatch,
        origin = AppendOrigin.Coordinator)
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
  def testFetchBeyondHighWatermark(): Unit = {
    val rm = setupReplicaManagerWithMockedPurgatories(new MockTimer(time), aliveBrokerIds = Seq(0, 1, 2))
    try {
      val brokerList = Seq[Integer](0, 1, 2).asJava

      val partition = rm.createPartition(new TopicPartition(topic, 0))
      partition.createLogIfNotExists(isNew = false, isFutureReplica = false,
        new LazyOffsetCheckpoints(rm.highWatermarkCheckpoints), None)

      // Make this replica the leader.
      val leaderAndIsrRequest1 = new LeaderAndIsrRequest.Builder(ApiKeys.LEADER_AND_ISR.latestVersion, 0, 0, brokerEpoch,
        Seq(new LeaderAndIsrPartitionState()
          .setTopicName(topic)
          .setPartitionIndex(0)
          .setControllerEpoch(0)
          .setLeader(0)
          .setLeaderEpoch(0)
          .setIsr(brokerList)
          .setZkVersion(0)
          .setReplicas(brokerList)
          .setIsNew(false)).asJava,
        Collections.singletonMap(topic, Uuid.randomUuid()),
        Set(new Node(0, "host1", 0), new Node(1, "host2", 1), new Node(2, "host2", 2)).asJava).build()
      rm.becomeLeaderOrFollower(0, leaderAndIsrRequest1, (_, _) => ())
      rm.getPartitionOrException(new TopicPartition(topic, 0))
        .localLogOrException

      // Append a couple of messages.
      for(i <- 1 to 2) {
        val records = TestUtils.singletonRecords(s"message $i".getBytes)
        appendRecords(rm, new TopicPartition(topic, 0), records).onFire { response =>
          assertEquals(Errors.NONE, response.error)
        }
      }

      // Followers are always allowed to fetch above the high watermark
      val followerFetchResult = fetchAsFollower(rm, new TopicPartition(topic, 0),
        new PartitionData(1, 0, 100000, Optional.empty()))
      val followerFetchData = followerFetchResult.assertFired
      assertEquals(Errors.NONE, followerFetchData.error, "Should not give an exception")
      assertTrue(followerFetchData.records.batches.iterator.hasNext, "Should return some data")

      // Consumers are not allowed to consume above the high watermark. However, since the
      // high watermark could be stale at the time of the request, we do not return an out of
      // range error and instead return an empty record set.
      val consumerFetchResult = fetchAsConsumer(rm, new TopicPartition(topic, 0),
        new PartitionData(1, 0, 100000, Optional.empty()))
      val consumerFetchData = consumerFetchResult.assertFired
      assertEquals(Errors.NONE, consumerFetchData.error, "Should not give an exception")
      assertEquals(MemoryRecords.EMPTY, consumerFetchData.records, "Should return empty response")
    } finally {
      rm.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testFollowerStateNotUpdatedIfLogReadFails(): Unit = {
    val maxFetchBytes = 1024 * 1024
    val aliveBrokersIds = Seq(0, 1)
    val leaderEpoch = 5
    val replicaManager = setupReplicaManagerWithMockedPurgatories(new MockTimer(time),
      brokerId = 0, aliveBrokersIds)
    try {
      val tp = new TopicPartition(topic, 0)
      val replicas = aliveBrokersIds.toList.map(Int.box).asJava

      // Broker 0 becomes leader of the partition
      val leaderAndIsrPartitionState = new LeaderAndIsrPartitionState()
        .setTopicName(topic)
        .setPartitionIndex(0)
        .setControllerEpoch(0)
        .setLeader(0)
        .setLeaderEpoch(leaderEpoch)
        .setIsr(replicas)
        .setZkVersion(0)
        .setReplicas(replicas)
        .setIsNew(true)
      val leaderAndIsrRequest = new LeaderAndIsrRequest.Builder(ApiKeys.LEADER_AND_ISR.latestVersion, 0, 0, brokerEpoch,
        Seq(leaderAndIsrPartitionState).asJava,
        Collections.singletonMap(topic, Uuid.randomUuid()),
        Set(new Node(0, "host1", 0), new Node(1, "host2", 1)).asJava).build()
      val leaderAndIsrResponse = replicaManager.becomeLeaderOrFollower(0, leaderAndIsrRequest, (_, _) => ())
      assertEquals(Errors.NONE, leaderAndIsrResponse.error)

      // Follower replica state is initialized, but initial state is not known
      assertTrue(replicaManager.onlinePartition(tp).isDefined)
      val partition = replicaManager.onlinePartition(tp).get

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
    val replicaManager = setupReplicaManagerWithMockedPurgatories(new MockTimer(time), aliveBrokerIds = Seq(0, 1, 2))

    try {
      // Create 2 partitions, assign replica 0 as the leader for both a different follower (1 and 2) for each
      val tp0 = new TopicPartition(topic, 0)
      val tp1 = new TopicPartition(topic, 1)
      val offsetCheckpoints = new LazyOffsetCheckpoints(replicaManager.highWatermarkCheckpoints)
      replicaManager.createPartition(tp0).createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints, None)
      replicaManager.createPartition(tp1).createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints, None)
      val partition0Replicas = Seq[Integer](0, 1).asJava
      val partition1Replicas = Seq[Integer](0, 2).asJava
      val topicIds = Map(tp0.topic -> Uuid.randomUuid(), tp1.topic -> Uuid.randomUuid()).asJava
      val leaderAndIsrRequest = new LeaderAndIsrRequest.Builder(ApiKeys.LEADER_AND_ISR.latestVersion, 0, 0, brokerEpoch,
        Seq(
          new LeaderAndIsrPartitionState()
            .setTopicName(tp0.topic)
            .setPartitionIndex(tp0.partition)
            .setControllerEpoch(0)
            .setLeader(0)
            .setLeaderEpoch(0)
            .setIsr(partition0Replicas)
            .setZkVersion(0)
            .setReplicas(partition0Replicas)
            .setIsNew(true),
          new LeaderAndIsrPartitionState()
            .setTopicName(tp1.topic)
            .setPartitionIndex(tp1.partition)
            .setControllerEpoch(0)
            .setLeader(0)
            .setLeaderEpoch(0)
            .setIsr(partition1Replicas)
            .setZkVersion(0)
            .setReplicas(partition1Replicas)
            .setIsNew(true)
        ).asJava,
        topicIds,
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
      assertEquals(1, tp0Log.get.highWatermark, "hw should be incremented")

      val tp1Replica = replicaManager.localLog(tp1)
      assertTrue(tp1Replica.isDefined)
      assertEquals(0, tp1Replica.get.highWatermark, "hw should not be incremented")

    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testBecomeFollowerWhenLeaderIsUnchangedButMissedLeaderUpdate(): Unit = {
    verifyBecomeFollowerWhenLeaderIsUnchangedButMissedLeaderUpdate(new Properties, expectTruncation = false)
  }

  @Test
  def testBecomeFollowerWhenLeaderIsUnchangedButMissedLeaderUpdateIbp26(): Unit = {
    val extraProps = new Properties
    extraProps.put(KafkaConfig.InterBrokerProtocolVersionProp, KAFKA_2_6_IV0.version)
    verifyBecomeFollowerWhenLeaderIsUnchangedButMissedLeaderUpdate(extraProps, expectTruncation = true)
  }

  /**
   * If a partition becomes a follower and the leader is unchanged it should check for truncation
   * if the epoch has increased by more than one (which suggests it has missed an update). For
   * IBP version 2.7 onwards, we don't require this since we can truncate at any time based
   * on diverging epochs returned in fetch responses.
   */
  private def verifyBecomeFollowerWhenLeaderIsUnchangedButMissedLeaderUpdate(extraProps: Properties,
                                                                             expectTruncation: Boolean): Unit = {
    val topicPartition = 0
    val topicId = Uuid.randomUuid()
    val followerBrokerId = 0
    val leaderBrokerId = 1
    val controllerId = 0
    val controllerEpoch = 0
    var leaderEpoch = 1
    val leaderEpochIncrement = 2
    val aliveBrokerIds = Seq[Integer] (followerBrokerId, leaderBrokerId)
    val countDownLatch = new CountDownLatch(1)

    // Prepare the mocked components for the test
    val (replicaManager, mockLogMgr) = prepareReplicaManagerAndLogManager(new MockTimer(time),
      topicPartition, leaderEpoch + leaderEpochIncrement, followerBrokerId, leaderBrokerId, countDownLatch,
      expectTruncation = expectTruncation, localLogOffset = Some(10), extraProps = extraProps, topicId = Some(topicId))

    // Initialize partition state to follower, with leader = 1, leaderEpoch = 1
    val tp = new TopicPartition(topic, topicPartition)
    val partition = replicaManager.createPartition(tp)
    val offsetCheckpoints = new LazyOffsetCheckpoints(replicaManager.highWatermarkCheckpoints)
    partition.createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints, None)
    partition.makeFollower(
      leaderAndIsrPartitionState(tp, leaderEpoch, leaderBrokerId, aliveBrokerIds),
      offsetCheckpoints,
      None)

    // Make local partition a follower - because epoch increased by more than 1, truncation should
    // trigger even though leader does not change
    leaderEpoch += leaderEpochIncrement
    val leaderAndIsrRequest0 = new LeaderAndIsrRequest.Builder(ApiKeys.LEADER_AND_ISR.latestVersion,
      controllerId, controllerEpoch, brokerEpoch,
      Seq(leaderAndIsrPartitionState(tp, leaderEpoch, leaderBrokerId, aliveBrokerIds)).asJava,
      Collections.singletonMap(topic, topicId),
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
    val leaderEpoch = 1
    val leaderEpochIncrement = 2
    val aliveBrokerIds = Seq[Integer] (followerBrokerId, leaderBrokerId)
    val countDownLatch = new CountDownLatch(1)

    // Prepare the mocked components for the test
    val (replicaManager, _) = prepareReplicaManagerAndLogManager(new MockTimer(time),
      topicPartition, leaderEpoch + leaderEpochIncrement, followerBrokerId,
      leaderBrokerId, countDownLatch, expectTruncation = true)

    val tp = new TopicPartition(topic, topicPartition)
    val partition = replicaManager.createPartition(tp)

    val offsetCheckpoints = new LazyOffsetCheckpoints(replicaManager.highWatermarkCheckpoints)
    partition.createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints, None)
    partition.makeLeader(
      leaderAndIsrPartitionState(tp, leaderEpoch, leaderBrokerId, aliveBrokerIds),
      offsetCheckpoints,
      None)

    val metadata: ClientMetadata = new DefaultClientMetadata("rack-a", "client-id",
      InetAddress.getByName("localhost"), KafkaPrincipal.ANONYMOUS, "default")

    // We expect to select the leader, which means we return None
    val preferredReadReplica: Option[Int] = replicaManager.findPreferredReadReplica(
      partition, metadata, Request.OrdinaryConsumerId, 1L, System.currentTimeMillis)
    assertFalse(preferredReadReplica.isDefined)
  }

  @Test
  def testPreferredReplicaAsFollower(): Unit = {
    val topicPartition = 0
    val topicId = Uuid.randomUuid()
    val followerBrokerId = 0
    val leaderBrokerId = 1
    val leaderEpoch = 1
    val leaderEpochIncrement = 2
    val countDownLatch = new CountDownLatch(1)

    // Prepare the mocked components for the test
    val (replicaManager, _) = prepareReplicaManagerAndLogManager(new MockTimer(time),
      topicPartition, leaderEpoch + leaderEpochIncrement, followerBrokerId,
      leaderBrokerId, countDownLatch, expectTruncation = true, topicId = Some(topicId))

    val brokerList = Seq[Integer](0, 1).asJava

    val tp0 = new TopicPartition(topic, 0)

    initializeLogAndTopicId(replicaManager, tp0, topicId)

    // Make this replica the follower
    val leaderAndIsrRequest2 = new LeaderAndIsrRequest.Builder(ApiKeys.LEADER_AND_ISR.latestVersion, 0, 0, brokerEpoch,
      Seq(new LeaderAndIsrPartitionState()
        .setTopicName(topic)
        .setPartitionIndex(0)
        .setControllerEpoch(0)
        .setLeader(1)
        .setLeaderEpoch(1)
        .setIsr(brokerList)
        .setZkVersion(0)
        .setReplicas(brokerList)
        .setIsNew(false)).asJava,
      Collections.singletonMap(topic, topicId),
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
    val topicId = Uuid.randomUuid()
    val followerBrokerId = 0
    val leaderBrokerId = 1
    val leaderEpoch = 1
    val leaderEpochIncrement = 2
    val countDownLatch = new CountDownLatch(1)

    // Prepare the mocked components for the test
    val (replicaManager, _) = prepareReplicaManagerAndLogManager(new MockTimer(time),
      topicPartition, leaderEpoch + leaderEpochIncrement, followerBrokerId,
      leaderBrokerId, countDownLatch, expectTruncation = true, topicId = Some(topicId))

    val brokerList = Seq[Integer](0, 1).asJava

    val tp0 = new TopicPartition(topic, 0)

    initializeLogAndTopicId(replicaManager, tp0, topicId)

    // Make this replica the follower
    val leaderAndIsrRequest2 = new LeaderAndIsrRequest.Builder(ApiKeys.LEADER_AND_ISR.latestVersion, 0, 0, brokerEpoch,
      Seq(new LeaderAndIsrPartitionState()
        .setTopicName(topic)
        .setPartitionIndex(0)
        .setControllerEpoch(0)
        .setLeader(0)
        .setLeaderEpoch(1)
        .setIsr(brokerList)
        .setZkVersion(0)
        .setReplicas(brokerList)
        .setIsNew(false)).asJava,
      Collections.singletonMap(topic, topicId),
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

  @Test
  def testFollowerFetchWithDefaultSelectorNoForcedHwPropagation(): Unit = {
    val topicPartition = 0
    val topicId = Uuid.randomUuid()
    val followerBrokerId = 0
    val leaderBrokerId = 1
    val leaderEpoch = 1
    val leaderEpochIncrement = 2
    val countDownLatch = new CountDownLatch(1)
    val timer = new MockTimer(time)

    // Prepare the mocked components for the test
    val (replicaManager, _) = prepareReplicaManagerAndLogManager(timer,
      topicPartition, leaderEpoch + leaderEpochIncrement, followerBrokerId,
      leaderBrokerId, countDownLatch, expectTruncation = true, topicId = Some(topicId))

    val brokerList = Seq[Integer](0, 1).asJava

    val tp0 = new TopicPartition(topic, 0)

    initializeLogAndTopicId(replicaManager, tp0, topicId)

    // Make this replica the follower
    val leaderAndIsrRequest2 = new LeaderAndIsrRequest.Builder(ApiKeys.LEADER_AND_ISR.latestVersion, 0, 0, brokerEpoch,
      Seq(new LeaderAndIsrPartitionState()
        .setTopicName(topic)
        .setPartitionIndex(0)
        .setControllerEpoch(0)
        .setLeader(0)
        .setLeaderEpoch(1)
        .setIsr(brokerList)
        .setZkVersion(0)
        .setReplicas(brokerList)
        .setIsNew(false)).asJava,
      Collections.singletonMap(topic, topicId),
      Set(new Node(0, "host1", 0), new Node(1, "host2", 1)).asJava).build()
    replicaManager.becomeLeaderOrFollower(1, leaderAndIsrRequest2, (_, _) => ())

    val simpleRecords = Seq(new SimpleRecord("a".getBytes), new SimpleRecord("b".getBytes))
    val appendResult = appendRecords(replicaManager, tp0,
      MemoryRecords.withRecords(CompressionType.NONE, simpleRecords.toSeq: _*), AppendOrigin.Client)

    // Increment the hw in the leader by fetching from the last offset
    val fetchOffset = simpleRecords.size
    var followerResult = fetchAsFollower(replicaManager, tp0,
      new PartitionData(fetchOffset, 0, 100000, Optional.empty()),
      clientMetadata = None)
    assertTrue(followerResult.isFired)
    assertEquals(0, followerResult.assertFired.highWatermark)

    assertTrue(appendResult.isFired, "Expected producer request to be acked")

    // Fetch from the same offset, no new data is expected and hence the fetch request should
    // go to the purgatory
    followerResult = fetchAsFollower(replicaManager, tp0,
      new PartitionData(fetchOffset, 0, 100000, Optional.empty()),
      clientMetadata = None, minBytes = 1000)
    assertFalse(followerResult.isFired, "Request completed immediately unexpectedly")

    // Complete the request in the purgatory by advancing the clock
    timer.advanceClock(1001)
    assertTrue(followerResult.isFired)

    assertEquals(fetchOffset, followerResult.assertFired.highWatermark)
  }

  @Test
  def testUnknownReplicaSelector(): Unit = {
    val topicPartition = 0
    val followerBrokerId = 0
    val leaderBrokerId = 1
    val leaderEpoch = 1
    val leaderEpochIncrement = 2
    val countDownLatch = new CountDownLatch(1)

    val props = new Properties()
    props.put(KafkaConfig.ReplicaSelectorClassProp, "non-a-class")
    assertThrows(classOf[ClassNotFoundException], () => prepareReplicaManagerAndLogManager(new MockTimer(time),
      topicPartition, leaderEpoch + leaderEpochIncrement, followerBrokerId,
      leaderBrokerId, countDownLatch, expectTruncation = true, extraProps = props))
  }

  // Due to some limitations to EasyMock, we need to create the log so that the Partition.topicId does not call
  // LogManager.getLog with a default argument
  // TODO: convert tests to using Mockito to avoid this issue.
  private def initializeLogAndTopicId(replicaManager: ReplicaManager, topicPartition: TopicPartition, topicId: Uuid): Unit = {
    val partition = replicaManager.createPartition(new TopicPartition(topic, 0))
    val log = replicaManager.logManager.getOrCreateLog(topicPartition, false, false, Some(topicId))
    log.topicId = Some(topicId)
    partition.log = Some(log)
  }

  @Test
  def testDefaultReplicaSelector(): Unit = {
    val topicPartition = 0
    val followerBrokerId = 0
    val leaderBrokerId = 1
    val leaderEpoch = 1
    val leaderEpochIncrement = 2
    val countDownLatch = new CountDownLatch(1)

    val (replicaManager, _) = prepareReplicaManagerAndLogManager(new MockTimer(time),
      topicPartition, leaderEpoch + leaderEpochIncrement, followerBrokerId,
      leaderBrokerId, countDownLatch, expectTruncation = true)
    assertFalse(replicaManager.replicaSelectorOpt.isDefined)
  }

  @Test
  def testFetchFollowerNotAllowedForOlderClients(): Unit = {
    val replicaManager = setupReplicaManagerWithMockedPurgatories(new MockTimer(time), aliveBrokerIds = Seq(0, 1))

    val tp0 = new TopicPartition(topic, 0)
    val offsetCheckpoints = new LazyOffsetCheckpoints(replicaManager.highWatermarkCheckpoints)
    replicaManager.createPartition(tp0).createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints, None)
    val partition0Replicas = Seq[Integer](0, 1).asJava
    val becomeFollowerRequest = new LeaderAndIsrRequest.Builder(ApiKeys.LEADER_AND_ISR.latestVersion, 0, 0, brokerEpoch,
      Seq(new LeaderAndIsrPartitionState()
        .setTopicName(tp0.topic)
        .setPartitionIndex(tp0.partition)
        .setControllerEpoch(0)
        .setLeader(1)
        .setLeaderEpoch(0)
        .setIsr(partition0Replicas)
        .setZkVersion(0)
        .setReplicas(partition0Replicas)
        .setIsNew(true)).asJava,
      Collections.singletonMap(tp0.topic, Uuid.randomUuid()),
      Set(new Node(0, "host1", 0), new Node(1, "host2", 1)).asJava).build()
    replicaManager.becomeLeaderOrFollower(0, becomeFollowerRequest, (_, _) => ())

    // Fetch from follower, with non-empty ClientMetadata (FetchRequest v11+)
    val clientMetadata = new DefaultClientMetadata("", "", null, KafkaPrincipal.ANONYMOUS, "")
    var partitionData = new FetchRequest.PartitionData(0L, 0L, 100,
      Optional.of(0))
    var fetchResult = sendConsumerFetch(replicaManager, tp0, partitionData, Some(clientMetadata))
    assertNotNull(fetchResult.get)
    assertEquals(Errors.NONE, fetchResult.get.error)

    // Fetch from follower, with empty ClientMetadata (which implies an older version)
    partitionData = new FetchRequest.PartitionData(0L, 0L, 100,
      Optional.of(0))
    fetchResult = sendConsumerFetch(replicaManager, tp0, partitionData, None)
    assertNotNull(fetchResult.get)
    assertEquals(Errors.NOT_LEADER_OR_FOLLOWER, fetchResult.get.error)
  }

  @Test
  def testFetchRequestRateMetrics(): Unit = {
    val mockTimer = new MockTimer(time)
    val replicaManager = setupReplicaManagerWithMockedPurgatories(mockTimer, aliveBrokerIds = Seq(0, 1))

    val tp0 = new TopicPartition(topic, 0)
    val offsetCheckpoints = new LazyOffsetCheckpoints(replicaManager.highWatermarkCheckpoints)
    replicaManager.createPartition(tp0).createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints, None)
    val partition0Replicas = Seq[Integer](0, 1).asJava

    val becomeLeaderRequest = new LeaderAndIsrRequest.Builder(ApiKeys.LEADER_AND_ISR.latestVersion, 0, 0, brokerEpoch,
      Seq(new LeaderAndIsrPartitionState()
        .setTopicName(tp0.topic)
        .setPartitionIndex(tp0.partition)
        .setControllerEpoch(0)
        .setLeader(0)
        .setLeaderEpoch(1)
        .setIsr(partition0Replicas)
        .setZkVersion(0)
        .setReplicas(partition0Replicas)
        .setIsNew(true)).asJava,
      Collections.singletonMap(tp0.topic, Uuid.randomUuid()),
      Set(new Node(0, "host1", 0), new Node(1, "host2", 1)).asJava).build()
    replicaManager.becomeLeaderOrFollower(1, becomeLeaderRequest, (_, _) => ())

    def assertMetricCount(expected: Int): Unit = {
      assertEquals(expected, replicaManager.brokerTopicStats.allTopicsStats.totalFetchRequestRate.count)
      assertEquals(expected, replicaManager.brokerTopicStats.topicStats(topic).totalFetchRequestRate.count)
    }

    val partitionData = new FetchRequest.PartitionData(0L, 0L, 100,
      Optional.empty())

    val nonPurgatoryFetchResult = sendConsumerFetch(replicaManager, tp0, partitionData, None, timeout = 0)
    assertNotNull(nonPurgatoryFetchResult.get)
    assertEquals(Errors.NONE, nonPurgatoryFetchResult.get.error)
    assertMetricCount(1)

    val purgatoryFetchResult = sendConsumerFetch(replicaManager, tp0, partitionData, None, timeout = 10)
    assertNull(purgatoryFetchResult.get)
    mockTimer.advanceClock(11)
    assertNotNull(purgatoryFetchResult.get)
    assertEquals(Errors.NONE, purgatoryFetchResult.get.error)
    assertMetricCount(2)
  }

  @Test
  def testBecomeFollowerWhileOldClientFetchInPurgatory(): Unit = {
    val mockTimer = new MockTimer(time)
    val replicaManager = setupReplicaManagerWithMockedPurgatories(mockTimer, aliveBrokerIds = Seq(0, 1))

    val tp0 = new TopicPartition(topic, 0)
    val offsetCheckpoints = new LazyOffsetCheckpoints(replicaManager.highWatermarkCheckpoints)
    replicaManager.createPartition(tp0).createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints, None)
    val partition0Replicas = Seq[Integer](0, 1).asJava
    val topicIds = Collections.singletonMap(tp0.topic, Uuid.randomUuid())

    val becomeLeaderRequest = new LeaderAndIsrRequest.Builder(ApiKeys.LEADER_AND_ISR.latestVersion, 0, 0, brokerEpoch,
      Seq(new LeaderAndIsrPartitionState()
        .setTopicName(tp0.topic)
        .setPartitionIndex(tp0.partition)
        .setControllerEpoch(0)
        .setLeader(0)
        .setLeaderEpoch(1)
        .setIsr(partition0Replicas)
        .setZkVersion(0)
        .setReplicas(partition0Replicas)
        .setIsNew(true)).asJava,
      topicIds,
      Set(new Node(0, "host1", 0), new Node(1, "host2", 1)).asJava).build()
    replicaManager.becomeLeaderOrFollower(1, becomeLeaderRequest, (_, _) => ())

    val partitionData = new FetchRequest.PartitionData(0L, 0L, 100,
      Optional.empty())
    val fetchResult = sendConsumerFetch(replicaManager, tp0, partitionData, None, timeout = 10)
    assertNull(fetchResult.get)

    // Become a follower and ensure that the delayed fetch returns immediately
    val becomeFollowerRequest = new LeaderAndIsrRequest.Builder(ApiKeys.LEADER_AND_ISR.latestVersion, 0, 0, brokerEpoch,
      Seq(new LeaderAndIsrPartitionState()
        .setTopicName(tp0.topic)
        .setPartitionIndex(tp0.partition)
        .setControllerEpoch(0)
        .setLeader(1)
        .setLeaderEpoch(2)
        .setIsr(partition0Replicas)
        .setZkVersion(0)
        .setReplicas(partition0Replicas)
        .setIsNew(true)).asJava,
      topicIds,
      Set(new Node(0, "host1", 0), new Node(1, "host2", 1)).asJava).build()
    replicaManager.becomeLeaderOrFollower(0, becomeFollowerRequest, (_, _) => ())

    assertNotNull(fetchResult.get)
    assertEquals(Errors.NOT_LEADER_OR_FOLLOWER, fetchResult.get.error)
  }

  @Test
  def testBecomeFollowerWhileNewClientFetchInPurgatory(): Unit = {
    val mockTimer = new MockTimer(time)
    val replicaManager = setupReplicaManagerWithMockedPurgatories(mockTimer, aliveBrokerIds = Seq(0, 1))

    val tp0 = new TopicPartition(topic, 0)
    val offsetCheckpoints = new LazyOffsetCheckpoints(replicaManager.highWatermarkCheckpoints)
    replicaManager.createPartition(tp0).createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints, None)
    val partition0Replicas = Seq[Integer](0, 1).asJava
    val topicIds = Collections.singletonMap(tp0.topic, Uuid.randomUuid())

    val becomeLeaderRequest = new LeaderAndIsrRequest.Builder(ApiKeys.LEADER_AND_ISR.latestVersion, 0, 0, brokerEpoch,
      Seq(new LeaderAndIsrPartitionState()
        .setTopicName(tp0.topic)
        .setPartitionIndex(tp0.partition)
        .setControllerEpoch(0)
        .setLeader(0)
        .setLeaderEpoch(1)
        .setIsr(partition0Replicas)
        .setZkVersion(0)
        .setReplicas(partition0Replicas)
        .setIsNew(true)).asJava,
      topicIds,
      Set(new Node(0, "host1", 0), new Node(1, "host2", 1)).asJava).build()
    replicaManager.becomeLeaderOrFollower(1, becomeLeaderRequest, (_, _) => ())

    val clientMetadata = new DefaultClientMetadata("", "", null, KafkaPrincipal.ANONYMOUS, "")
    val partitionData = new FetchRequest.PartitionData(0L, 0L, 100,
      Optional.of(1))
    val fetchResult = sendConsumerFetch(replicaManager, tp0, partitionData, Some(clientMetadata), timeout = 10)
    assertNull(fetchResult.get)

    // Become a follower and ensure that the delayed fetch returns immediately
    val becomeFollowerRequest = new LeaderAndIsrRequest.Builder(ApiKeys.LEADER_AND_ISR.latestVersion, 0, 0, brokerEpoch,
      Seq(new LeaderAndIsrPartitionState()
        .setTopicName(tp0.topic)
        .setPartitionIndex(tp0.partition)
        .setControllerEpoch(0)
        .setLeader(1)
        .setLeaderEpoch(2)
        .setIsr(partition0Replicas)
        .setZkVersion(0)
        .setReplicas(partition0Replicas)
        .setIsNew(true)).asJava,
      topicIds,
      Set(new Node(0, "host1", 0), new Node(1, "host2", 1)).asJava).build()
    replicaManager.becomeLeaderOrFollower(0, becomeFollowerRequest, (_, _) => ())

    assertNotNull(fetchResult.get)
    assertEquals(Errors.FENCED_LEADER_EPOCH, fetchResult.get.error)
  }

  @Test
  def testFetchFromLeaderAlwaysAllowed(): Unit = {
    val replicaManager = setupReplicaManagerWithMockedPurgatories(new MockTimer(time), aliveBrokerIds = Seq(0, 1))

    val tp0 = new TopicPartition(topic, 0)
    val offsetCheckpoints = new LazyOffsetCheckpoints(replicaManager.highWatermarkCheckpoints)
    replicaManager.createPartition(tp0).createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints, None)
    val partition0Replicas = Seq[Integer](0, 1).asJava

    val becomeLeaderRequest = new LeaderAndIsrRequest.Builder(ApiKeys.LEADER_AND_ISR.latestVersion, 0, 0, brokerEpoch,
      Seq(new LeaderAndIsrPartitionState()
        .setTopicName(tp0.topic)
        .setPartitionIndex(tp0.partition)
        .setControllerEpoch(0)
        .setLeader(0)
        .setLeaderEpoch(1)
        .setIsr(partition0Replicas)
        .setZkVersion(0)
        .setReplicas(partition0Replicas)
        .setIsNew(true)).asJava,
      Collections.singletonMap(tp0.topic, Uuid.randomUuid()),
      Set(new Node(0, "host1", 0), new Node(1, "host2", 1)).asJava).build()
    replicaManager.becomeLeaderOrFollower(1, becomeLeaderRequest, (_, _) => ())

    val clientMetadata = new DefaultClientMetadata("", "", null, KafkaPrincipal.ANONYMOUS, "")
    var partitionData = new FetchRequest.PartitionData(0L, 0L, 100,
      Optional.of(1))
    var fetchResult = sendConsumerFetch(replicaManager, tp0, partitionData, Some(clientMetadata))
    assertNotNull(fetchResult.get)
    assertEquals(Errors.NONE, fetchResult.get.error)

    partitionData = new FetchRequest.PartitionData(0L, 0L, 100,
      Optional.empty())
    fetchResult = sendConsumerFetch(replicaManager, tp0, partitionData, Some(clientMetadata))
    assertNotNull(fetchResult.get)
    assertEquals(Errors.NONE, fetchResult.get.error)
  }

  @Test
  def testClearFetchPurgatoryOnStopReplica(): Unit = {
    // As part of a reassignment, we may send StopReplica to the old leader.
    // In this case, we should ensure that pending purgatory operations are cancelled
    // immediately rather than sitting around to timeout.

    val mockTimer = new MockTimer(time)
    val replicaManager = setupReplicaManagerWithMockedPurgatories(mockTimer, aliveBrokerIds = Seq(0, 1))

    val tp0 = new TopicPartition(topic, 0)
    val offsetCheckpoints = new LazyOffsetCheckpoints(replicaManager.highWatermarkCheckpoints)
    replicaManager.createPartition(tp0).createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints, None)
    val partition0Replicas = Seq[Integer](0, 1).asJava

    val becomeLeaderRequest = new LeaderAndIsrRequest.Builder(ApiKeys.LEADER_AND_ISR.latestVersion, 0, 0, brokerEpoch,
      Seq(new LeaderAndIsrPartitionState()
        .setTopicName(tp0.topic)
        .setPartitionIndex(tp0.partition)
        .setControllerEpoch(0)
        .setLeader(0)
        .setLeaderEpoch(1)
        .setIsr(partition0Replicas)
        .setZkVersion(0)
        .setReplicas(partition0Replicas)
        .setIsNew(true)).asJava,
      Collections.singletonMap(tp0.topic, Uuid.randomUuid()),
      Set(new Node(0, "host1", 0), new Node(1, "host2", 1)).asJava).build()
    replicaManager.becomeLeaderOrFollower(1, becomeLeaderRequest, (_, _) => ())

    val partitionData = new FetchRequest.PartitionData(0L, 0L, 100,
      Optional.of(1))
    val fetchResult = sendConsumerFetch(replicaManager, tp0, partitionData, None, timeout = 10)
    assertNull(fetchResult.get)

    Mockito.when(replicaManager.metadataCache.contains(tp0)).thenReturn(true)

    // We have a fetch in purgatory, now receive a stop replica request and
    // assert that the fetch returns with a NOT_LEADER error
    replicaManager.stopReplicas(2, 0, 0, brokerEpoch,
      mutable.Map(tp0 -> new StopReplicaPartitionState()
        .setPartitionIndex(tp0.partition)
        .setDeletePartition(true)
        .setLeaderEpoch(LeaderAndIsr.EpochDuringDelete)))

    assertNotNull(fetchResult.get)
    assertEquals(Errors.NOT_LEADER_OR_FOLLOWER, fetchResult.get.error)
  }

  @Test
  def testClearProducePurgatoryOnStopReplica(): Unit = {
    val mockTimer = new MockTimer(time)
    val replicaManager = setupReplicaManagerWithMockedPurgatories(mockTimer, aliveBrokerIds = Seq(0, 1))

    val tp0 = new TopicPartition(topic, 0)
    val offsetCheckpoints = new LazyOffsetCheckpoints(replicaManager.highWatermarkCheckpoints)
    replicaManager.createPartition(tp0).createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints, None)
    val partition0Replicas = Seq[Integer](0, 1).asJava

    val becomeLeaderRequest = new LeaderAndIsrRequest.Builder(ApiKeys.LEADER_AND_ISR.latestVersion, 0, 0, brokerEpoch,
      Seq(new LeaderAndIsrPartitionState()
        .setTopicName(tp0.topic)
        .setPartitionIndex(tp0.partition)
        .setControllerEpoch(0)
        .setLeader(0)
        .setLeaderEpoch(1)
        .setIsr(partition0Replicas)
        .setZkVersion(0)
        .setReplicas(partition0Replicas)
        .setIsNew(true)).asJava,
      Collections.singletonMap(tp0.topic, Uuid.randomUuid()),
      Set(new Node(0, "host1", 0), new Node(1, "host2", 1)).asJava).build()
    replicaManager.becomeLeaderOrFollower(1, becomeLeaderRequest, (_, _) => ())

    val produceResult = sendProducerAppend(replicaManager, tp0)
    assertNull(produceResult.get)

    Mockito.when(replicaManager.metadataCache.contains(tp0)).thenReturn(true)

    replicaManager.stopReplicas(2, 0, 0, brokerEpoch,
      mutable.Map(tp0 -> new StopReplicaPartitionState()
        .setPartitionIndex(tp0.partition)
        .setDeletePartition(true)
        .setLeaderEpoch(LeaderAndIsr.EpochDuringDelete)))

    assertNotNull(produceResult.get)
    assertEquals(Errors.NOT_LEADER_OR_FOLLOWER, produceResult.get.error)
  }

  private def sendProducerAppend(replicaManager: ReplicaManager,
                                 topicPartition: TopicPartition): AtomicReference[PartitionResponse] = {
    val produceResult = new AtomicReference[PartitionResponse]()
    def callback(response: Map[TopicPartition, PartitionResponse]): Unit = {
      produceResult.set(response(topicPartition))
    }

    val records = MemoryRecords.withRecords(CompressionType.NONE,
      new SimpleRecord("a".getBytes()),
      new SimpleRecord("b".getBytes()),
      new SimpleRecord("c".getBytes())
    )

    replicaManager.appendRecords(
      timeout = 10,
      requiredAcks = -1,
      internalTopicsAllowed = false,
      origin = AppendOrigin.Client,
      entriesPerPartition = Map(topicPartition -> records),
      responseCallback = callback
    )
    produceResult
  }

  private def sendConsumerFetch(replicaManager: ReplicaManager,
                                topicPartition: TopicPartition,
                                partitionData: FetchRequest.PartitionData,
                                clientMetadataOpt: Option[ClientMetadata],
                                timeout: Long = 0L): AtomicReference[FetchPartitionData] = {
    val fetchResult = new AtomicReference[FetchPartitionData]()
    def callback(response: Seq[(TopicPartition, FetchPartitionData)]): Unit = {
      fetchResult.set(response.toMap.apply(topicPartition))
    }
    replicaManager.fetchMessages(
      timeout = timeout,
      replicaId = Request.OrdinaryConsumerId,
      fetchMinBytes = 1,
      fetchMaxBytes = 100,
      hardMaxBytesLimit = false,
      fetchInfos = Seq(topicPartition -> partitionData),
      quota = UnboundedQuota,
      isolationLevel = IsolationLevel.READ_UNCOMMITTED,
      responseCallback = callback,
      clientMetadata = clientMetadataOpt
    )
    fetchResult
  }

  /**
   * This method assumes that the test using created ReplicaManager calls
   * ReplicaManager.becomeLeaderOrFollower() once with LeaderAndIsrRequest containing
   * 'leaderEpochInLeaderAndIsr' leader epoch for partition 'topicPartition'.
   */
  private def prepareReplicaManagerAndLogManager(timer: MockTimer,
                                                 topicPartition: Int,
                                                 leaderEpochInLeaderAndIsr: Int,
                                                 followerBrokerId: Int,
                                                 leaderBrokerId: Int,
                                                 countDownLatch: CountDownLatch,
                                                 expectTruncation: Boolean,
                                                 localLogOffset: Option[Long] = None,
                                                 offsetFromLeader: Long = 5,
                                                 leaderEpochFromLeader: Int = 3,
                                                 extraProps: Properties = new Properties(),
                                                 topicId: Option[Uuid] = None) : (ReplicaManager, LogManager) = {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect)
    props.put("log.dir", TestUtils.tempRelativeDir("data").getAbsolutePath)
    props.asScala ++= extraProps.asScala
    val config = KafkaConfig.fromProps(props)

    val mockScheduler = new MockScheduler(time)
    val mockBrokerTopicStats = new BrokerTopicStats
    val mockLogDirFailureChannel = new LogDirFailureChannel(config.logDirs.size)
    val mockLog = new Log(
      _dir = new File(new File(config.logDirs.head), s"$topic-0"),
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
      logDirFailureChannel = mockLogDirFailureChannel,
      topicId = topicId,
      keepPartitionMetadataFile = true) {

      override def endOffsetForEpoch(leaderEpoch: Int): Option[OffsetAndEpoch] = {
        assertEquals(leaderEpoch, leaderEpochFromLeader)
        localLogOffset.map { logOffset =>
          Some(OffsetAndEpoch(logOffset, leaderEpochFromLeader))
        }.getOrElse(super.endOffsetForEpoch(leaderEpoch))
      }

      override def latestEpoch: Option[Int] = Some(leaderEpochFromLeader)

      override def logEndOffsetMetadata: LogOffsetMetadata =
        localLogOffset.map(LogOffsetMetadata(_)).getOrElse(super.logEndOffsetMetadata)

      override def logEndOffset: Long = localLogOffset.getOrElse(super.logEndOffset)
    }

    // Expect to call LogManager.truncateTo exactly once
    val topicPartitionObj = new TopicPartition(topic, topicPartition)
    val mockLogMgr: LogManager = EasyMock.createMock(classOf[LogManager])
    EasyMock.expect(mockLogMgr.liveLogDirs).andReturn(config.logDirs.map(new File(_).getAbsoluteFile)).anyTimes
    EasyMock.expect(mockLogMgr.getOrCreateLog(EasyMock.eq(topicPartitionObj),
      isNew = EasyMock.eq(false), isFuture = EasyMock.eq(false), EasyMock.anyObject())).andReturn(mockLog).anyTimes
    if (expectTruncation) {
      EasyMock.expect(mockLogMgr.truncateTo(Map(topicPartitionObj -> offsetFromLeader),
        isFuture = false)).once
    }
    EasyMock.expect(mockLogMgr.initializingLog(topicPartitionObj)).anyTimes
    EasyMock.expect(mockLogMgr.getLog(topicPartitionObj, isFuture = true)).andReturn(None)

    EasyMock.expect(mockLogMgr.finishedInitializingLog(
      EasyMock.eq(topicPartitionObj), EasyMock.anyObject())).anyTimes

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

    val mockProducePurgatory = new DelayedOperationPurgatory[DelayedProduce](
      purgatoryName = "Produce", timer, reaperEnabled = false)
    val mockFetchPurgatory = new DelayedOperationPurgatory[DelayedFetch](
      purgatoryName = "Fetch", timer, reaperEnabled = false)
    val mockDeleteRecordsPurgatory = new DelayedOperationPurgatory[DelayedDeleteRecords](
      purgatoryName = "DeleteRecords", timer, reaperEnabled = false)
    val mockElectLeaderPurgatory = new DelayedOperationPurgatory[DelayedElectLeader](
      purgatoryName = "ElectLeader", timer, reaperEnabled = false)

    // Mock network client to show leader offset of 5
    val blockingSend = new ReplicaFetcherMockBlockingSend(
      Map(topicPartitionObj -> new EpochEndOffset()
        .setPartition(topicPartitionObj.partition)
        .setErrorCode(Errors.NONE.code)
        .setLeaderEpoch(leaderEpochFromLeader)
        .setEndOffset(offsetFromLeader)).asJava,
      BrokerEndPoint(1, "host1" ,1), time)
    val replicaManager = new ReplicaManager(config, metrics, time, None, mockScheduler, mockLogMgr,
      new AtomicBoolean(false), quotaManager, mockBrokerTopicStats,
      metadataCache, mockLogDirFailureChannel, mockProducePurgatory, mockFetchPurgatory,
      mockDeleteRecordsPurgatory, mockElectLeaderPurgatory, Option(this.getClass.getName),
      configRepository, alterIsrManager) {

      override protected def createReplicaFetcherManager(metrics: Metrics,
                                                     time: Time,
                                                     threadNamePrefix: Option[String],
                                                     replicationQuotaManager: ReplicationQuotaManager): ReplicaFetcherManager = {
        new ReplicaFetcherManager(config, this, metrics, time, threadNamePrefix, replicationQuotaManager) {

          override def createFetcherThread(fetcherId: Int, sourceBroker: BrokerEndPoint): ReplicaFetcherThread = {
            new ReplicaFetcherThread(s"ReplicaFetcherThread-$fetcherId", fetcherId,
              sourceBroker, config, failedPartitions, replicaManager, metrics, time, quotaManager.follower, Some(blockingSend)) {

              override def doWork() = {
                // In case the thread starts before the partition is added by AbstractFetcherManager,
                // add it here (it's a no-op if already added)
                val initialOffset = InitialFetchState(
                  leader = new BrokerEndPoint(0, "localhost", 9092),
                  initOffset = 0L, currentLeaderEpoch = leaderEpochInLeaderAndIsr)
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

  private def leaderAndIsrPartitionState(topicPartition: TopicPartition,
                                         leaderEpoch: Int,
                                         leaderBrokerId: Int,
                                         aliveBrokerIds: Seq[Integer],
                                         isNew: Boolean = false): LeaderAndIsrPartitionState = {
    new LeaderAndIsrPartitionState()
      .setTopicName(topic)
      .setPartitionIndex(topicPartition.partition)
      .setControllerEpoch(controllerEpoch)
      .setLeader(leaderBrokerId)
      .setLeaderEpoch(leaderEpoch)
      .setIsr(aliveBrokerIds.asJava)
      .setZkVersion(zkVersion)
      .setReplicas(aliveBrokerIds.asJava)
      .setIsNew(isNew)
  }

  private class CallbackResult[T] {
    private var value: Option[T] = None
    private var fun: Option[T => Unit] = None

    def assertFired: T = {
      assertTrue(isFired, "Callback has not been fired")
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
                            origin: AppendOrigin = AppendOrigin.Client,
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
      origin = origin,
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

  private def setupReplicaManagerWithMockedPurgatories(
    timer: MockTimer,
    brokerId: Int = 0,
    aliveBrokerIds: Seq[Int] = Seq(0, 1),
    propsModifier: Properties => Unit = _ => {}
  ): ReplicaManager = {
    val props = TestUtils.createBrokerConfig(brokerId, TestUtils.MockZkConnect)
    props.put("log.dirs", TestUtils.tempRelativeDir("data").getAbsolutePath + "," + TestUtils.tempRelativeDir("data2").getAbsolutePath)
    propsModifier.apply(props)
    val config = KafkaConfig.fromProps(props)
    val logProps = new Properties()
    val mockLogMgr = TestUtils.createLogManager(config.logDirs.map(new File(_)), LogConfig(logProps))
    val aliveBrokers = aliveBrokerIds.map(brokerId => createBroker(brokerId, s"host$brokerId", brokerId))

    val metadataCache: MetadataCache = Mockito.mock(classOf[MetadataCache])
    Mockito.when(metadataCache.getAliveBrokers).thenReturn(aliveBrokers)

    aliveBrokerIds.foreach { brokerId =>
      Mockito.when(metadataCache.getAliveBroker(brokerId))
        .thenReturn(Option(createBroker(brokerId, s"host$brokerId", brokerId)))
    }

    val mockProducePurgatory = new DelayedOperationPurgatory[DelayedProduce](
      purgatoryName = "Produce", timer, reaperEnabled = false)
    val mockFetchPurgatory = new DelayedOperationPurgatory[DelayedFetch](
      purgatoryName = "Fetch", timer, reaperEnabled = false)
    val mockDeleteRecordsPurgatory = new DelayedOperationPurgatory[DelayedDeleteRecords](
      purgatoryName = "DeleteRecords", timer, reaperEnabled = false)
    val mockDelayedElectLeaderPurgatory = new DelayedOperationPurgatory[DelayedElectLeader](
      purgatoryName = "DelayedElectLeader", timer, reaperEnabled = false)

    new ReplicaManager(config, metrics, time, None, scheduler, mockLogMgr,
      new AtomicBoolean(false), quotaManager, new BrokerTopicStats,
      metadataCache, new LogDirFailureChannel(config.logDirs.size), mockProducePurgatory, mockFetchPurgatory,
      mockDeleteRecordsPurgatory, mockDelayedElectLeaderPurgatory, Option(this.getClass.getName),
      configRepository, alterIsrManager)
  }

  @Test
  def testOldLeaderLosesMetricsWhenReassignPartitions(): Unit = {
    val controllerEpoch = 0
    val leaderEpoch = 0
    val leaderEpochIncrement = 1
    val correlationId = 0
    val controllerId = 0
    val mockTopicStats1: BrokerTopicStats = EasyMock.mock(classOf[BrokerTopicStats])
    val (rm0, rm1) = prepareDifferentReplicaManagers(EasyMock.mock(classOf[BrokerTopicStats]), mockTopicStats1)

    EasyMock.expect(mockTopicStats1.removeOldLeaderMetrics(topic)).andVoid.once
    EasyMock.replay(mockTopicStats1)

    try {
      // make broker 0 the leader of partition 0 and
      // make broker 1 the leader of partition 1
      val tp0 = new TopicPartition(topic, 0)
      val tp1 = new TopicPartition(topic, 1)
      val partition0Replicas = Seq[Integer](0, 1).asJava
      val partition1Replicas = Seq[Integer](1, 0).asJava
      val topicIds = Map(tp0.topic -> Uuid.randomUuid(), tp1.topic -> Uuid.randomUuid()).asJava

      val leaderAndIsrRequest1 = new LeaderAndIsrRequest.Builder(ApiKeys.LEADER_AND_ISR.latestVersion,
        controllerId, 0, brokerEpoch,
        Seq(
          new LeaderAndIsrPartitionState()
            .setTopicName(tp0.topic)
            .setPartitionIndex(tp0.partition)
            .setControllerEpoch(controllerEpoch)
            .setLeader(0)
            .setLeaderEpoch(leaderEpoch)
            .setIsr(partition0Replicas)
            .setZkVersion(0)
            .setReplicas(partition0Replicas)
            .setIsNew(true),
          new LeaderAndIsrPartitionState()
            .setTopicName(tp1.topic)
            .setPartitionIndex(tp1.partition)
            .setControllerEpoch(controllerEpoch)
            .setLeader(1)
            .setLeaderEpoch(leaderEpoch)
            .setIsr(partition1Replicas)
            .setZkVersion(0)
            .setReplicas(partition1Replicas)
            .setIsNew(true)
        ).asJava,
        topicIds,
        Set(new Node(0, "host0", 0), new Node(1, "host1", 1)).asJava).build()

      rm0.becomeLeaderOrFollower(correlationId, leaderAndIsrRequest1, (_, _) => ())
      rm1.becomeLeaderOrFollower(correlationId, leaderAndIsrRequest1, (_, _) => ())

      // make broker 0 the leader of partition 1 so broker 1 loses its leadership position
      val leaderAndIsrRequest2 = new LeaderAndIsrRequest.Builder(ApiKeys.LEADER_AND_ISR.latestVersion, controllerId,
        controllerEpoch, brokerEpoch,
        Seq(
          new LeaderAndIsrPartitionState()
            .setTopicName(tp0.topic)
            .setPartitionIndex(tp0.partition)
            .setControllerEpoch(controllerEpoch)
            .setLeader(0)
            .setLeaderEpoch(leaderEpoch + leaderEpochIncrement)
            .setIsr(partition0Replicas)
            .setZkVersion(0)
            .setReplicas(partition0Replicas)
            .setIsNew(true),
          new LeaderAndIsrPartitionState()
            .setTopicName(tp1.topic)
            .setPartitionIndex(tp1.partition)
            .setControllerEpoch(controllerEpoch)
            .setLeader(0)
            .setLeaderEpoch(leaderEpoch + leaderEpochIncrement)
            .setIsr(partition1Replicas)
            .setZkVersion(0)
            .setReplicas(partition1Replicas)
            .setIsNew(true)
        ).asJava,
        topicIds,
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
    val mockTopicStats1: BrokerTopicStats = EasyMock.mock(classOf[BrokerTopicStats])
    val (rm0, rm1) = prepareDifferentReplicaManagers(EasyMock.mock(classOf[BrokerTopicStats]), mockTopicStats1)

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
      val topicIds = Map(tp0.topic -> Uuid.randomUuid(), tp1.topic -> Uuid.randomUuid()).asJava

      val leaderAndIsrRequest1 = new LeaderAndIsrRequest.Builder(ApiKeys.LEADER_AND_ISR.latestVersion,
        controllerId, 0, brokerEpoch,
        Seq(
          new LeaderAndIsrPartitionState()
            .setTopicName(tp0.topic)
            .setPartitionIndex(tp0.partition)
            .setControllerEpoch(controllerEpoch)
            .setLeader(1)
            .setLeaderEpoch(leaderEpoch)
            .setIsr(partition0Replicas)
            .setZkVersion(0)
            .setReplicas(partition0Replicas)
            .setIsNew(true),
          new LeaderAndIsrPartitionState()
            .setTopicName(tp1.topic)
            .setPartitionIndex(tp1.partition)
            .setControllerEpoch(controllerEpoch)
            .setLeader(1)
            .setLeaderEpoch(leaderEpoch)
            .setIsr(partition1Replicas)
            .setZkVersion(0)
            .setReplicas(partition1Replicas)
            .setIsNew(true)
        ).asJava,
        topicIds,
        Set(new Node(0, "host0", 0), new Node(1, "host1", 1)).asJava).build()

      rm0.becomeLeaderOrFollower(correlationId, leaderAndIsrRequest1, (_, _) => ())
      rm1.becomeLeaderOrFollower(correlationId, leaderAndIsrRequest1, (_, _) => ())

      // make broker 0 the leader of partition 1 so broker 1 loses its leadership position
      val leaderAndIsrRequest2 = new LeaderAndIsrRequest.Builder(ApiKeys.LEADER_AND_ISR.latestVersion, controllerId,
        controllerEpoch, brokerEpoch,
        Seq(
          new LeaderAndIsrPartitionState()
            .setTopicName(tp0.topic)
            .setPartitionIndex(tp0.partition)
            .setControllerEpoch(controllerEpoch)
            .setLeader(0)
            .setLeaderEpoch(leaderEpoch + leaderEpochIncrement)
            .setIsr(partition0Replicas)
            .setZkVersion(0)
            .setReplicas(partition0Replicas)
            .setIsNew(true),
          new LeaderAndIsrPartitionState()
            .setTopicName(tp1.topic)
            .setPartitionIndex(tp1.partition)
            .setControllerEpoch(controllerEpoch)
            .setLeader(0)
            .setLeaderEpoch(leaderEpoch + leaderEpochIncrement)
            .setIsr(partition1Replicas)
            .setZkVersion(0)
            .setReplicas(partition1Replicas)
            .setIsNew(true)
        ).asJava,
        topicIds,
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

  private def prepareDifferentReplicaManagers(brokerTopicStats1: BrokerTopicStats,
                                              brokerTopicStats2: BrokerTopicStats): (ReplicaManager, ReplicaManager) = {
    val props0 = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect)
    val props1 = TestUtils.createBrokerConfig(1, TestUtils.MockZkConnect)

    props0.put("log0.dir", TestUtils.tempRelativeDir("data").getAbsolutePath)
    props1.put("log1.dir", TestUtils.tempRelativeDir("data").getAbsolutePath)

    val config0 = KafkaConfig.fromProps(props0)
    val config1 = KafkaConfig.fromProps(props1)

    val mockLogMgr0 = TestUtils.createLogManager(config0.logDirs.map(new File(_)))
    val mockLogMgr1 = TestUtils.createLogManager(config1.logDirs.map(new File(_)))

    val metadataCache0: MetadataCache = EasyMock.createMock(classOf[MetadataCache])
    val metadataCache1: MetadataCache = EasyMock.createMock(classOf[MetadataCache])

    val aliveBrokers = Seq(createBroker(0, "host0", 0), createBroker(1, "host1", 1))

    EasyMock.expect(metadataCache0.getAliveBrokers).andReturn(aliveBrokers).anyTimes()
    EasyMock.replay(metadataCache0)
    EasyMock.expect(metadataCache1.getAliveBrokers).andReturn(aliveBrokers).anyTimes()
    EasyMock.replay(metadataCache1)

    // each replica manager is for a broker
    val rm0 = new ReplicaManager(config0, metrics, time, None, new MockScheduler(time), mockLogMgr0,
      new AtomicBoolean(false), quotaManager,
      brokerTopicStats1, metadataCache0, new LogDirFailureChannel(config0.logDirs.size), alterIsrManager, configRepository)
    val rm1 = new ReplicaManager(config1, metrics, time, None, new MockScheduler(time), mockLogMgr1,
      new AtomicBoolean(false), quotaManager,
      brokerTopicStats2, metadataCache1, new LogDirFailureChannel(config1.logDirs.size), alterIsrManager, configRepository)

    (rm0, rm1)
  }

  @Test
  def testStopReplicaWithStaleControllerEpoch(): Unit = {
    val mockTimer = new MockTimer(time)
    val replicaManager = setupReplicaManagerWithMockedPurgatories(mockTimer, aliveBrokerIds = Seq(0, 1))

    val tp0 = new TopicPartition(topic, 0)
    val offsetCheckpoints = new LazyOffsetCheckpoints(replicaManager.highWatermarkCheckpoints)
    replicaManager.createPartition(tp0).createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints, None)

    val becomeLeaderRequest = new LeaderAndIsrRequest.Builder(ApiKeys.LEADER_AND_ISR.latestVersion, 0, 10, brokerEpoch,
      Seq(leaderAndIsrPartitionState(tp0, 1, 0, Seq(0, 1), true)).asJava,
      Collections.singletonMap(topic, Uuid.randomUuid()),
      Set(new Node(0, "host1", 0), new Node(1, "host2", 1)).asJava
    ).build()

    replicaManager.becomeLeaderOrFollower(1, becomeLeaderRequest, (_, _) => ())

    val partitionStates = Map(tp0 -> new StopReplicaPartitionState()
      .setPartitionIndex(tp0.partition)
      .setLeaderEpoch(1)
      .setDeletePartition(false)
    )

    val (_, error) = replicaManager.stopReplicas(1, 0, 0, brokerEpoch, partitionStates)
    assertEquals(Errors.STALE_CONTROLLER_EPOCH, error)
  }

  @Test
  def testStopReplicaWithOfflinePartition(): Unit = {
    val mockTimer = new MockTimer(time)
    val replicaManager = setupReplicaManagerWithMockedPurgatories(mockTimer, aliveBrokerIds = Seq(0, 1))

    val tp0 = new TopicPartition(topic, 0)
    val offsetCheckpoints = new LazyOffsetCheckpoints(replicaManager.highWatermarkCheckpoints)
    replicaManager.createPartition(tp0).createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints, None)

    val becomeLeaderRequest = new LeaderAndIsrRequest.Builder(ApiKeys.LEADER_AND_ISR.latestVersion, 0, 0, brokerEpoch,
      Seq(leaderAndIsrPartitionState(tp0, 1, 0, Seq(0, 1), true)).asJava,
      Collections.singletonMap(topic, Uuid.randomUuid()),
      Set(new Node(0, "host1", 0), new Node(1, "host2", 1)).asJava
    ).build()

    replicaManager.becomeLeaderOrFollower(1, becomeLeaderRequest, (_, _) => ())
    replicaManager.markPartitionOffline(tp0)

    val partitionStates = Map(tp0 -> new StopReplicaPartitionState()
      .setPartitionIndex(tp0.partition)
      .setLeaderEpoch(1)
      .setDeletePartition(false)
    )

    val (result, error) = replicaManager.stopReplicas(1, 0, 0, brokerEpoch, partitionStates)
    assertEquals(Errors.NONE, error)
    assertEquals(Map(tp0 -> Errors.KAFKA_STORAGE_ERROR), result)
  }

  @Test
  def testStopReplicaWithInexistentPartition(): Unit = {
    testStopReplicaWithInexistentPartition(false, false)
  }

  @Test
  def testStopReplicaWithInexistentPartitionAndPartitionsDelete(): Unit = {
    testStopReplicaWithInexistentPartition(true, false)
  }

  @Test
  def testStopReplicaWithInexistentPartitionAndPartitionsDeleteAndIOException(): Unit = {
    testStopReplicaWithInexistentPartition(true, true)
  }

  private def testStopReplicaWithInexistentPartition(deletePartitions: Boolean, throwIOException: Boolean): Unit = {
    val mockTimer = new MockTimer(time)
    val replicaManager = setupReplicaManagerWithMockedPurgatories(mockTimer, aliveBrokerIds = Seq(0, 1))

    val tp0 = new TopicPartition(topic, 0)
    val log = replicaManager.logManager.getOrCreateLog(tp0, true, topicId = None)

    if (throwIOException) {
      // Delete the underlying directory to trigger an KafkaStorageException
      val dir = log.dir.getParentFile
      Utils.delete(dir)
      dir.createNewFile()
    }

    val partitionStates = Map(tp0 -> new StopReplicaPartitionState()
      .setPartitionIndex(tp0.partition)
      .setLeaderEpoch(1)
      .setDeletePartition(deletePartitions)
    )

    val (result, error) = replicaManager.stopReplicas(1, 0, 0, brokerEpoch, partitionStates)
    assertEquals(Errors.NONE, error)

    if (throwIOException && deletePartitions) {
      assertEquals(Map(tp0 -> Errors.KAFKA_STORAGE_ERROR), result)
      assertTrue(replicaManager.logManager.getLog(tp0).isEmpty)
    } else if (deletePartitions) {
      assertEquals(Map(tp0 -> Errors.NONE), result)
      assertTrue(replicaManager.logManager.getLog(tp0).isEmpty)
    } else {
      assertEquals(Map(tp0 -> Errors.NONE), result)
      assertTrue(replicaManager.logManager.getLog(tp0).isDefined)
    }
  }

  @Test
  def testStopReplicaWithExistingPartitionAndNewerLeaderEpoch(): Unit = {
    testStopReplicaWithExistingPartition(2, false, false, Errors.NONE)
  }

  @Test
  def testStopReplicaWithExistingPartitionAndOlderLeaderEpoch(): Unit = {
    testStopReplicaWithExistingPartition(0, false, false, Errors.FENCED_LEADER_EPOCH)
  }

  @Test
  def testStopReplicaWithExistingPartitionAndEqualLeaderEpoch(): Unit = {
    testStopReplicaWithExistingPartition(1, false, false, Errors.FENCED_LEADER_EPOCH)
  }

  @Test
  def testStopReplicaWithExistingPartitionAndDeleteSentinel(): Unit = {
    testStopReplicaWithExistingPartition(LeaderAndIsr.EpochDuringDelete, false, false, Errors.NONE)
  }

  @Test
  def testStopReplicaWithExistingPartitionAndLeaderEpochNotProvided(): Unit = {
    testStopReplicaWithExistingPartition(LeaderAndIsr.NoEpoch, false, false, Errors.NONE)
  }

  @Test
  def testStopReplicaWithDeletePartitionAndExistingPartitionAndNewerLeaderEpoch(): Unit = {
    testStopReplicaWithExistingPartition(2, true, false, Errors.NONE)
  }

  @Test
  def testStopReplicaWithDeletePartitionAndExistingPartitionAndNewerLeaderEpochAndIOException(): Unit = {
    testStopReplicaWithExistingPartition(2, true, true, Errors.KAFKA_STORAGE_ERROR)
  }

  @Test
  def testStopReplicaWithDeletePartitionAndExistingPartitionAndOlderLeaderEpoch(): Unit = {
    testStopReplicaWithExistingPartition(0, true, false, Errors.FENCED_LEADER_EPOCH)
  }

  @Test
  def testStopReplicaWithDeletePartitionAndExistingPartitionAndEqualLeaderEpoch(): Unit = {
    testStopReplicaWithExistingPartition(1, true, false, Errors.FENCED_LEADER_EPOCH)
  }

  @Test
  def testStopReplicaWithDeletePartitionAndExistingPartitionAndDeleteSentinel(): Unit = {
    testStopReplicaWithExistingPartition(LeaderAndIsr.EpochDuringDelete, true, false, Errors.NONE)
  }

  @Test
  def testStopReplicaWithDeletePartitionAndExistingPartitionAndLeaderEpochNotProvided(): Unit = {
    testStopReplicaWithExistingPartition(LeaderAndIsr.NoEpoch, true, false, Errors.NONE)
  }

  private def testStopReplicaWithExistingPartition(leaderEpoch: Int,
                                                   deletePartition: Boolean,
                                                   throwIOException: Boolean,
                                                   expectedOutput: Errors): Unit = {
    val mockTimer = new MockTimer(time)
    val replicaManager = setupReplicaManagerWithMockedPurgatories(mockTimer, aliveBrokerIds = Seq(0, 1))

    val tp0 = new TopicPartition(topic, 0)
    val offsetCheckpoints = new LazyOffsetCheckpoints(replicaManager.highWatermarkCheckpoints)
    val partition = replicaManager.createPartition(tp0)
    partition.createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints, None)

    val logDirFailureChannel = new LogDirFailureChannel(replicaManager.config.logDirs.size)
    val logDir = partition.log.get.parentDirFile

    def readRecoveryPointCheckpoint(): Map[TopicPartition, Long] = {
      new OffsetCheckpointFile(new File(logDir, LogManager.RecoveryPointCheckpointFile),
        logDirFailureChannel).read()
    }

    def readLogStartOffsetCheckpoint(): Map[TopicPartition, Long] = {
      new OffsetCheckpointFile(new File(logDir, LogManager.LogStartOffsetCheckpointFile),
        logDirFailureChannel).read()
    }

    val becomeLeaderRequest = new LeaderAndIsrRequest.Builder(ApiKeys.LEADER_AND_ISR.latestVersion, 0, 0, brokerEpoch,
      Seq(leaderAndIsrPartitionState(tp0, 1, 0, Seq(0, 1), true)).asJava,
      Collections.singletonMap(tp0.topic(), Uuid.randomUuid()),
      Set(new Node(0, "host1", 0), new Node(1, "host2", 1)).asJava
    ).build()

    replicaManager.becomeLeaderOrFollower(1, becomeLeaderRequest, (_, _) => ())

    val batch = TestUtils.records(records = List(
      new SimpleRecord(10, "k1".getBytes, "v1".getBytes),
      new SimpleRecord(11, "k2".getBytes, "v2".getBytes)))
    partition.appendRecordsToLeader(batch, AppendOrigin.Client, requiredAcks = 0)
    partition.log.get.updateHighWatermark(2L)
    partition.log.get.maybeIncrementLogStartOffset(1L, LeaderOffsetIncremented)
    replicaManager.logManager.checkpointLogRecoveryOffsets()
    replicaManager.logManager.checkpointLogStartOffsets()
    assertEquals(Some(1L), readRecoveryPointCheckpoint().get(tp0))
    assertEquals(Some(1L), readLogStartOffsetCheckpoint().get(tp0))

    if (throwIOException) {
      // Delete the underlying directory to trigger an KafkaStorageException
      val dir = partition.log.get.dir
      Utils.delete(dir)
      dir.createNewFile()
    }

    val partitionStates = Map(tp0 -> new StopReplicaPartitionState()
      .setPartitionIndex(tp0.partition)
      .setLeaderEpoch(leaderEpoch)
      .setDeletePartition(deletePartition)
    )

    val (result, error) = replicaManager.stopReplicas(1, 0, 0, brokerEpoch, partitionStates)
    assertEquals(Errors.NONE, error)
    assertEquals(Map(tp0 -> expectedOutput), result)

    if (expectedOutput == Errors.NONE && deletePartition) {
      assertEquals(HostedPartition.None, replicaManager.getPartition(tp0))
      assertFalse(readRecoveryPointCheckpoint().contains(tp0))
      assertFalse(readLogStartOffsetCheckpoint().contains(tp0))
    }
  }

  @Test
  def testReplicaNotAvailable(): Unit = {

    def createReplicaManager(): ReplicaManager = {
      val props = TestUtils.createBrokerConfig(1, TestUtils.MockZkConnect)
      val config = KafkaConfig.fromProps(props)
      val mockLogMgr = TestUtils.createLogManager(config.logDirs.map(new File(_)))
      new ReplicaManager(config, metrics, time, None, new MockScheduler(time), mockLogMgr,
        new AtomicBoolean(false), quotaManager, new BrokerTopicStats,
        MetadataCache.zkMetadataCache(config.brokerId), new LogDirFailureChannel(config.logDirs.size), alterIsrManager, configRepository) {
        override def getPartitionOrException(topicPartition: TopicPartition): Partition = {
          throw Errors.NOT_LEADER_OR_FOLLOWER.exception()
        }
      }
    }

    val replicaManager = createReplicaManager()
    try {
      val tp = new TopicPartition(topic, 0)
      val dir = replicaManager.logManager.liveLogDirs.head.getAbsolutePath
      val errors = replicaManager.alterReplicaLogDirs(Map(tp -> dir))
      assertEquals(Errors.REPLICA_NOT_AVAILABLE, errors(tp))
    } finally {
      replicaManager.shutdown(false)
    }
  }

  @Test
  def testPartitionMetadataFile(): Unit = {
    val replicaManager = setupReplicaManagerWithMockedPurgatories(new MockTimer(time))
    try {
      val brokerList = Seq[Integer](0, 1).asJava
      val topicPartition = new TopicPartition(topic, 0)
      val topicIds = Collections.singletonMap(topic, Uuid.randomUuid())
      val topicNames = topicIds.asScala.map(_.swap).asJava

      def leaderAndIsrRequest(epoch: Int, topicIds: java.util.Map[String, Uuid]): LeaderAndIsrRequest =
        new LeaderAndIsrRequest.Builder(ApiKeys.LEADER_AND_ISR.latestVersion, 0, 0, brokerEpoch,
          Seq(new LeaderAndIsrPartitionState()
            .setTopicName(topic)
            .setPartitionIndex(0)
            .setControllerEpoch(0)
            .setLeader(0)
            .setLeaderEpoch(epoch)
            .setIsr(brokerList)
            .setZkVersion(0)
            .setReplicas(brokerList)
            .setIsNew(true)).asJava,
          topicIds,
          Set(new Node(0, "host1", 0), new Node(1, "host2", 1)).asJava).build()

      val response = replicaManager.becomeLeaderOrFollower(0, leaderAndIsrRequest(0, topicIds), (_, _) => ())
      assertEquals(Errors.NONE, response.partitionErrors(topicNames).get(topicPartition))
      assertFalse(replicaManager.localLog(topicPartition).isEmpty)
      val id = topicIds.get(topicPartition.topic())
      val log = replicaManager.localLog(topicPartition).get
      assertTrue(log.partitionMetadataFile.exists())
      val partitionMetadata = log.partitionMetadataFile.read()

      // Current version of PartitionMetadataFile is 0.
      assertEquals(0, partitionMetadata.version)
      assertEquals(id, partitionMetadata.topicId)
    } finally replicaManager.shutdown(checkpointHW = false)
  }

  @Test
  def testPartitionMetadataFileCreatedWithExistingLog(): Unit = {
    val replicaManager = setupReplicaManagerWithMockedPurgatories(new MockTimer(time))
    try {
      val brokerList = Seq[Integer](0, 1).asJava
      val topicPartition = new TopicPartition(topic, 0)

      replicaManager.logManager.getOrCreateLog(topicPartition, isNew = true, topicId = None)

      assertTrue(replicaManager.getLog(topicPartition).isDefined)
      var log = replicaManager.getLog(topicPartition).get
      assertEquals(None, log.topicId)
      assertFalse(log.partitionMetadataFile.exists())

      val topicIds = Collections.singletonMap(topic, Uuid.randomUuid())
      val topicNames = topicIds.asScala.map(_.swap).asJava

      def leaderAndIsrRequest(epoch: Int): LeaderAndIsrRequest = new LeaderAndIsrRequest.Builder(ApiKeys.LEADER_AND_ISR.latestVersion, 0, 0, brokerEpoch,
        Seq(new LeaderAndIsrPartitionState()
          .setTopicName(topic)
          .setPartitionIndex(0)
          .setControllerEpoch(0)
          .setLeader(0)
          .setLeaderEpoch(epoch)
          .setIsr(brokerList)
          .setZkVersion(0)
          .setReplicas(brokerList)
          .setIsNew(true)).asJava,
        topicIds,
        Set(new Node(0, "host1", 0), new Node(1, "host2", 1)).asJava).build()

      val response = replicaManager.becomeLeaderOrFollower(0, leaderAndIsrRequest(0), (_, _) => ())
      assertEquals(Errors.NONE, response.partitionErrors(topicNames).get(topicPartition))
      assertFalse(replicaManager.localLog(topicPartition).isEmpty)
      val id = topicIds.get(topicPartition.topic())
      log = replicaManager.localLog(topicPartition).get
      assertTrue(log.partitionMetadataFile.exists())
      val partitionMetadata = log.partitionMetadataFile.read()

      // Current version of PartitionMetadataFile is 0.
      assertEquals(0, partitionMetadata.version)
      assertEquals(id, partitionMetadata.topicId)
    } finally replicaManager.shutdown(checkpointHW = false)
  }

  @Test
  def testInconsistentIdReturnsError(): Unit = {
    val replicaManager = setupReplicaManagerWithMockedPurgatories(new MockTimer(time))
    try {
      val brokerList = Seq[Integer](0, 1).asJava
      val topicPartition = new TopicPartition(topic, 0)
      val topicIds = Collections.singletonMap(topic, Uuid.randomUuid())
      val topicNames = topicIds.asScala.map(_.swap).asJava

      val invalidTopicIds = Collections.singletonMap(topic, Uuid.randomUuid())
      val invalidTopicNames = invalidTopicIds.asScala.map(_.swap).asJava

      def leaderAndIsrRequest(epoch: Int, topicIds: java.util.Map[String, Uuid]): LeaderAndIsrRequest =
        new LeaderAndIsrRequest.Builder(ApiKeys.LEADER_AND_ISR.latestVersion, 0, 0, brokerEpoch,
        Seq(new LeaderAndIsrPartitionState()
          .setTopicName(topic)
          .setPartitionIndex(0)
          .setControllerEpoch(0)
          .setLeader(0)
          .setLeaderEpoch(epoch)
          .setIsr(brokerList)
          .setZkVersion(0)
          .setReplicas(brokerList)
          .setIsNew(true)).asJava,
        topicIds,
        Set(new Node(0, "host1", 0), new Node(1, "host2", 1)).asJava).build()

      val response = replicaManager.becomeLeaderOrFollower(0, leaderAndIsrRequest(0, topicIds), (_, _) => ())
      assertEquals(Errors.NONE, response.partitionErrors(topicNames).get(topicPartition))

      val response2 = replicaManager.becomeLeaderOrFollower(0, leaderAndIsrRequest(1, topicIds), (_, _) => ())
      assertEquals(Errors.NONE, response2.partitionErrors(topicNames).get(topicPartition))

      // Send request with inconsistent ID.
      val response3 = replicaManager.becomeLeaderOrFollower(0, leaderAndIsrRequest(1, invalidTopicIds), (_, _) => ())
      assertEquals(Errors.INCONSISTENT_TOPIC_ID, response3.partitionErrors(invalidTopicNames).get(topicPartition))

      val response4 = replicaManager.becomeLeaderOrFollower(0, leaderAndIsrRequest(2, invalidTopicIds), (_, _) => ())
      assertEquals(Errors.INCONSISTENT_TOPIC_ID, response4.partitionErrors(invalidTopicNames).get(topicPartition))
    } finally replicaManager.shutdown(checkpointHW = false)
  }

  @Test
  def testPartitionMetadataFileNotCreated(): Unit = {
    val replicaManager = setupReplicaManagerWithMockedPurgatories(new MockTimer(time))
    try {
      val brokerList = Seq[Integer](0, 1).asJava
      val topicPartition = new TopicPartition(topic, 0)
      val topicPartitionFoo = new TopicPartition("foo", 0)
      val topicPartitionFake = new TopicPartition("fakeTopic", 0)
      val topicIds = Map(topic -> Uuid.ZERO_UUID, "foo" -> Uuid.randomUuid()).asJava
      val topicNames = topicIds.asScala.map(_.swap).asJava

      def leaderAndIsrRequest(epoch: Int, name: String, version: Short): LeaderAndIsrRequest = LeaderAndIsrRequest.parse(
        new LeaderAndIsrRequest.Builder(version, 0, 0, brokerEpoch,
        Seq(new LeaderAndIsrPartitionState()
          .setTopicName(name)
          .setPartitionIndex(0)
          .setControllerEpoch(0)
          .setLeader(0)
          .setLeaderEpoch(epoch)
          .setIsr(brokerList)
          .setZkVersion(0)
          .setReplicas(brokerList)
          .setIsNew(true)).asJava,
        topicIds,
        Set(new Node(0, "host1", 0), new Node(1, "host2", 1)).asJava).build().serialize(), version)

      // There is no file if the topic does not have an associated topic ID.
      val response = replicaManager.becomeLeaderOrFollower(0, leaderAndIsrRequest(0, "fakeTopic", ApiKeys.LEADER_AND_ISR.latestVersion), (_, _) => ())
      assertTrue(replicaManager.localLog(topicPartitionFake).isDefined)
      val log = replicaManager.localLog(topicPartitionFake).get
      assertFalse(log.partitionMetadataFile.exists())
      assertEquals(Errors.NONE, response.partitionErrors(topicNames).get(topicPartition))

      // There is no file if the topic has the default UUID.
      val response2 = replicaManager.becomeLeaderOrFollower(0, leaderAndIsrRequest(0, topic, ApiKeys.LEADER_AND_ISR.latestVersion), (_, _) => ())
      assertTrue(replicaManager.localLog(topicPartition).isDefined)
      val log2 = replicaManager.localLog(topicPartition).get
      assertFalse(log2.partitionMetadataFile.exists())
      assertEquals(Errors.NONE, response2.partitionErrors(topicNames).get(topicPartition))

      // There is no file if the request an older version
      val response3 = replicaManager.becomeLeaderOrFollower(0, leaderAndIsrRequest(0, "foo", 0), (_, _) => ())
      assertTrue(replicaManager.localLog(topicPartitionFoo).isDefined)
      val log3 = replicaManager.localLog(topicPartitionFoo).get
      assertFalse(log3.partitionMetadataFile.exists())
      assertEquals(Errors.NONE, response3.partitionErrors(topicNames).get(topicPartitionFoo))

      // There is no file if the request is an older version
      val response4 = replicaManager.becomeLeaderOrFollower(0, leaderAndIsrRequest(1, "foo", 4), (_, _) => ())
      assertTrue(replicaManager.localLog(topicPartitionFoo).isDefined)
      val log4 = replicaManager.localLog(topicPartitionFoo).get
      assertFalse(log4.partitionMetadataFile.exists())
      assertEquals(Errors.NONE, response4.partitionErrors(topicNames).get(topicPartitionFoo))
    } finally replicaManager.shutdown(checkpointHW = false)
  }

  private def leaderAndIsrRequest(
    topicId: Uuid,
    topicPartition: TopicPartition,
    replicas: Seq[Int],
    leaderAndIsr: LeaderAndIsr,
    isNew: Boolean = true,
    brokerEpoch: Int = 0,
    controllerId: Int = 0,
    controllerEpoch: Int = 0,
    version: Short = LeaderAndIsrRequestData.HIGHEST_SUPPORTED_VERSION
  ): LeaderAndIsrRequest = {
    val partitionState = new LeaderAndIsrPartitionState()
      .setTopicName(topicPartition.topic)
      .setPartitionIndex(topicPartition.partition)
      .setControllerEpoch(controllerEpoch)
      .setLeader(leaderAndIsr.leader)
      .setLeaderEpoch(leaderAndIsr.leaderEpoch)
      .setIsr(leaderAndIsr.isr.map(Int.box).asJava)
      .setZkVersion(leaderAndIsr.zkVersion)
      .setReplicas(replicas.map(Int.box).asJava)
      .setIsNew(isNew)

    def mkNode(replicaId: Int): Node = {
      new Node(replicaId, s"host-$replicaId", 9092)
    }

    val nodes = Set(mkNode(controllerId)) ++ replicas.map(mkNode).toSet

    new LeaderAndIsrRequest.Builder(
      version,
      controllerId,
      controllerEpoch,
      brokerEpoch,
      Seq(partitionState).asJava,
      Map(topicPartition.topic -> topicId).asJava,
      nodes.asJava
    ).build()
  }

  @Test
  def testActiveProducerState(): Unit = {
    val brokerId = 0
    val replicaManager = setupReplicaManagerWithMockedPurgatories(new MockTimer(time), brokerId)
    try {
      val fooPartition = new TopicPartition("foo", 0)
      Mockito.when(replicaManager.metadataCache.contains(fooPartition)).thenReturn(false)
      val fooProducerState = replicaManager.activeProducerState(fooPartition)
      assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION, Errors.forCode(fooProducerState.errorCode))

      val oofPartition = new TopicPartition("oof", 0)
      Mockito.when(replicaManager.metadataCache.contains(oofPartition)).thenReturn(true)
      val oofProducerState = replicaManager.activeProducerState(oofPartition)
      assertEquals(Errors.NOT_LEADER_OR_FOLLOWER, Errors.forCode(oofProducerState.errorCode))

      // This API is supported by both leaders and followers

      val barPartition = new TopicPartition("bar", 0)
      val barLeaderAndIsrRequest = leaderAndIsrRequest(
        topicId = Uuid.randomUuid(),
        topicPartition = barPartition,
        replicas = Seq(brokerId),
        leaderAndIsr = LeaderAndIsr(brokerId, List(brokerId))
      )
      replicaManager.becomeLeaderOrFollower(0, barLeaderAndIsrRequest, (_, _) => ())
      val barProducerState = replicaManager.activeProducerState(barPartition)
      assertEquals(Errors.NONE, Errors.forCode(barProducerState.errorCode))

      val otherBrokerId = 1
      val bazPartition = new TopicPartition("baz", 0)
      val bazLeaderAndIsrRequest = leaderAndIsrRequest(
        topicId = Uuid.randomUuid(),
        topicPartition = bazPartition,
        replicas = Seq(brokerId, otherBrokerId),
        leaderAndIsr = LeaderAndIsr(otherBrokerId, List(brokerId, otherBrokerId))
      )
      replicaManager.becomeLeaderOrFollower(0, bazLeaderAndIsrRequest, (_, _) => ())
      val bazProducerState = replicaManager.activeProducerState(bazPartition)
      assertEquals(Errors.NONE, Errors.forCode(bazProducerState.errorCode))
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }

  }

}
