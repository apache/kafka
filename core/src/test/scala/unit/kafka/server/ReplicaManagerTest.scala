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
import java.nio.file.Files
import java.util
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong, AtomicReference}
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.stream.IntStream
import java.util.{Collections, Optional, Properties}
import kafka.api._
import kafka.cluster.{BrokerEndPoint, Partition}
import kafka.log._
import kafka.server.QuotaFactory.{QuotaManagers, UnboundedQuota}
import kafka.server.checkpoints.{LazyOffsetCheckpoints, OffsetCheckpointFile}
import kafka.server.epoch.util.MockBlockingSender
import kafka.utils.timer.MockTimer
import kafka.utils.{MockScheduler, MockTime, Pool, TestUtils}
import org.apache.kafka.clients.FetchSessionHandler
import org.apache.kafka.common.errors.KafkaStorageException
import org.apache.kafka.common.message.FetchResponseData
import org.apache.kafka.common.message.LeaderAndIsrRequestData
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.EpochEndOffset
import org.apache.kafka.common.message.StopReplicaRequestData.StopReplicaPartitionState
import org.apache.kafka.common.metadata.{PartitionChangeRecord, PartitionRecord, RemoveTopicRecord, TopicRecord}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record._
import org.apache.kafka.common.replica.{ClientMetadata, PartitionView, ReplicaSelector, ReplicaView}
import org.apache.kafka.common.replica.ClientMetadata.DefaultClientMetadata
import org.apache.kafka.common.replica.ReplicaView.DefaultReplicaView
import org.apache.kafka.common.requests.FetchRequest.PartitionData
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.requests._
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.utils.{LogContext, Time, Utils}
import org.apache.kafka.common.{IsolationLevel, Node, TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.image.{AclsImage, ClientQuotasImage, ClusterImageTest, ConfigurationsImage, FeaturesImage, MetadataImage, MetadataProvenance, ProducerIdsImage, TopicsDelta, TopicsImage}
import org.apache.kafka.metadata.LeaderConstants.NO_LEADER
import org.apache.kafka.metadata.LeaderRecoveryState
import org.apache.kafka.server.common.MetadataVersion.IBP_2_6_IV0
import org.apache.kafka.server.log.internals.{AppendOrigin, LogDirFailureChannel, LogOffsetMetadata}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.{any, anyInt, anyString}
import org.mockito.Mockito.{mock, never, reset, times, verify, when}

import scala.collection.{Map, Seq, mutable}
import scala.jdk.CollectionConverters._

class ReplicaManagerTest {

  val topic = "test-topic"
  val topicId = Uuid.randomUuid()
  val topicIds = scala.Predef.Map("test-topic" -> topicId)
  val topicNames = scala.Predef.Map(topicId -> "test-topic")
  val time = new MockTime
  val scheduler = new MockScheduler(time)
  val metrics = new Metrics
  var alterPartitionManager: AlterPartitionManager = _
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
    alterPartitionManager = mock(classOf[AlterPartitionManager])
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
    val rm = new ReplicaManager(
      metrics = metrics,
      config = config,
      time = time,
      scheduler = new MockScheduler(time),
      logManager = mockLogMgr,
      quotaManagers = quotaManager,
      metadataCache = MetadataCache.zkMetadataCache(config.brokerId, config.interBrokerProtocolVersion),
      logDirFailureChannel = new LogDirFailureChannel(config.logDirs.size),
      alterPartitionManager = alterPartitionManager)
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
    val rm = new ReplicaManager(
      metrics = metrics,
      config = config,
      time = time,
      scheduler = new MockScheduler(time),
      logManager = mockLogMgr,
      quotaManagers = quotaManager,
      metadataCache = MetadataCache.zkMetadataCache(config.brokerId, config.interBrokerProtocolVersion),
      logDirFailureChannel = new LogDirFailureChannel(config.logDirs.size),
      alterPartitionManager = alterPartitionManager)
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
    val rm = new ReplicaManager(
      metrics = metrics,
      config = config,
      time = time,
      scheduler = new MockScheduler(time),
      logManager = mockLogMgr,
      quotaManagers = quotaManager,
      metadataCache = MetadataCache.zkMetadataCache(config.brokerId, config.interBrokerProtocolVersion),
      logDirFailureChannel = new LogDirFailureChannel(config.logDirs.size),
      alterPartitionManager = alterPartitionManager,
      threadNamePrefix = Option(this.getClass.getName))
    try {
      def callback(responseStatus: Map[TopicPartition, PartitionResponse]): Unit = {
        assert(responseStatus.values.head.error == Errors.INVALID_REQUIRED_ACKS)
      }
      rm.appendRecords(
        timeout = 0,
        requiredAcks = 3,
        internalTopicsAllowed = false,
        origin = AppendOrigin.CLIENT,
        entriesPerPartition = Map(new TopicPartition("test1", 0) -> MemoryRecords.withRecords(CompressionType.NONE,
          new SimpleRecord("first message".getBytes))),
        responseCallback = callback)
    } finally {
      rm.shutdown(checkpointHW = false)
    }

    TestUtils.assertNoNonDaemonThreads(this.getClass.getName)
  }

  private def mockGetAliveBrokerFunctions(cache: MetadataCache, aliveBrokers: Seq[Node]): Unit = {
    when(cache.hasAliveBroker(anyInt)).thenAnswer(new Answer[Boolean]() {
      override def answer(invocation: InvocationOnMock): Boolean = {
        aliveBrokers.map(_.id()).contains(invocation.getArgument(0).asInstanceOf[Int])
      }
    })
    when(cache.getAliveBrokerNode(anyInt, any[ListenerName])).
      thenAnswer(new Answer[Option[Node]]() {
        override def answer(invocation: InvocationOnMock): Option[Node] = {
          aliveBrokers.find(node => node.id == invocation.getArgument(0).asInstanceOf[Integer])
        }
      })
    when(cache.getAliveBrokerNodes(any[ListenerName])).thenReturn(aliveBrokers)
  }

  @Test
  def testClearPurgatoryOnBecomingFollower(): Unit = {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect)
    props.put("log.dir", TestUtils.tempRelativeDir("data").getAbsolutePath)
    val config = KafkaConfig.fromProps(props)
    val logProps = new Properties()
    val mockLogMgr = TestUtils.createLogManager(config.logDirs.map(new File(_)), LogConfig(logProps))
    val aliveBrokers = Seq(new Node(0, "host0", 0), new Node(1, "host1", 1))
    val metadataCache: MetadataCache = mock(classOf[MetadataCache])
    mockGetAliveBrokerFunctions(metadataCache, aliveBrokers)
    when(metadataCache.metadataVersion()).thenReturn(config.interBrokerProtocolVersion)
    val rm = new ReplicaManager(
      metrics = metrics,
      config = config,
      time = time,
      scheduler = new MockScheduler(time),
      logManager = mockLogMgr,
      quotaManagers = quotaManager,
      metadataCache = metadataCache,
      logDirFailureChannel = new LogDirFailureChannel(config.logDirs.size),
      alterPartitionManager = alterPartitionManager)

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
          .setPartitionEpoch(0)
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
          .setPartitionEpoch(0)
          .setReplicas(brokerList)
          .setIsNew(false)).asJava,
        topicIds,
        Set(new Node(0, "host1", 0), new Node(1, "host2", 1)).asJava).build()
      rm.becomeLeaderOrFollower(1, leaderAndIsrRequest2, (_, _) => ())

      assertTrue(appendResult.hasFired)
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

      def leaderAndIsrRequest(epoch: Int): LeaderAndIsrRequest = new LeaderAndIsrRequest.Builder(ApiKeys.LEADER_AND_ISR.latestVersion, 0, 0, brokerEpoch,
        Seq(new LeaderAndIsrPartitionState()
          .setTopicName(topic)
          .setPartitionIndex(0)
          .setControllerEpoch(0)
          .setLeader(0)
          .setLeaderEpoch(epoch)
          .setIsr(brokerList)
          .setPartitionEpoch(0)
          .setReplicas(brokerList)
          .setIsNew(true)).asJava,
        topicIds.asJava,
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
          .setPartitionEpoch(0)
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
  def testPartitionsWithLateTransactionsCount(): Unit = {
    val timer = new MockTimer(time)
    val replicaManager = setupReplicaManagerWithMockedPurgatories(timer)
    val topicPartition = new TopicPartition(topic, 0)

    def assertLateTransactionCount(expectedCount: Option[Int]): Unit = {
      assertEquals(expectedCount, TestUtils.yammerGaugeValue[Int]("PartitionsWithLateTransactionsCount"))
    }

    try {
      assertLateTransactionCount(Some(0))

      val partition = replicaManager.createPartition(topicPartition)
      partition.createLogIfNotExists(isNew = false, isFutureReplica = false,
        new LazyOffsetCheckpoints(replicaManager.highWatermarkCheckpoints), None)

      // Make this replica the leader.
      val brokerList = Seq[Integer](0, 1, 2).asJava
      val leaderAndIsrRequest1 = new LeaderAndIsrRequest.Builder(ApiKeys.LEADER_AND_ISR.latestVersion, 0, 0, brokerEpoch,
        Seq(new LeaderAndIsrPartitionState()
          .setTopicName(topic)
          .setPartitionIndex(0)
          .setControllerEpoch(0)
          .setLeader(0)
          .setLeaderEpoch(0)
          .setIsr(brokerList)
          .setPartitionEpoch(0)
          .setReplicas(brokerList)
          .setIsNew(true)).asJava,
        topicIds.asJava,
        Set(new Node(0, "host1", 0), new Node(1, "host2", 1)).asJava).build()
      replicaManager.becomeLeaderOrFollower(0, leaderAndIsrRequest1, (_, _) => ())

      // Start a transaction
      val producerId = 234L
      val epoch = 5.toShort
      val sequence = 9
      val records = MemoryRecords.withTransactionalRecords(CompressionType.NONE, producerId, epoch, sequence,
        new SimpleRecord(time.milliseconds(), s"message $sequence".getBytes))
      appendRecords(replicaManager, new TopicPartition(topic, 0), records).onFire { response =>
        assertEquals(Errors.NONE, response.error)
      }
      assertLateTransactionCount(Some(0))

      // The transaction becomes late if not finished before the max transaction timeout passes
      time.sleep(replicaManager.logManager.maxTransactionTimeoutMs + ProducerStateManager.LateTransactionBufferMs)
      assertLateTransactionCount(Some(0))
      time.sleep(1)
      assertLateTransactionCount(Some(1))

      // After the late transaction is aborted, we expect the count to return to 0
      val abortTxnMarker = new EndTransactionMarker(ControlRecordType.ABORT, 0)
      val abortRecordBatch = MemoryRecords.withEndTransactionMarker(producerId, epoch, abortTxnMarker)
      appendRecords(replicaManager, new TopicPartition(topic, 0),
        abortRecordBatch, origin = AppendOrigin.COORDINATOR).onFire { response =>
        assertEquals(Errors.NONE, response.error)
      }
      assertLateTransactionCount(Some(0))
    } finally {
      // After shutdown, the metric should no longer be registered
      replicaManager.shutdown(checkpointHW = false)
      assertLateTransactionCount(None)
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
          .setPartitionEpoch(0)
          .setReplicas(brokerList)
          .setIsNew(true)).asJava,
        topicIds.asJava,
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
      fetchPartitionAsFollower(
        replicaManager,
        new TopicIdPartition(topicId, new TopicPartition(topic, 0)),
        new PartitionData(Uuid.ZERO_UUID, numRecords, 0, 100000, Optional.empty()),
        replicaId = 1
      )

      // fetch should return empty since LSO should be stuck at 0
      var consumerFetchResult = fetchPartitionAsConsumer(replicaManager, new TopicIdPartition(topicId, new TopicPartition(topic, 0)),
        new PartitionData(Uuid.ZERO_UUID, 0, 0, 100000, Optional.empty()),
        isolationLevel = IsolationLevel.READ_COMMITTED)
      var fetchData = consumerFetchResult.assertFired
      assertEquals(Errors.NONE, fetchData.error)
      assertTrue(fetchData.records.batches.asScala.isEmpty)
      assertEquals(Some(0), fetchData.lastStableOffset)
      assertEquals(Some(List.empty[FetchResponseData.AbortedTransaction]), fetchData.abortedTransactions)

      // delayed fetch should timeout and return nothing
      consumerFetchResult = fetchPartitionAsConsumer(
        replicaManager,
        new TopicIdPartition(topicId, new TopicPartition(topic, 0)),
        new PartitionData(Uuid.ZERO_UUID, 0, 0, 100000, Optional.empty()),
        isolationLevel = IsolationLevel.READ_COMMITTED,
        minBytes = 1000,
        maxWaitMs = 1000
      )
      assertFalse(consumerFetchResult.hasFired)
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
        origin = AppendOrigin.COORDINATOR)
        .onFire { response => assertEquals(Errors.NONE, response.error) }

      // the LSO has advanced, but the appended commit marker has not been replicated, so
      // none of the data from the transaction should be visible yet
      consumerFetchResult = fetchPartitionAsConsumer(
        replicaManager,
        new TopicIdPartition(topicId, new TopicPartition(topic, 0)),
        new PartitionData(Uuid.ZERO_UUID, 0, 0, 100000, Optional.empty()),
        isolationLevel = IsolationLevel.READ_COMMITTED
      )

      fetchData = consumerFetchResult.assertFired
      assertEquals(Errors.NONE, fetchData.error)
      assertTrue(fetchData.records.batches.asScala.isEmpty)

      // fetch as follower to advance the high watermark
      fetchPartitionAsFollower(
        replicaManager,
        new TopicIdPartition(topicId, new TopicPartition(topic, 0)),
        new PartitionData(Uuid.ZERO_UUID, numRecords + 1, 0, 100000, Optional.empty()),
        replicaId = 1
      )

      // now all of the records should be fetchable
      consumerFetchResult = fetchPartitionAsConsumer(replicaManager, new TopicIdPartition(topicId, new TopicPartition(topic, 0)),
        new PartitionData(Uuid.ZERO_UUID, 0, 0, 100000, Optional.empty()),
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
          .setPartitionEpoch(0)
          .setReplicas(brokerList)
          .setIsNew(true)).asJava,
        topicIds.asJava,
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
        origin = AppendOrigin.COORDINATOR)
        .onFire { response => assertEquals(Errors.NONE, response.error) }

      // fetch as follower to advance the high watermark
      fetchPartitionAsFollower(
        replicaManager,
        new TopicIdPartition(topicId, new TopicPartition(topic, 0)),
        new PartitionData(Uuid.ZERO_UUID, numRecords + 1, 0, 100000, Optional.empty()),
        replicaId = 1
      )

      // Set the minBytes in order force this request to enter purgatory. When it returns, we should still
      // see the newly aborted transaction.
      val fetchResult = fetchPartitionAsConsumer(
        replicaManager,
        new TopicIdPartition(topicId, new TopicPartition(topic, 0)),
        new PartitionData(Uuid.ZERO_UUID, 0, 0, 100000, Optional.empty()),
        isolationLevel = IsolationLevel.READ_COMMITTED,
        minBytes = 10000,
        maxWaitMs = 1000
      )
      assertFalse(fetchResult.hasFired)

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
          .setPartitionEpoch(0)
          .setReplicas(brokerList)
          .setIsNew(false)).asJava,
        topicIds.asJava,
        Set(new Node(0, "host1", 0), new Node(1, "host2", 1), new Node(2, "host2", 2)).asJava).build()
      rm.becomeLeaderOrFollower(0, leaderAndIsrRequest1, (_, _) => ())
      rm.getPartitionOrException(new TopicPartition(topic, 0))
        .localLogOrException

      // Append a couple of messages.
      for (i <- 1 to 2) {
        val records = TestUtils.singletonRecords(s"message $i".getBytes)
        appendRecords(rm, new TopicPartition(topic, 0), records).onFire { response =>
          assertEquals(Errors.NONE, response.error)
        }
      }

      // Followers are always allowed to fetch above the high watermark
      val followerFetchResult = fetchPartitionAsFollower(
        rm,
        new TopicIdPartition(topicId, new TopicPartition(topic, 0)),
        new PartitionData(Uuid.ZERO_UUID, 1, 0, 100000, Optional.empty()),
        replicaId = 1
      )
      val followerFetchData = followerFetchResult.assertFired
      assertEquals(Errors.NONE, followerFetchData.error, "Should not give an exception")
      assertTrue(followerFetchData.records.batches.iterator.hasNext, "Should return some data")

      // Consumers are not allowed to consume above the high watermark. However, since the
      // high watermark could be stale at the time of the request, we do not return an out of
      // range error and instead return an empty record set.
      val consumerFetchResult = fetchPartitionAsConsumer(rm, new TopicIdPartition(topicId, new TopicPartition(topic, 0)),
        new PartitionData(Uuid.ZERO_UUID, 1, 0, 100000, Optional.empty()))
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
      val tidp = new TopicIdPartition(topicId, tp)
      val replicas = aliveBrokersIds.toList.map(Int.box).asJava

      // Broker 0 becomes leader of the partition
      val leaderAndIsrPartitionState = new LeaderAndIsrPartitionState()
        .setTopicName(topic)
        .setPartitionIndex(0)
        .setControllerEpoch(0)
        .setLeader(0)
        .setLeaderEpoch(leaderEpoch)
        .setIsr(replicas)
        .setPartitionEpoch(0)
        .setReplicas(replicas)
        .setIsNew(true)
      val leaderAndIsrRequest = new LeaderAndIsrRequest.Builder(ApiKeys.LEADER_AND_ISR.latestVersion, 0, 0, brokerEpoch,
        Seq(leaderAndIsrPartitionState).asJava,
        Collections.singletonMap(topic, topicId),
        Set(new Node(0, "host1", 0), new Node(1, "host2", 1)).asJava).build()
      val leaderAndIsrResponse = replicaManager.becomeLeaderOrFollower(0, leaderAndIsrRequest, (_, _) => ())
      assertEquals(Errors.NONE, leaderAndIsrResponse.error)

      // Follower replica state is initialized, but initial state is not known
      assertTrue(replicaManager.onlinePartition(tp).isDefined)
      val partition = replicaManager.onlinePartition(tp).get

      assertTrue(partition.getReplica(1).isDefined)
      val followerReplica = partition.getReplica(1).get
      assertEquals(-1L, followerReplica.stateSnapshot.logStartOffset)
      assertEquals(-1L, followerReplica.stateSnapshot.logEndOffset)

      // Leader appends some data
      for (i <- 1 to 5) {
        appendRecords(replicaManager, tp, TestUtils.singletonRecords(s"message $i".getBytes)).onFire { response =>
          assertEquals(Errors.NONE, response.error)
        }
      }

      // We receive one valid request from the follower and replica state is updated
      val validFetchPartitionData = new FetchRequest.PartitionData(Uuid.ZERO_UUID, 0L, 0L, maxFetchBytes,
        Optional.of(leaderEpoch))

      val validFetchResult = fetchPartitionAsFollower(
        replicaManager,
        tidp,
        validFetchPartitionData,
        replicaId = 1
      )

      assertEquals(Errors.NONE, validFetchResult.assertFired.error)
      assertEquals(0L, followerReplica.stateSnapshot.logStartOffset)
      assertEquals(0L, followerReplica.stateSnapshot.logEndOffset)

      // Next we receive an invalid request with a higher fetch offset, but an old epoch.
      // We expect that the replica state does not get updated.
      val invalidFetchPartitionData = new FetchRequest.PartitionData(Uuid.ZERO_UUID, 3L, 0L, maxFetchBytes,
        Optional.of(leaderEpoch - 1))


      val invalidFetchResult = fetchPartitionAsFollower(
        replicaManager,
        tidp,
        invalidFetchPartitionData,
        replicaId = 1
      )

      assertEquals(Errors.FENCED_LEADER_EPOCH, invalidFetchResult.assertFired.error)
      assertEquals(0L, followerReplica.stateSnapshot.logStartOffset)
      assertEquals(0L, followerReplica.stateSnapshot.logEndOffset)

      // Next we receive an invalid request with a higher fetch offset, but a diverging epoch.
      // We expect that the replica state does not get updated.
      val divergingFetchPartitionData = new FetchRequest.PartitionData(tidp.topicId, 3L, 0L, maxFetchBytes,
        Optional.of(leaderEpoch), Optional.of(leaderEpoch - 1))

      val divergingEpochResult = fetchPartitionAsFollower(
        replicaManager,
        tidp,
        divergingFetchPartitionData,
        replicaId = 1
      )

      assertEquals(Errors.NONE, divergingEpochResult.assertFired.error)
      assertTrue(divergingEpochResult.assertFired.divergingEpoch.isDefined)
      assertEquals(0L, followerReplica.stateSnapshot.logStartOffset)
      assertEquals(0L, followerReplica.stateSnapshot.logEndOffset)
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testFetchMessagesWithInconsistentTopicId(): Unit = {
    val maxFetchBytes = 1024 * 1024
    val aliveBrokersIds = Seq(0, 1)
    val leaderEpoch = 5
    val replicaManager = setupReplicaManagerWithMockedPurgatories(new MockTimer(time),
      brokerId = 0, aliveBrokersIds)
    try {
      val tp = new TopicPartition(topic, 0)
      val tidp = new TopicIdPartition(topicId, tp)
      val replicas = aliveBrokersIds.toList.map(Int.box).asJava

      // Broker 0 becomes leader of the partition
      val leaderAndIsrPartitionState = new LeaderAndIsrPartitionState()
        .setTopicName(topic)
        .setPartitionIndex(0)
        .setControllerEpoch(0)
        .setLeader(0)
        .setLeaderEpoch(leaderEpoch)
        .setIsr(replicas)
        .setPartitionEpoch(0)
        .setReplicas(replicas)
        .setIsNew(true)
      val leaderAndIsrRequest = new LeaderAndIsrRequest.Builder(ApiKeys.LEADER_AND_ISR.latestVersion, 0, 0, brokerEpoch,
        Seq(leaderAndIsrPartitionState).asJava,
        Collections.singletonMap(topic, topicId),
        Set(new Node(0, "host1", 0), new Node(1, "host2", 1)).asJava).build()
      val leaderAndIsrResponse = replicaManager.becomeLeaderOrFollower(0, leaderAndIsrRequest, (_, _) => ())
      assertEquals(Errors.NONE, leaderAndIsrResponse.error)

      assertEquals(Some(topicId), replicaManager.getPartitionOrException(tp).topicId)

      // We receive one valid request from the follower and replica state is updated
      var successfulFetch: Seq[(TopicIdPartition, FetchPartitionData)] = Seq()

      val validFetchPartitionData = new FetchRequest.PartitionData(Uuid.ZERO_UUID, 0L, 0L, maxFetchBytes,
        Optional.of(leaderEpoch))

      // Fetch messages simulating a different ID than the one in the log.
      val inconsistentTidp = new TopicIdPartition(Uuid.randomUuid(), tidp.topicPartition)
      def callback(response: Seq[(TopicIdPartition, FetchPartitionData)]): Unit = {
        successfulFetch = response
      }

      fetchPartitions(
        replicaManager,
        replicaId = 1,
        fetchInfos = Seq(inconsistentTidp -> validFetchPartitionData),
        responseCallback = callback
      )

      val fetch1 = successfulFetch.headOption.filter(_._1 == inconsistentTidp).map(_._2)
      assertTrue(fetch1.isDefined)
      assertEquals(Errors.INCONSISTENT_TOPIC_ID, fetch1.get.error)

      // Simulate where the fetch request did not use topic IDs
      // Fetch messages simulating an ID in the log.
      // We should not see topic ID errors.
      val zeroTidp = new TopicIdPartition(Uuid.ZERO_UUID, tidp.topicPartition)
      fetchPartitions(
        replicaManager,
        replicaId = 1,
        fetchInfos = Seq(zeroTidp -> validFetchPartitionData),
        responseCallback = callback
      )
      val fetch2 = successfulFetch.headOption.filter(_._1 == zeroTidp).map(_._2)
      assertTrue(fetch2.isDefined)
      assertEquals(Errors.NONE, fetch2.get.error)

      // Next create a topic without a topic ID written in the log.
      val tp2 = new TopicPartition("noIdTopic", 0)
      val tidp2 = new TopicIdPartition(Uuid.randomUuid(), tp2)

      // Broker 0 becomes leader of the partition
      val leaderAndIsrPartitionState2 = new LeaderAndIsrPartitionState()
        .setTopicName("noIdTopic")
        .setPartitionIndex(0)
        .setControllerEpoch(0)
        .setLeader(0)
        .setLeaderEpoch(leaderEpoch)
        .setIsr(replicas)
        .setPartitionEpoch(0)
        .setReplicas(replicas)
        .setIsNew(true)
      val leaderAndIsrRequest2 = new LeaderAndIsrRequest.Builder(ApiKeys.LEADER_AND_ISR.latestVersion, 0, 0, brokerEpoch,
        Seq(leaderAndIsrPartitionState2).asJava,
        Collections.emptyMap(),
        Set(new Node(0, "host1", 0), new Node(1, "host2", 1)).asJava).build()
      val leaderAndIsrResponse2 = replicaManager.becomeLeaderOrFollower(0, leaderAndIsrRequest2, (_, _) => ())
      assertEquals(Errors.NONE, leaderAndIsrResponse2.error)

      assertEquals(None, replicaManager.getPartitionOrException(tp2).topicId)

      // Fetch messages simulating the request containing a topic ID. We should not have an error.
      fetchPartitions(
        replicaManager,
        replicaId = 1,
        fetchInfos = Seq(tidp2 -> validFetchPartitionData),
        responseCallback = callback
      )
      val fetch3 = successfulFetch.headOption.filter(_._1 == tidp2).map(_._2)
      assertTrue(fetch3.isDefined)
      assertEquals(Errors.NONE, fetch3.get.error)

      // Fetch messages simulating the request not containing a topic ID. We should not have an error.
      val zeroTidp2 = new TopicIdPartition(Uuid.ZERO_UUID, tidp2.topicPartition)
      fetchPartitions(
        replicaManager,
        replicaId = 1,
        fetchInfos = Seq(zeroTidp2 -> validFetchPartitionData),
        responseCallback = callback
      )
      val fetch4 = successfulFetch.headOption.filter(_._1 == zeroTidp2).map(_._2)
      assertTrue(fetch4.isDefined)
      assertEquals(Errors.NONE, fetch4.get.error)

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
      val topicId = Uuid.randomUuid()
      val tidp0 = new TopicIdPartition(topicId, tp0)
      val tidp1 = new TopicIdPartition(topicId, tp1)
      val offsetCheckpoints = new LazyOffsetCheckpoints(replicaManager.highWatermarkCheckpoints)
      replicaManager.createPartition(tp0).createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints, None)
      replicaManager.createPartition(tp1).createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints, None)
      val partition0Replicas = Seq[Integer](0, 1).asJava
      val partition1Replicas = Seq[Integer](0, 2).asJava
      val topicIds = Map(tp0.topic -> topicId, tp1.topic -> topicId).asJava
      val leaderEpoch = 0
      val leaderAndIsrRequest = new LeaderAndIsrRequest.Builder(ApiKeys.LEADER_AND_ISR.latestVersion, 0, 0, brokerEpoch,
        Seq(
          new LeaderAndIsrPartitionState()
            .setTopicName(tp0.topic)
            .setPartitionIndex(tp0.partition)
            .setControllerEpoch(0)
            .setLeader(leaderEpoch)
            .setLeaderEpoch(0)
            .setIsr(partition0Replicas)
            .setPartitionEpoch(0)
            .setReplicas(partition0Replicas)
            .setIsNew(true),
          new LeaderAndIsrPartitionState()
            .setTopicName(tp1.topic)
            .setPartitionIndex(tp1.partition)
            .setControllerEpoch(0)
            .setLeader(0)
            .setLeaderEpoch(leaderEpoch)
            .setIsr(partition1Replicas)
            .setPartitionEpoch(0)
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

      def fetchCallback(responseStatus: Seq[(TopicIdPartition, FetchPartitionData)]): Unit = {
        val responseStatusMap = responseStatus.toMap
        assertEquals(2, responseStatus.size)
        assertEquals(Set(tidp0, tidp1), responseStatusMap.keySet)

        val tp0Status = responseStatusMap.get(tidp0)
        assertTrue(tp0Status.isDefined)
        // the response contains high watermark on the leader before it is updated based
        // on this fetch request
        assertEquals(0, tp0Status.get.highWatermark)
        assertEquals(Some(0), tp0Status.get.lastStableOffset)
        assertEquals(Errors.NONE, tp0Status.get.error)
        assertTrue(tp0Status.get.records.batches.iterator.hasNext)

        // Replica 1 is not a valid replica for partition 1
        val tp1Status = responseStatusMap.get(tidp1)
        assertEquals(Errors.UNKNOWN_LEADER_EPOCH, tp1Status.get.error)
      }

      fetchPartitions(
        replicaManager,
        replicaId = 1,
        fetchInfos = Seq(
          tidp0 -> new PartitionData(Uuid.ZERO_UUID, 1, 0, 100000, Optional.of[Integer](leaderEpoch)),
          tidp1 -> new PartitionData(Uuid.ZERO_UUID, 1, 0, 100000, Optional.of[Integer](leaderEpoch))
        ),
        responseCallback = fetchCallback,
        maxWaitMs = 1000,
        minBytes = 0,
        maxBytes = Int.MaxValue
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
    extraProps.put(KafkaConfig.InterBrokerProtocolVersionProp, IBP_2_6_IV0.version)
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
    val aliveBrokerIds = Seq[Integer](followerBrokerId, leaderBrokerId)
    val countDownLatch = new CountDownLatch(1)
    val offsetFromLeader = 5

    // Prepare the mocked components for the test
    val (replicaManager, mockLogMgr) = prepareReplicaManagerAndLogManager(new MockTimer(time),
      topicPartition, leaderEpoch + leaderEpochIncrement, followerBrokerId, leaderBrokerId, countDownLatch,
      expectTruncation = expectTruncation, localLogOffset = Some(10), offsetFromLeader = offsetFromLeader, extraProps = extraProps, topicId = Some(topicId))

    try {
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
      if (expectTruncation) {
        verify(mockLogMgr).truncateTo(Map(tp -> offsetFromLeader), isFuture = false)
      }

      verify(mockLogMgr).finishedInitializingLog(ArgumentMatchers.eq(tp), any())
    } finally {
      replicaManager.shutdown()
    }

    TestUtils.assertNoNonDaemonThreads(this.getClass.getName)
  }

  @Test
  def testReplicaSelector(): Unit = {
    val topicPartition = 0
    val followerBrokerId = 0
    val leaderBrokerId = 1
    val leaderEpoch = 1
    val leaderEpochIncrement = 2
    val aliveBrokerIds = Seq[Integer](followerBrokerId, leaderBrokerId)
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

    try {
      val brokerList = Seq[Integer](0, 1).asJava

      val tp0 = new TopicPartition(topic, 0)
      val tidp0 = new TopicIdPartition(topicId, tp0)

      // Make this replica the follower
      val leaderAndIsrRequest = new LeaderAndIsrRequest.Builder(ApiKeys.LEADER_AND_ISR.latestVersion, 0, 0, brokerEpoch,
        Seq(new LeaderAndIsrPartitionState()
          .setTopicName(topic)
          .setPartitionIndex(0)
          .setControllerEpoch(0)
          .setLeader(1)
          .setLeaderEpoch(1)
          .setIsr(brokerList)
          .setPartitionEpoch(0)
          .setReplicas(brokerList)
          .setIsNew(false)).asJava,
        Collections.singletonMap(topic, topicId),
        Set(new Node(0, "host1", 0), new Node(1, "host2", 1)).asJava).build()
      replicaManager.becomeLeaderOrFollower(1, leaderAndIsrRequest, (_, _) => ())

      val metadata: ClientMetadata = new DefaultClientMetadata("rack-a", "client-id",
        InetAddress.getByName("localhost"), KafkaPrincipal.ANONYMOUS, "default")

      val consumerResult = fetchPartitionAsConsumer(replicaManager, tidp0,
        new PartitionData(Uuid.ZERO_UUID, 0, 0, 100000, Optional.empty()),
        clientMetadata = Some(metadata))

      // Fetch from follower succeeds
      assertTrue(consumerResult.hasFired)

      // But only leader will compute preferred replica
      assertTrue(consumerResult.assertFired.preferredReadReplica.isEmpty)
    } finally {
      replicaManager.shutdown()
    }

    TestUtils.assertNoNonDaemonThreads(this.getClass.getName)
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

    try {
      val brokerList = Seq[Integer](0, 1).asJava

      val tp0 = new TopicPartition(topic, 0)
      val tidp0 = new TopicIdPartition(topicId, tp0)

      // Make this replica the leader
      val leaderAndIsrRequest = new LeaderAndIsrRequest.Builder(ApiKeys.LEADER_AND_ISR.latestVersion, 0, 0, brokerEpoch,
        Seq(new LeaderAndIsrPartitionState()
          .setTopicName(topic)
          .setPartitionIndex(0)
          .setControllerEpoch(0)
          .setLeader(0)
          .setLeaderEpoch(1)
          .setIsr(brokerList)
          .setPartitionEpoch(0)
          .setReplicas(brokerList)
          .setIsNew(false)).asJava,
        Collections.singletonMap(topic, topicId),
        Set(new Node(0, "host1", 0), new Node(1, "host2", 1)).asJava).build()
      replicaManager.becomeLeaderOrFollower(1, leaderAndIsrRequest, (_, _) => ())

      val metadata = new DefaultClientMetadata("rack-a", "client-id",
        InetAddress.getByName("localhost"), KafkaPrincipal.ANONYMOUS, "default")

      val consumerResult = fetchPartitionAsConsumer(replicaManager, tidp0,
        new PartitionData(Uuid.ZERO_UUID, 0, 0, 100000, Optional.empty()),
        clientMetadata = Some(metadata))

      // Fetch from leader succeeds
      assertTrue(consumerResult.hasFired)

      // Returns a preferred replica (should just be the leader, which is None)
      assertFalse(consumerResult.assertFired.preferredReadReplica.isDefined)
    } finally {
      replicaManager.shutdown()
    }

    TestUtils.assertNoNonDaemonThreads(this.getClass.getName)
  }

  @Test
  def testPreferredReplicaAsLeaderWhenSameRackFollowerIsOutOfIsr(): Unit = {
    val replicaManager = setupReplicaManagerWithMockedPurgatories(new MockTimer(time),
      propsModifier = props => props.put(KafkaConfig.ReplicaSelectorClassProp, classOf[MockReplicaSelector].getName))

    try {
      val leaderBrokerId = 0
      val followerBrokerId = 1
      val leaderNode = new Node(leaderBrokerId, "host1", 0, "rack-a")
      val followerNode = new Node(followerBrokerId, "host2", 1, "rack-b")
      val brokerList = Seq[Integer](leaderBrokerId, followerBrokerId).asJava
      val topicId = Uuid.randomUuid()
      val tp0 = new TopicPartition(topic, 0)
      val tidp0 = new TopicIdPartition(topicId, tp0)

      when(replicaManager.metadataCache.getPartitionReplicaEndpoints(
        tp0,
        new ListenerName("default")
      )).thenReturn(Map(
        leaderBrokerId -> leaderNode,
        followerBrokerId -> followerNode
      ).toMap)

      // Make this replica the leader and remove follower from ISR.
      val leaderAndIsrRequest = new LeaderAndIsrRequest.Builder(
        ApiKeys.LEADER_AND_ISR.latestVersion,
        0,
        0,
        brokerEpoch,
        Seq(new LeaderAndIsrPartitionState()
          .setTopicName(topic)
          .setPartitionIndex(0)
          .setControllerEpoch(0)
          .setLeader(leaderBrokerId)
          .setLeaderEpoch(1)
          .setIsr(Seq[Integer](leaderBrokerId).asJava)
          .setPartitionEpoch(0)
          .setReplicas(brokerList)
          .setIsNew(false)).asJava,
        Collections.singletonMap(topic, topicId),
        Set(leaderNode, followerNode).asJava).build()

      replicaManager.becomeLeaderOrFollower(2, leaderAndIsrRequest, (_, _) => ())

      appendRecords(replicaManager, tp0, TestUtils.singletonRecords(s"message".getBytes)).onFire { response =>
        assertEquals(Errors.NONE, response.error)
      }
      // Fetch as follower to initialise the log end offset of the replica
      fetchPartitionAsFollower(
        replicaManager,
        new TopicIdPartition(topicId, new TopicPartition(topic, 0)),
        new PartitionData(Uuid.ZERO_UUID, 0, 0, 100000, Optional.empty()),
        replicaId = 1
      )

      val metadata = new DefaultClientMetadata("rack-b", "client-id",
        InetAddress.getByName("localhost"), KafkaPrincipal.ANONYMOUS, "default")

      val consumerResult = fetchPartitionAsConsumer(
        replicaManager,
        tidp0,
        new PartitionData(Uuid.ZERO_UUID, 0, 0, 100000, Optional.empty()),
        clientMetadata = Some(metadata)
      )

      // Fetch from leader succeeds
      assertTrue(consumerResult.hasFired)

      // PartitionView passed to ReplicaSelector should not contain the follower as it's not in the ISR
      val expectedReplicaViews = Set(new DefaultReplicaView(leaderNode, 1, 0))
      val partitionView = replicaManager.replicaSelectorOpt.get
        .asInstanceOf[MockReplicaSelector].getPartitionViewArgument

      assertTrue(partitionView.isDefined)
      assertEquals(expectedReplicaViews.asJava, partitionView.get.replicas)
    } finally {
      replicaManager.shutdown()
    }
  }

  @Test
  def testFetchFromFollowerShouldNotRunPreferLeaderSelect(): Unit = {
    val replicaManager = setupReplicaManagerWithMockedPurgatories(new MockTimer(time),
      propsModifier = props => props.put(KafkaConfig.ReplicaSelectorClassProp, classOf[MockReplicaSelector].getName))
    try {
      val leaderBrokerId = 0
      val followerBrokerId = 1
      val brokerList = Seq[Integer](leaderBrokerId, followerBrokerId).asJava
      val topicId = Uuid.randomUuid()
      val tp0 = new TopicPartition(topic, 0)
      val tidp0 = new TopicIdPartition(topicId, tp0)

      // Make this replica the follower
      val leaderAndIsrRequest = new LeaderAndIsrRequest.Builder(ApiKeys.LEADER_AND_ISR.latestVersion, 0, 0, brokerEpoch,
        Seq(new LeaderAndIsrPartitionState()
          .setTopicName(topic)
          .setPartitionIndex(0)
          .setControllerEpoch(0)
          .setLeader(1)
          .setLeaderEpoch(1)
          .setIsr(brokerList)
          .setPartitionEpoch(0)
          .setReplicas(brokerList)
          .setIsNew(false)).asJava,
        Collections.singletonMap(topic, topicId),
        Set(new Node(0, "host1", 0), new Node(1, "host2", 1)).asJava).build()
      replicaManager.becomeLeaderOrFollower(1, leaderAndIsrRequest, (_, _) => ())

      val metadata = new DefaultClientMetadata("rack-a", "client-id",
        InetAddress.getLocalHost, KafkaPrincipal.ANONYMOUS, "default")

      val consumerResult = fetchPartitionAsConsumer(replicaManager, tidp0,
        new PartitionData(Uuid.ZERO_UUID, 0, 0, 100000,
          Optional.empty()), clientMetadata = Some(metadata))

      // Fetch from follower succeeds
      assertTrue(consumerResult.hasFired)

      // Expect not run the preferred read replica selection
      assertEquals(0, replicaManager.replicaSelectorOpt.get.asInstanceOf[MockReplicaSelector].getSelectionCount)

      // Only leader will compute preferred replica
      assertTrue(consumerResult.assertFired.preferredReadReplica.isEmpty)

    } finally replicaManager.shutdown(checkpointHW = false)
  }

  @Test
  def testFetchShouldReturnImmediatelyWhenPreferredReadReplicaIsDefined(): Unit = {
    val replicaManager = setupReplicaManagerWithMockedPurgatories(new MockTimer(time),
      propsModifier = props => props.put(KafkaConfig.ReplicaSelectorClassProp, "org.apache.kafka.common.replica.RackAwareReplicaSelector"))

    try {
      val leaderBrokerId = 0
      val followerBrokerId = 1
      val brokerList = Seq[Integer](leaderBrokerId, followerBrokerId).asJava
      val topicId = Uuid.randomUuid()
      val tp0 = new TopicPartition(topic, 0)
      val tidp0 = new TopicIdPartition(topicId, tp0)

      when(replicaManager.metadataCache.getPartitionReplicaEndpoints(
        tp0,
        new ListenerName("default")
      )).thenReturn(Map(
        leaderBrokerId -> new Node(leaderBrokerId, "host1", 9092, "rack-a"),
        followerBrokerId -> new Node(followerBrokerId, "host2", 9092, "rack-b")
      ).toMap)

      // Make this replica the leader
      val leaderEpoch = 1
      val leaderAndIsrRequest = new LeaderAndIsrRequest.Builder(ApiKeys.LEADER_AND_ISR.latestVersion, 0, 0, brokerEpoch,
        Seq(new LeaderAndIsrPartitionState()
          .setTopicName(topic)
          .setPartitionIndex(0)
          .setControllerEpoch(0)
          .setLeader(0)
          .setLeaderEpoch(leaderEpoch)
          .setIsr(brokerList)
          .setPartitionEpoch(0)
          .setReplicas(brokerList)
          .setIsNew(false)).asJava,
        Collections.singletonMap(topic, topicId),
        Set(new Node(0, "host1", 0), new Node(1, "host2", 1)).asJava).build()
      replicaManager.becomeLeaderOrFollower(1, leaderAndIsrRequest, (_, _) => ())

      // The leader must record the follower's fetch offset to make it eligible for follower fetch selection
      val followerFetchData = new PartitionData(topicId, 0L, 0L, Int.MaxValue, Optional.of(Int.box(leaderEpoch)), Optional.empty[Integer])
      fetchPartitionAsFollower(
        replicaManager,
        tidp0,
        followerFetchData,
        replicaId = followerBrokerId
      )

      val metadata = new DefaultClientMetadata("rack-b", "client-id",
        InetAddress.getLocalHost, KafkaPrincipal.ANONYMOUS, "default")

      // If a preferred read replica is selected, the fetch response returns immediately, even if min bytes and timeout conditions are not met.
      val consumerResult = fetchPartitionAsConsumer(replicaManager, tidp0,
        new PartitionData(topicId, 0, 0, 100000, Optional.empty()),
        minBytes = 1, clientMetadata = Some(metadata), maxWaitMs = 5000)

      // Fetch from leader succeeds
      assertTrue(consumerResult.hasFired)

      // No delayed fetch was inserted
      assertEquals(0, replicaManager.delayedFetchPurgatory.watched)

      // Returns a preferred replica
      assertTrue(consumerResult.assertFired.preferredReadReplica.isDefined)
    } finally replicaManager.shutdown(checkpointHW = false)
  }

  @Test
  def testFollowerFetchWithDefaultSelectorNoForcedHwPropagation(): Unit = {
    val topicPartition = 0
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
    val tidp0 = new TopicIdPartition(topicId, tp0)

    // Make this replica the follower
    val leaderAndIsrRequest2 = new LeaderAndIsrRequest.Builder(ApiKeys.LEADER_AND_ISR.latestVersion, 0, 0, brokerEpoch,
      Seq(new LeaderAndIsrPartitionState()
        .setTopicName(topic)
        .setPartitionIndex(0)
        .setControllerEpoch(0)
        .setLeader(0)
        .setLeaderEpoch(1)
        .setIsr(brokerList)
        .setPartitionEpoch(0)
        .setReplicas(brokerList)
        .setIsNew(false)).asJava,
      Collections.singletonMap(topic, topicId),
      Set(new Node(0, "host1", 0), new Node(1, "host2", 1)).asJava).build()
    replicaManager.becomeLeaderOrFollower(1, leaderAndIsrRequest2, (_, _) => ())

    val simpleRecords = Seq(new SimpleRecord("a".getBytes), new SimpleRecord("b".getBytes))
    val appendResult = appendRecords(replicaManager, tp0,
      MemoryRecords.withRecords(CompressionType.NONE, simpleRecords.toSeq: _*), AppendOrigin.CLIENT)

    // Increment the hw in the leader by fetching from the last offset
    val fetchOffset = simpleRecords.size
    var followerResult = fetchPartitionAsFollower(
      replicaManager,
      tidp0,
      new PartitionData(Uuid.ZERO_UUID, fetchOffset, 0, 100000, Optional.empty()),
      replicaId = 1,
      minBytes = 0
    )
    assertTrue(followerResult.hasFired)
    assertEquals(0, followerResult.assertFired.highWatermark)

    assertTrue(appendResult.hasFired, "Expected producer request to be acked")

    // Fetch from the same offset, no new data is expected and hence the fetch request should
    // go to the purgatory
    followerResult = fetchPartitionAsFollower(
      replicaManager,
      tidp0,
      new PartitionData(Uuid.ZERO_UUID, fetchOffset, 0, 100000, Optional.empty()),
      replicaId = 1,
      minBytes = 1000,
      maxWaitMs = 1000
    )
    assertFalse(followerResult.hasFired, "Request completed immediately unexpectedly")

    // Complete the request in the purgatory by advancing the clock
    timer.advanceClock(1001)
    assertTrue(followerResult.hasFired)

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

    try {
      val tp0 = new TopicPartition(topic, 0)
      val tidp0 = new TopicIdPartition(topicId, tp0)
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
          .setPartitionEpoch(0)
          .setReplicas(partition0Replicas)
          .setIsNew(true)).asJava,
        topicIds.asJava,
        Set(new Node(0, "host1", 0), new Node(1, "host2", 1)).asJava).build()
      replicaManager.becomeLeaderOrFollower(0, becomeFollowerRequest, (_, _) => ())

      // Fetch from follower, with non-empty ClientMetadata (FetchRequest v11+)
      val clientMetadata = new DefaultClientMetadata("", "", null, KafkaPrincipal.ANONYMOUS, "")
      var partitionData = new FetchRequest.PartitionData(Uuid.ZERO_UUID, 0L, 0L, 100,
        Optional.of(0))
      var fetchResult = fetchPartitionAsConsumer(replicaManager, tidp0, partitionData,
        clientMetadata = Some(clientMetadata))
      assertEquals(Errors.NONE, fetchResult.assertFired.error)

      // Fetch from follower, with empty ClientMetadata (which implies an older version)
      partitionData = new FetchRequest.PartitionData(Uuid.ZERO_UUID, 0L, 0L, 100,
        Optional.of(0))
      fetchResult = fetchPartitionAsConsumer(replicaManager, tidp0, partitionData)
      assertEquals(Errors.NOT_LEADER_OR_FOLLOWER, fetchResult.assertFired.error)
    } finally {
      replicaManager.shutdown()
    }

    TestUtils.assertNoNonDaemonThreads(this.getClass.getName)
  }

  @Test
  def testFetchRequestRateMetrics(): Unit = {
    val mockTimer = new MockTimer(time)
    val replicaManager = setupReplicaManagerWithMockedPurgatories(mockTimer, aliveBrokerIds = Seq(0, 1))

    val tp0 = new TopicPartition(topic, 0)
    val tidp0 = new TopicIdPartition(topicId, tp0)
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
        .setPartitionEpoch(0)
        .setReplicas(partition0Replicas)
        .setIsNew(true)).asJava,
      topicIds.asJava,
      Set(new Node(0, "host1", 0), new Node(1, "host2", 1)).asJava).build()
    replicaManager.becomeLeaderOrFollower(1, becomeLeaderRequest, (_, _) => ())

    def assertMetricCount(expected: Int): Unit = {
      assertEquals(expected, replicaManager.brokerTopicStats.allTopicsStats.totalFetchRequestRate.count)
      assertEquals(expected, replicaManager.brokerTopicStats.topicStats(topic).totalFetchRequestRate.count)
    }

    val partitionData = new FetchRequest.PartitionData(Uuid.ZERO_UUID, 0L, 0L, 100,
      Optional.empty())

    val nonPurgatoryFetchResult = fetchPartitionAsConsumer(replicaManager, tidp0, partitionData)
    assertEquals(Errors.NONE, nonPurgatoryFetchResult.assertFired.error)
    assertMetricCount(1)

    val purgatoryFetchResult = fetchPartitionAsConsumer(replicaManager, tidp0, partitionData, maxWaitMs = 10)
    assertFalse(purgatoryFetchResult.hasFired)
    mockTimer.advanceClock(11)
    assertEquals(Errors.NONE, purgatoryFetchResult.assertFired.error)
    assertMetricCount(2)
  }

  @Test
  def testBecomeFollowerWhileOldClientFetchInPurgatory(): Unit = {
    val mockTimer = new MockTimer(time)
    val replicaManager = setupReplicaManagerWithMockedPurgatories(mockTimer, aliveBrokerIds = Seq(0, 1))

    try {
      val tp0 = new TopicPartition(topic, 0)
      val tidp0 = new TopicIdPartition(topicId, tp0)
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
          .setPartitionEpoch(0)
          .setReplicas(partition0Replicas)
          .setIsNew(true)).asJava,
        topicIds.asJava,
        Set(new Node(0, "host1", 0), new Node(1, "host2", 1)).asJava).build()
      replicaManager.becomeLeaderOrFollower(1, becomeLeaderRequest, (_, _) => ())

      val partitionData = new FetchRequest.PartitionData(Uuid.ZERO_UUID, 0L, 0L, 100,
        Optional.empty())
      val fetchResult = fetchPartitionAsConsumer(replicaManager, tidp0, partitionData, maxWaitMs = 10)
      assertFalse(fetchResult.hasFired)

      // Become a follower and ensure that the delayed fetch returns immediately
      val becomeFollowerRequest = new LeaderAndIsrRequest.Builder(ApiKeys.LEADER_AND_ISR.latestVersion, 0, 0, brokerEpoch,
        Seq(new LeaderAndIsrPartitionState()
          .setTopicName(tp0.topic)
          .setPartitionIndex(tp0.partition)
          .setControllerEpoch(0)
          .setLeader(1)
          .setLeaderEpoch(2)
          .setIsr(partition0Replicas)
          .setPartitionEpoch(0)
          .setReplicas(partition0Replicas)
          .setIsNew(true)).asJava,
        topicIds.asJava,
        Set(new Node(0, "host1", 0), new Node(1, "host2", 1)).asJava).build()
      replicaManager.becomeLeaderOrFollower(0, becomeFollowerRequest, (_, _) => ())
      assertEquals(Errors.NOT_LEADER_OR_FOLLOWER, fetchResult.assertFired.error)
    } finally {
      replicaManager.shutdown()
    }

    TestUtils.assertNoNonDaemonThreads(this.getClass.getName)
  }

  @Test
  def testBecomeFollowerWhileNewClientFetchInPurgatory(): Unit = {
    val mockTimer = new MockTimer(time)
    val replicaManager = setupReplicaManagerWithMockedPurgatories(mockTimer, aliveBrokerIds = Seq(0, 1))

    try {
      val tp0 = new TopicPartition(topic, 0)
      val tidp0 = new TopicIdPartition(topicId, tp0)
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
          .setPartitionEpoch(0)
          .setReplicas(partition0Replicas)
          .setIsNew(true)).asJava,
        topicIds.asJava,
        Set(new Node(0, "host1", 0), new Node(1, "host2", 1)).asJava).build()
      replicaManager.becomeLeaderOrFollower(1, becomeLeaderRequest, (_, _) => ())

      val clientMetadata = new DefaultClientMetadata("", "", null, KafkaPrincipal.ANONYMOUS, "")
      val partitionData = new FetchRequest.PartitionData(Uuid.ZERO_UUID, 0L, 0L, 100,
        Optional.of(1))
      val fetchResult = fetchPartitionAsConsumer(
        replicaManager,
        tidp0,
        partitionData,
        clientMetadata = Some(clientMetadata),
        maxWaitMs = 10
      )
      assertFalse(fetchResult.hasFired)

      // Become a follower and ensure that the delayed fetch returns immediately
      val becomeFollowerRequest = new LeaderAndIsrRequest.Builder(ApiKeys.LEADER_AND_ISR.latestVersion, 0, 0, brokerEpoch,
        Seq(new LeaderAndIsrPartitionState()
          .setTopicName(tp0.topic)
          .setPartitionIndex(tp0.partition)
          .setControllerEpoch(0)
          .setLeader(1)
          .setLeaderEpoch(2)
          .setIsr(partition0Replicas)
          .setPartitionEpoch(0)
          .setReplicas(partition0Replicas)
          .setIsNew(true)).asJava,
        topicIds.asJava,
        Set(new Node(0, "host1", 0), new Node(1, "host2", 1)).asJava).build()
      replicaManager.becomeLeaderOrFollower(0, becomeFollowerRequest, (_, _) => ())
      assertEquals(Errors.FENCED_LEADER_EPOCH, fetchResult.assertFired.error)
    } finally {
      replicaManager.shutdown()
    }

    TestUtils.assertNoNonDaemonThreads(this.getClass.getName)
  }

  @Test
  def testFetchFromLeaderAlwaysAllowed(): Unit = {
    val replicaManager = setupReplicaManagerWithMockedPurgatories(new MockTimer(time), aliveBrokerIds = Seq(0, 1))

    val tp0 = new TopicPartition(topic, 0)
    val tidp0 = new TopicIdPartition(topicId, tp0)
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
        .setPartitionEpoch(0)
        .setReplicas(partition0Replicas)
        .setIsNew(true)).asJava,
      topicIds.asJava,
      Set(new Node(0, "host1", 0), new Node(1, "host2", 1)).asJava).build()
    replicaManager.becomeLeaderOrFollower(1, becomeLeaderRequest, (_, _) => ())

    val clientMetadata = new DefaultClientMetadata("", "", null, KafkaPrincipal.ANONYMOUS, "")
    var partitionData = new FetchRequest.PartitionData(Uuid.ZERO_UUID, 0L, 0L, 100,
      Optional.of(1))
    var fetchResult = fetchPartitionAsConsumer(replicaManager, tidp0, partitionData, clientMetadata = Some(clientMetadata))
    assertEquals(Errors.NONE, fetchResult.assertFired.error)

    partitionData = new FetchRequest.PartitionData(Uuid.ZERO_UUID, 0L, 0L, 100,
      Optional.empty())
    fetchResult = fetchPartitionAsConsumer(replicaManager, tidp0, partitionData, clientMetadata = Some(clientMetadata))
    assertEquals(Errors.NONE, fetchResult.assertFired.error)
  }

  @Test
  def testClearFetchPurgatoryOnStopReplica(): Unit = {
    // As part of a reassignment, we may send StopReplica to the old leader.
    // In this case, we should ensure that pending purgatory operations are cancelled
    // immediately rather than sitting around to timeout.

    val mockTimer = new MockTimer(time)
    val replicaManager = setupReplicaManagerWithMockedPurgatories(mockTimer, aliveBrokerIds = Seq(0, 1))

    val tp0 = new TopicPartition(topic, 0)
    val tidp0 = new TopicIdPartition(topicId, tp0)
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
        .setPartitionEpoch(0)
        .setReplicas(partition0Replicas)
        .setIsNew(true)).asJava,
      topicIds.asJava,
      Set(new Node(0, "host1", 0), new Node(1, "host2", 1)).asJava).build()
    replicaManager.becomeLeaderOrFollower(1, becomeLeaderRequest, (_, _) => ())

    val partitionData = new FetchRequest.PartitionData(Uuid.ZERO_UUID, 0L, 0L, 100,
      Optional.of(1))
    val fetchResult = fetchPartitionAsConsumer(replicaManager, tidp0, partitionData, maxWaitMs = 10)
    assertFalse(fetchResult.hasFired)
    when(replicaManager.metadataCache.contains(tp0)).thenReturn(true)

    // We have a fetch in purgatory, now receive a stop replica request and
    // assert that the fetch returns with a NOT_LEADER error
    replicaManager.stopReplicas(2, 0, 0,
      mutable.Map(tp0 -> new StopReplicaPartitionState()
        .setPartitionIndex(tp0.partition)
        .setDeletePartition(true)
        .setLeaderEpoch(LeaderAndIsr.EpochDuringDelete)))

    assertEquals(Errors.NOT_LEADER_OR_FOLLOWER, fetchResult.assertFired.error)
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
        .setPartitionEpoch(0)
        .setReplicas(partition0Replicas)
        .setIsNew(true)).asJava,
      topicIds.asJava,
      Set(new Node(0, "host1", 0), new Node(1, "host2", 1)).asJava).build()
    replicaManager.becomeLeaderOrFollower(1, becomeLeaderRequest, (_, _) => ())

    val produceResult = sendProducerAppend(replicaManager, tp0, 3)
    assertNull(produceResult.get)

    when(replicaManager.metadataCache.contains(tp0)).thenReturn(true)

    replicaManager.stopReplicas(2, 0, 0,
      mutable.Map(tp0 -> new StopReplicaPartitionState()
        .setPartitionIndex(tp0.partition)
        .setDeletePartition(true)
        .setLeaderEpoch(LeaderAndIsr.EpochDuringDelete)))

    assertNotNull(produceResult.get)
    assertEquals(Errors.NOT_LEADER_OR_FOLLOWER, produceResult.get.error)
  }

  private def sendProducerAppend(
    replicaManager: ReplicaManager,
    topicPartition: TopicPartition,
    numOfRecords: Int
  ): AtomicReference[PartitionResponse] = {
    val produceResult = new AtomicReference[PartitionResponse]()
    def callback(response: Map[TopicPartition, PartitionResponse]): Unit = {
      produceResult.set(response(topicPartition))
    }

    val records = MemoryRecords.withRecords(
      CompressionType.NONE,
      IntStream
        .range(0, numOfRecords)
        .mapToObj(i => new SimpleRecord(i.toString.getBytes))
        .toArray(Array.ofDim[SimpleRecord]): _*
    )

    replicaManager.appendRecords(
      timeout = 10,
      requiredAcks = -1,
      internalTopicsAllowed = false,
      origin = AppendOrigin.CLIENT,
      entriesPerPartition = Map(topicPartition -> records),
      responseCallback = callback
    )
    produceResult
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
                                                 topicId: Option[Uuid] = None): (ReplicaManager, LogManager) = {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect)
    props.put("log.dir", TestUtils.tempRelativeDir("data").getAbsolutePath)
    props.asScala ++= extraProps.asScala
    val config = KafkaConfig.fromProps(props)
    val logConfig = LogConfig()
    val logDir = new File(new File(config.logDirs.head), s"$topic-$topicPartition")
    Files.createDirectories(logDir.toPath)
    val mockScheduler = new MockScheduler(time)
    val mockBrokerTopicStats = new BrokerTopicStats
    val mockLogDirFailureChannel = new LogDirFailureChannel(config.logDirs.size)
    val tp = new TopicPartition(topic, topicPartition)
    val maxTransactionTimeoutMs = 30000
    val maxProducerIdExpirationMs = 30000
    val segments = new LogSegments(tp)
    val leaderEpochCache = UnifiedLog.maybeCreateLeaderEpochCache(logDir, tp, mockLogDirFailureChannel, logConfig.recordVersion, "")
    val producerStateManager = new ProducerStateManager(tp, logDir,
      maxTransactionTimeoutMs, new ProducerStateManagerConfig(maxProducerIdExpirationMs), time)
    val offsets = new LogLoader(
      logDir,
      tp,
      logConfig,
      mockScheduler,
      time,
      mockLogDirFailureChannel,
      hadCleanShutdown = true,
      segments,
      0L,
      0L,
      leaderEpochCache,
      producerStateManager
    ).load()
    val localLog = new LocalLog(logDir, logConfig, segments, offsets.recoveryPoint,
      offsets.nextOffsetMetadata, mockScheduler, time, tp, mockLogDirFailureChannel)
    val mockLog = new UnifiedLog(
      logStartOffset = offsets.logStartOffset,
      localLog = localLog,
      brokerTopicStats = mockBrokerTopicStats,
      producerIdExpirationCheckIntervalMs = 30000,
      leaderEpochCache = leaderEpochCache,
      producerStateManager = producerStateManager,
      _topicId = topicId,
      keepPartitionMetadataFile = true) {

      override def endOffsetForEpoch(leaderEpoch: Int): Option[OffsetAndEpoch] = {
        assertEquals(leaderEpoch, leaderEpochFromLeader)
        localLogOffset.map { logOffset =>
          Some(OffsetAndEpoch(logOffset, leaderEpochFromLeader))
        }.getOrElse(super.endOffsetForEpoch(leaderEpoch))
      }

      override def latestEpoch: Option[Int] = Some(leaderEpochFromLeader)

      override def logEndOffsetMetadata: LogOffsetMetadata =
        localLogOffset.map(new LogOffsetMetadata(_)).getOrElse(super.logEndOffsetMetadata)

      override def logEndOffset: Long = localLogOffset.getOrElse(super.logEndOffset)
    }

    // Expect to call LogManager.truncateTo exactly once
    val topicPartitionObj = new TopicPartition(topic, topicPartition)
    val mockLogMgr: LogManager = mock(classOf[LogManager])
    when(mockLogMgr.liveLogDirs).thenReturn(config.logDirs.map(new File(_).getAbsoluteFile))
    when(mockLogMgr.getOrCreateLog(ArgumentMatchers.eq(topicPartitionObj), ArgumentMatchers.eq(false), ArgumentMatchers.eq(false), any())).thenReturn(mockLog)
    when(mockLogMgr.getLog(topicPartitionObj, isFuture = false)).thenReturn(Some(mockLog))
    when(mockLogMgr.getLog(topicPartitionObj, isFuture = true)).thenReturn(None)
    val allLogs = new Pool[TopicPartition, UnifiedLog]()
    allLogs.put(topicPartitionObj, mockLog)
    when(mockLogMgr.allLogs).thenReturn(allLogs.values)
    when(mockLogMgr.isLogDirOnline(anyString)).thenReturn(true)

    val aliveBrokerIds = Seq[Integer](followerBrokerId, leaderBrokerId)
    val aliveBrokers = aliveBrokerIds.map(brokerId => new Node(brokerId, s"host$brokerId", brokerId))

    val metadataCache: MetadataCache = mock(classOf[MetadataCache])
    mockGetAliveBrokerFunctions(metadataCache, aliveBrokers)
    when(metadataCache.getPartitionReplicaEndpoints(
      any[TopicPartition], any[ListenerName])).
        thenReturn(Map(leaderBrokerId -> new Node(leaderBrokerId, "host1", 9092, "rack-a"),
          followerBrokerId -> new Node(followerBrokerId, "host2", 9092, "rack-b")).toMap)
    when(metadataCache.metadataVersion()).thenReturn(config.interBrokerProtocolVersion)
    val mockProducePurgatory = new DelayedOperationPurgatory[DelayedProduce](
      purgatoryName = "Produce", timer, reaperEnabled = false)
    val mockFetchPurgatory = new DelayedOperationPurgatory[DelayedFetch](
      purgatoryName = "Fetch", timer, reaperEnabled = false)
    val mockDeleteRecordsPurgatory = new DelayedOperationPurgatory[DelayedDeleteRecords](
      purgatoryName = "DeleteRecords", timer, reaperEnabled = false)
    val mockElectLeaderPurgatory = new DelayedOperationPurgatory[DelayedElectLeader](
      purgatoryName = "ElectLeader", timer, reaperEnabled = false)

    // Mock network client to show leader offset of 5
    val blockingSend = new MockBlockingSender(
      Map(topicPartitionObj -> new EpochEndOffset()
        .setPartition(topicPartitionObj.partition)
        .setErrorCode(Errors.NONE.code)
        .setLeaderEpoch(leaderEpochFromLeader)
        .setEndOffset(offsetFromLeader)).asJava,
      BrokerEndPoint(1, "host1" ,1), time)
    val replicaManager = new ReplicaManager(
      metrics = metrics,
      config = config,
      time = time,
      scheduler = mockScheduler,
      logManager = mockLogMgr,
      quotaManagers = quotaManager,
      brokerTopicStats = mockBrokerTopicStats,
      metadataCache = metadataCache,
      logDirFailureChannel = mockLogDirFailureChannel,
      alterPartitionManager = alterPartitionManager,
      delayedProducePurgatoryParam = Some(mockProducePurgatory),
      delayedFetchPurgatoryParam = Some(mockFetchPurgatory),
      delayedDeleteRecordsPurgatoryParam = Some(mockDeleteRecordsPurgatory),
      delayedElectLeaderPurgatoryParam = Some(mockElectLeaderPurgatory),
      threadNamePrefix = Option(this.getClass.getName)) {

      override protected def createReplicaFetcherManager(metrics: Metrics,
                                                         time: Time,
                                                         threadNamePrefix: Option[String],
                                                         replicationQuotaManager: ReplicationQuotaManager): ReplicaFetcherManager = {
        new ReplicaFetcherManager(config, this, metrics, time, threadNamePrefix, replicationQuotaManager, () => metadataCache.metadataVersion()) {

          override def createFetcherThread(fetcherId: Int, sourceBroker: BrokerEndPoint): ReplicaFetcherThread = {
            val logContext = new LogContext(s"[ReplicaFetcher replicaId=${config.brokerId}, leaderId=${sourceBroker.id}, " +
              s"fetcherId=$fetcherId] ")
            val fetchSessionHandler = new FetchSessionHandler(logContext, sourceBroker.id)
            val leader = new RemoteLeaderEndPoint(logContext.logPrefix, blockingSend, fetchSessionHandler, config,
              replicaManager, quotaManager.follower, () => config.interBrokerProtocolVersion)
            new ReplicaFetcherThread(s"ReplicaFetcherThread-$fetcherId", leader, config, failedPartitions, replicaManager,
              quotaManager.follower, logContext.logPrefix, () => config.interBrokerProtocolVersion) {
              override def doWork(): Unit = {
                // In case the thread starts before the partition is added by AbstractFetcherManager,
                // add it here (it's a no-op if already added)
                val initialOffset = InitialFetchState(
                  topicId = topicId,
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
      .setPartitionEpoch(zkVersion)
      .setReplicas(aliveBrokerIds.asJava)
      .setIsNew(isNew)
  }

  private class CallbackResult[T] {
    private var value: Option[T] = None
    private var fun: Option[T => Unit] = None

    def assertFired: T = {
      assertTrue(hasFired, "Callback has not been fired")
      value.get
    }

    def hasFired: Boolean = {
      value.isDefined
    }

    def fire(value: T): Unit = {
      this.value = Some(value)
      fun.foreach(f => f(value))
    }

    def onFire(fun: T => Unit): CallbackResult[T] = {
      this.fun = Some(fun)
      if (this.hasFired) fire(value.get)
      this
    }
  }

  private def appendRecords(replicaManager: ReplicaManager,
                            partition: TopicPartition,
                            records: MemoryRecords,
                            origin: AppendOrigin = AppendOrigin.CLIENT,
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

  private def fetchPartitionAsConsumer(
    replicaManager: ReplicaManager,
    partition: TopicIdPartition,
    partitionData: PartitionData,
    maxWaitMs: Long = 0,
    minBytes: Int = 1,
    maxBytes: Int = 1024 * 1024,
    isolationLevel: IsolationLevel = IsolationLevel.READ_UNCOMMITTED,
    clientMetadata: Option[ClientMetadata] = None,
  ): CallbackResult[FetchPartitionData] = {
    val isolation = isolationLevel match {
      case IsolationLevel.READ_COMMITTED => FetchTxnCommitted
      case IsolationLevel.READ_UNCOMMITTED => FetchHighWatermark
    }

    fetchPartition(
      replicaManager,
      replicaId = Request.OrdinaryConsumerId,
      partition,
      partitionData,
      minBytes,
      maxBytes,
      isolation,
      clientMetadata,
      maxWaitMs
    )
  }

  private def fetchPartitionAsFollower(
    replicaManager: ReplicaManager,
    partition: TopicIdPartition,
    partitionData: PartitionData,
    replicaId: Int,
    maxWaitMs: Long = 0,
    minBytes: Int = 1,
    maxBytes: Int = 1024 * 1024,
  ): CallbackResult[FetchPartitionData] = {
    fetchPartition(
      replicaManager,
      replicaId = replicaId,
      partition,
      partitionData,
      minBytes = minBytes,
      maxBytes = maxBytes,
      isolation = FetchLogEnd,
      clientMetadata = None,
      maxWaitMs = maxWaitMs
    )
  }

  private def fetchPartition(
    replicaManager: ReplicaManager,
    replicaId: Int,
    partition: TopicIdPartition,
    partitionData: PartitionData,
    minBytes: Int,
    maxBytes: Int,
    isolation: FetchIsolation,
    clientMetadata: Option[ClientMetadata],
    maxWaitMs: Long
  ): CallbackResult[FetchPartitionData] = {
    val result = new CallbackResult[FetchPartitionData]()
    def fetchCallback(responseStatus: Seq[(TopicIdPartition, FetchPartitionData)]): Unit = {
      assertEquals(1, responseStatus.size)
      val (topicPartition, fetchData) = responseStatus.head
      assertEquals(partition, topicPartition)
      result.fire(fetchData)
    }

    fetchPartitions(
      replicaManager,
      replicaId = replicaId,
      fetchInfos = Seq(partition -> partitionData),
      responseCallback = fetchCallback,
      maxWaitMs = maxWaitMs,
      minBytes = minBytes,
      maxBytes = maxBytes,
      isolation = isolation,
      clientMetadata = clientMetadata
    )

    result
  }

  private def fetchPartitions(
    replicaManager: ReplicaManager,
    replicaId: Int,
    fetchInfos: Seq[(TopicIdPartition, PartitionData)],
    responseCallback: Seq[(TopicIdPartition, FetchPartitionData)] => Unit,
    requestVersion: Short = ApiKeys.FETCH.latestVersion,
    maxWaitMs: Long = 0,
    minBytes: Int = 1,
    maxBytes: Int = 1024 * 1024,
    quota: ReplicaQuota = UnboundedQuota,
    isolation: FetchIsolation = FetchLogEnd,
    clientMetadata: Option[ClientMetadata] = None
  ): Unit = {
    val params = FetchParams(
      requestVersion = requestVersion,
      replicaId = replicaId,
      maxWaitMs = maxWaitMs,
      minBytes = minBytes,
      maxBytes = maxBytes,
      isolation = isolation,
      clientMetadata = clientMetadata
    )

    replicaManager.fetchMessages(
      params,
      fetchInfos,
      quota,
      responseCallback
    )
  }

  private def setupReplicaManagerWithMockedPurgatories(
    timer: MockTimer,
    brokerId: Int = 0,
    aliveBrokerIds: Seq[Int] = Seq(0, 1),
    propsModifier: Properties => Unit = _ => {},
    mockReplicaFetcherManager: Option[ReplicaFetcherManager] = None,
    mockReplicaAlterLogDirsManager: Option[ReplicaAlterLogDirsManager] = None,
    isShuttingDown: AtomicBoolean = new AtomicBoolean(false)
  ): ReplicaManager = {
    val props = TestUtils.createBrokerConfig(brokerId, TestUtils.MockZkConnect)
    props.put("log.dirs", TestUtils.tempRelativeDir("data").getAbsolutePath + "," + TestUtils.tempRelativeDir("data2").getAbsolutePath)
    propsModifier.apply(props)
    val config = KafkaConfig.fromProps(props)
    val logProps = new Properties()
    val mockLogMgr = TestUtils.createLogManager(config.logDirs.map(new File(_)), LogConfig(logProps))
    val aliveBrokers = aliveBrokerIds.map(brokerId => new Node(brokerId, s"host$brokerId", brokerId))

    val metadataCache: MetadataCache = mock(classOf[MetadataCache])
    when(metadataCache.topicIdInfo()).thenReturn((topicIds.asJava, topicNames.asJava))
    when(metadataCache.topicNamesToIds()).thenReturn(topicIds.asJava)
    when(metadataCache.topicIdsToNames()).thenReturn(topicNames.asJava)
    when(metadataCache.metadataVersion()).thenReturn(config.interBrokerProtocolVersion)
    mockGetAliveBrokerFunctions(metadataCache, aliveBrokers)
    val mockProducePurgatory = new DelayedOperationPurgatory[DelayedProduce](
      purgatoryName = "Produce", timer, reaperEnabled = false)
    val mockFetchPurgatory = new DelayedOperationPurgatory[DelayedFetch](
      purgatoryName = "Fetch", timer, reaperEnabled = false)
    val mockDeleteRecordsPurgatory = new DelayedOperationPurgatory[DelayedDeleteRecords](
      purgatoryName = "DeleteRecords", timer, reaperEnabled = false)
    val mockDelayedElectLeaderPurgatory = new DelayedOperationPurgatory[DelayedElectLeader](
      purgatoryName = "DelayedElectLeader", timer, reaperEnabled = false)

    new ReplicaManager(
      metrics = metrics,
      config = config,
      time = time,
      scheduler = scheduler,
      logManager = mockLogMgr,
      quotaManagers = quotaManager,
      metadataCache = metadataCache,
      logDirFailureChannel = new LogDirFailureChannel(config.logDirs.size),
      alterPartitionManager = alterPartitionManager,
      isShuttingDown = isShuttingDown,
      delayedProducePurgatoryParam = Some(mockProducePurgatory),
      delayedFetchPurgatoryParam = Some(mockFetchPurgatory),
      delayedDeleteRecordsPurgatoryParam = Some(mockDeleteRecordsPurgatory),
      delayedElectLeaderPurgatoryParam = Some(mockDelayedElectLeaderPurgatory),
      threadNamePrefix = Option(this.getClass.getName)) {

      override protected def createReplicaFetcherManager(
        metrics: Metrics,
        time: Time,
        threadNamePrefix: Option[String],
        quotaManager: ReplicationQuotaManager
      ): ReplicaFetcherManager = {
        mockReplicaFetcherManager.getOrElse {
          super.createReplicaFetcherManager(
            metrics,
            time,
            threadNamePrefix,
            quotaManager
          )
        }
      }

      override def createReplicaAlterLogDirsManager(
        quotaManager: ReplicationQuotaManager,
        brokerTopicStats: BrokerTopicStats
      ): ReplicaAlterLogDirsManager = {
        mockReplicaAlterLogDirsManager.getOrElse {
          super.createReplicaAlterLogDirsManager(
            quotaManager,
            brokerTopicStats
          )
        }
      }
    }
  }

  @Test
  def testOldLeaderLosesMetricsWhenReassignPartitions(): Unit = {
    val controllerEpoch = 0
    val leaderEpoch = 0
    val leaderEpochIncrement = 1
    val correlationId = 0
    val controllerId = 0
    val mockTopicStats1: BrokerTopicStats = mock(classOf[BrokerTopicStats])
    val (rm0, rm1) = prepareDifferentReplicaManagers(mock(classOf[BrokerTopicStats]), mockTopicStats1)

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
            .setPartitionEpoch(0)
            .setReplicas(partition0Replicas)
            .setIsNew(true),
          new LeaderAndIsrPartitionState()
            .setTopicName(tp1.topic)
            .setPartitionIndex(tp1.partition)
            .setControllerEpoch(controllerEpoch)
            .setLeader(1)
            .setLeaderEpoch(leaderEpoch)
            .setIsr(partition1Replicas)
            .setPartitionEpoch(0)
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
            .setPartitionEpoch(0)
            .setReplicas(partition0Replicas)
            .setIsNew(true),
          new LeaderAndIsrPartitionState()
            .setTopicName(tp1.topic)
            .setPartitionIndex(tp1.partition)
            .setControllerEpoch(controllerEpoch)
            .setLeader(0)
            .setLeaderEpoch(leaderEpoch + leaderEpochIncrement)
            .setIsr(partition1Replicas)
            .setPartitionEpoch(0)
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
    verify(mockTopicStats1).removeOldLeaderMetrics(topic)
  }

  @Test
  def testOldFollowerLosesMetricsWhenReassignPartitions(): Unit = {
    val controllerEpoch = 0
    val leaderEpoch = 0
    val leaderEpochIncrement = 1
    val correlationId = 0
    val controllerId = 0
    val mockTopicStats1: BrokerTopicStats = mock(classOf[BrokerTopicStats])
    val (rm0, rm1) = prepareDifferentReplicaManagers(mock(classOf[BrokerTopicStats]), mockTopicStats1)

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
            .setPartitionEpoch(0)
            .setReplicas(partition0Replicas)
            .setIsNew(true),
          new LeaderAndIsrPartitionState()
            .setTopicName(tp1.topic)
            .setPartitionIndex(tp1.partition)
            .setControllerEpoch(controllerEpoch)
            .setLeader(1)
            .setLeaderEpoch(leaderEpoch)
            .setIsr(partition1Replicas)
            .setPartitionEpoch(0)
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
            .setPartitionEpoch(0)
            .setReplicas(partition0Replicas)
            .setIsNew(true),
          new LeaderAndIsrPartitionState()
            .setTopicName(tp1.topic)
            .setPartitionIndex(tp1.partition)
            .setControllerEpoch(controllerEpoch)
            .setLeader(0)
            .setLeaderEpoch(leaderEpoch + leaderEpochIncrement)
            .setIsr(partition1Replicas)
            .setPartitionEpoch(0)
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
    verify(mockTopicStats1).removeOldLeaderMetrics(topic)
    verify(mockTopicStats1).removeOldFollowerMetrics(topic)
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

    val metadataCache0: MetadataCache = mock(classOf[MetadataCache])
    val metadataCache1: MetadataCache = mock(classOf[MetadataCache])
    val aliveBrokers = Seq(new Node(0, "host0", 0), new Node(1, "host1", 1))
    mockGetAliveBrokerFunctions(metadataCache0, aliveBrokers)
    mockGetAliveBrokerFunctions(metadataCache1, aliveBrokers)
    when(metadataCache0.metadataVersion()).thenReturn(config0.interBrokerProtocolVersion)
    when(metadataCache1.metadataVersion()).thenReturn(config1.interBrokerProtocolVersion)

    // each replica manager is for a broker
    val rm0 = new ReplicaManager(
      metrics = metrics,
      config = config0,
      time = time,
      scheduler = new MockScheduler(time),
      logManager = mockLogMgr0,
      quotaManagers = quotaManager,
      brokerTopicStats = brokerTopicStats1,
      metadataCache = metadataCache0,
      logDirFailureChannel = new LogDirFailureChannel(config0.logDirs.size),
      alterPartitionManager = alterPartitionManager)
    val rm1 = new ReplicaManager(
      metrics = metrics,
      config = config1,
      time = time,
      scheduler = new MockScheduler(time),
      logManager = mockLogMgr1,
      quotaManagers = quotaManager,
      brokerTopicStats = brokerTopicStats2,
      metadataCache = metadataCache1,
      logDirFailureChannel = new LogDirFailureChannel(config1.logDirs.size),
      alterPartitionManager = alterPartitionManager)

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

    val (_, error) = replicaManager.stopReplicas(1, 0, 0, partitionStates)
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

    val (result, error) = replicaManager.stopReplicas(1, 0, 0, partitionStates)
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
      Files.createFile(dir.toPath)
    }

    val partitionStates = Map(tp0 -> new StopReplicaPartitionState()
      .setPartitionIndex(tp0.partition)
      .setLeaderEpoch(1)
      .setDeletePartition(deletePartitions)
    )

    val (result, error) = replicaManager.stopReplicas(1, 0, 0, partitionStates)
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
    partition.appendRecordsToLeader(batch, AppendOrigin.CLIENT, requiredAcks = 0, RequestLocal.withThreadConfinedCaching)
    partition.log.get.updateHighWatermark(2L)
    partition.log.get.maybeIncrementLogStartOffset(1L, LeaderOffsetIncremented)
    replicaManager.logManager.checkpointLogRecoveryOffsets()
    replicaManager.logManager.checkpointLogStartOffsets()
    assertEquals(Some(1L), readRecoveryPointCheckpoint().get(tp0))
    assertEquals(Some(1L), readLogStartOffsetCheckpoint().get(tp0))

    if (throwIOException) {
      // Replace underlying PartitionMetadataFile with a mock which throws
      // a KafkaStorageException when maybeFlush is called.
      val mockPartitionMetadataFile = mock(classOf[PartitionMetadataFile])
      when(mockPartitionMetadataFile.maybeFlush()).thenThrow(new KafkaStorageException())
      partition.log.get.partitionMetadataFile = Some(mockPartitionMetadataFile)
    }

    val partitionStates = Map(tp0 -> new StopReplicaPartitionState()
      .setPartitionIndex(tp0.partition)
      .setLeaderEpoch(leaderEpoch)
      .setDeletePartition(deletePartition)
    )

    val (result, error) = replicaManager.stopReplicas(1, 0, 0, partitionStates)
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
      new ReplicaManager(
        metrics = metrics,
        config = config,
        time = time,
        scheduler = new MockScheduler(time),
        logManager = mockLogMgr,
        quotaManagers = quotaManager,
        metadataCache = MetadataCache.zkMetadataCache(config.brokerId, config.interBrokerProtocolVersion),
        logDirFailureChannel = new LogDirFailureChannel(config.logDirs.size),
        alterPartitionManager = alterPartitionManager) {
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
            .setPartitionEpoch(0)
            .setReplicas(brokerList)
            .setIsNew(true)).asJava,
          topicIds,
          Set(new Node(0, "host1", 0), new Node(1, "host2", 1)).asJava).build()

      val response = replicaManager.becomeLeaderOrFollower(0, leaderAndIsrRequest(0, topicIds), (_, _) => ())
      assertEquals(Errors.NONE, response.partitionErrors(topicNames).get(topicPartition))
      assertFalse(replicaManager.localLog(topicPartition).isEmpty)
      val id = topicIds.get(topicPartition.topic())
      val log = replicaManager.localLog(topicPartition).get
      assertTrue(log.partitionMetadataFile.get.exists())
      val partitionMetadata = log.partitionMetadataFile.get.read()

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
      assertFalse(log.partitionMetadataFile.get.exists())

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
          .setPartitionEpoch(0)
          .setReplicas(brokerList)
          .setIsNew(true)).asJava,
        topicIds,
        Set(new Node(0, "host1", 0), new Node(1, "host2", 1)).asJava).build()

      val response = replicaManager.becomeLeaderOrFollower(0, leaderAndIsrRequest(0), (_, _) => ())
      assertEquals(Errors.NONE, response.partitionErrors(topicNames).get(topicPartition))
      assertFalse(replicaManager.localLog(topicPartition).isEmpty)
      val id = topicIds.get(topicPartition.topic())
      log = replicaManager.localLog(topicPartition).get
      assertTrue(log.partitionMetadataFile.get.exists())
      val partitionMetadata = log.partitionMetadataFile.get.read()

      // Current version of PartitionMetadataFile is 0.
      assertEquals(0, partitionMetadata.version)
      assertEquals(id, partitionMetadata.topicId)
    } finally replicaManager.shutdown(checkpointHW = false)
  }

  @Test
  def testPartitionMetadataFileCreatedAfterPreviousRequestWithoutIds(): Unit = {
    val replicaManager = setupReplicaManagerWithMockedPurgatories(new MockTimer(time))
    try {
      val brokerList = Seq[Integer](0, 1).asJava
      val topicPartition = new TopicPartition(topic, 0)
      val topicPartition2 = new TopicPartition(topic, 1)

      def leaderAndIsrRequest(topicIds: util.Map[String, Uuid], version: Short, partition: Int = 0, leaderEpoch: Int = 0): LeaderAndIsrRequest =
        new LeaderAndIsrRequest.Builder(version, 0, 0, brokerEpoch,
        Seq(new LeaderAndIsrPartitionState()
          .setTopicName(topic)
          .setPartitionIndex(partition)
          .setControllerEpoch(0)
          .setLeader(0)
          .setLeaderEpoch(leaderEpoch)
          .setIsr(brokerList)
          .setPartitionEpoch(0)
          .setReplicas(brokerList)
          .setIsNew(true)).asJava,
        topicIds,
        Set(new Node(0, "host1", 0), new Node(1, "host2", 1)).asJava).build()

      // Send a request without a topic ID so that we have a log without a topic ID associated to the partition.
      val response = replicaManager.becomeLeaderOrFollower(0, leaderAndIsrRequest(Collections.emptyMap(), 4), (_, _) => ())
      assertEquals(Errors.NONE, response.partitionErrors(Collections.emptyMap()).get(topicPartition))
      assertTrue(replicaManager.localLog(topicPartition).isDefined)
      val log = replicaManager.localLog(topicPartition).get
      assertFalse(log.partitionMetadataFile.get.exists())
      assertTrue(log.topicId.isEmpty)

      val response2 = replicaManager.becomeLeaderOrFollower(0, leaderAndIsrRequest(topicIds.asJava, ApiKeys.LEADER_AND_ISR.latestVersion), (_, _) => ())
      assertEquals(Errors.NONE, response2.partitionErrors(topicNames.asJava).get(topicPartition))
      assertTrue(replicaManager.localLog(topicPartition).isDefined)
      assertTrue(log.partitionMetadataFile.get.exists())
      assertTrue(log.topicId.isDefined)
      assertEquals(topicId, log.topicId.get)

      // Repeat with partition 2, but in this case, update the leader epoch
      // Send a request without a topic ID so that we have a log without a topic ID associated to the partition.
      val response3 = replicaManager.becomeLeaderOrFollower(0, leaderAndIsrRequest(Collections.emptyMap(), 4, 1), (_, _) => ())
      assertEquals(Errors.NONE, response3.partitionErrors(Collections.emptyMap()).get(topicPartition2))
      assertTrue(replicaManager.localLog(topicPartition2).isDefined)
      val log2 = replicaManager.localLog(topicPartition2).get
      assertFalse(log2.partitionMetadataFile.get.exists())
      assertTrue(log2.topicId.isEmpty)

      val response4 = replicaManager.becomeLeaderOrFollower(0, leaderAndIsrRequest(topicIds.asJava, ApiKeys.LEADER_AND_ISR.latestVersion, 1, 1), (_, _) => ())
      assertEquals(Errors.NONE, response4.partitionErrors(topicNames.asJava).get(topicPartition2))
      assertTrue(replicaManager.localLog(topicPartition2).isDefined)
      assertTrue(log2.partitionMetadataFile.get.exists())
      assertTrue(log2.topicId.isDefined)
      assertEquals(topicId, log2.topicId.get)

      assertEquals(topicId, log.partitionMetadataFile.get.read().topicId)
      assertEquals(topicId, log2.partitionMetadataFile.get.read().topicId)
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
          .setPartitionEpoch(0)
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
          .setPartitionEpoch(0)
          .setReplicas(brokerList)
          .setIsNew(true)).asJava,
        topicIds,
        Set(new Node(0, "host1", 0), new Node(1, "host2", 1)).asJava).build().serialize(), version)

      // There is no file if the topic does not have an associated topic ID.
      val response = replicaManager.becomeLeaderOrFollower(0, leaderAndIsrRequest(0, "fakeTopic", ApiKeys.LEADER_AND_ISR.latestVersion), (_, _) => ())
      assertTrue(replicaManager.localLog(topicPartitionFake).isDefined)
      val log = replicaManager.localLog(topicPartitionFake).get
      assertFalse(log.partitionMetadataFile.get.exists())
      assertEquals(Errors.NONE, response.partitionErrors(topicNames).get(topicPartition))

      // There is no file if the topic has the default UUID.
      val response2 = replicaManager.becomeLeaderOrFollower(0, leaderAndIsrRequest(0, topic, ApiKeys.LEADER_AND_ISR.latestVersion), (_, _) => ())
      assertTrue(replicaManager.localLog(topicPartition).isDefined)
      val log2 = replicaManager.localLog(topicPartition).get
      assertFalse(log2.partitionMetadataFile.get.exists())
      assertEquals(Errors.NONE, response2.partitionErrors(topicNames).get(topicPartition))

      // There is no file if the request an older version
      val response3 = replicaManager.becomeLeaderOrFollower(0, leaderAndIsrRequest(0, "foo", 0), (_, _) => ())
      assertTrue(replicaManager.localLog(topicPartitionFoo).isDefined)
      val log3 = replicaManager.localLog(topicPartitionFoo).get
      assertFalse(log3.partitionMetadataFile.get.exists())
      assertEquals(Errors.NONE, response3.partitionErrors(topicNames).get(topicPartitionFoo))

      // There is no file if the request is an older version
      val response4 = replicaManager.becomeLeaderOrFollower(0, leaderAndIsrRequest(1, "foo", 4), (_, _) => ())
      assertTrue(replicaManager.localLog(topicPartitionFoo).isDefined)
      val log4 = replicaManager.localLog(topicPartitionFoo).get
      assertFalse(log4.partitionMetadataFile.get.exists())
      assertEquals(Errors.NONE, response4.partitionErrors(topicNames).get(topicPartitionFoo))
    } finally replicaManager.shutdown(checkpointHW = false)
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testPartitionMarkedOfflineIfLogCantBeCreated(becomeLeader: Boolean): Unit = {
    val dataDir = TestUtils.tempDir()
    val topicPartition = new TopicPartition(topic, 0)
    val replicaManager = setupReplicaManagerWithMockedPurgatories(
      timer = new MockTimer(time),
      propsModifier = props => props.put(KafkaConfig.LogDirsProp, dataDir.getAbsolutePath)
    )

    try {
      // Delete the data directory to trigger a storage exception
      Utils.delete(dataDir)

      val request = makeLeaderAndIsrRequest(
        topicId = Uuid.randomUuid(),
        topicPartition = topicPartition,
        replicas = Seq(0, 1),
        leaderAndIsr = LeaderAndIsr(if (becomeLeader) 0 else 1, List(0, 1))
      )

      replicaManager.becomeLeaderOrFollower(0, request, (_, _) => ())

      assertEquals(HostedPartition.Offline, replicaManager.getPartition(topicPartition))
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  private def makeLeaderAndIsrRequest(
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
      .setPartitionEpoch(leaderAndIsr.partitionEpoch)
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
      when(replicaManager.metadataCache.contains(fooPartition)).thenReturn(false)
      val fooProducerState = replicaManager.activeProducerState(fooPartition)
      assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION, Errors.forCode(fooProducerState.errorCode))

      val oofPartition = new TopicPartition("oof", 0)
      when(replicaManager.metadataCache.contains(oofPartition)).thenReturn(true)
      val oofProducerState = replicaManager.activeProducerState(oofPartition)
      assertEquals(Errors.NOT_LEADER_OR_FOLLOWER, Errors.forCode(oofProducerState.errorCode))

      // This API is supported by both leaders and followers

      val barPartition = new TopicPartition("bar", 0)
      val barLeaderAndIsrRequest = makeLeaderAndIsrRequest(
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
      val bazLeaderAndIsrRequest = makeLeaderAndIsrRequest(
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

  val FOO_UUID = Uuid.fromString("fFJBx0OmQG-UqeaT6YaSwA")

  val BAR_UUID = Uuid.fromString("vApAP6y7Qx23VOfKBzbOBQ")

  @Test
  def testGetOrCreatePartition(): Unit = {
    val brokerId = 0
    val replicaManager = setupReplicaManagerWithMockedPurgatories(new MockTimer(time), brokerId)
    val foo0 = new TopicPartition("foo", 0)
    val emptyDelta = new TopicsDelta(TopicsImage.EMPTY)
    val (fooPart, fooNew) = replicaManager.getOrCreatePartition(foo0, emptyDelta, FOO_UUID).get
    assertTrue(fooNew)
    assertEquals(foo0, fooPart.topicPartition)
    val (fooPart2, fooNew2) = replicaManager.getOrCreatePartition(foo0, emptyDelta, FOO_UUID).get
    assertFalse(fooNew2)
    assertTrue(fooPart eq fooPart2)
    val bar1 = new TopicPartition("bar", 1)
    replicaManager.markPartitionOffline(bar1)
    assertEquals(None, replicaManager.getOrCreatePartition(bar1, emptyDelta, BAR_UUID))
  }

  @Test
  def testDeltaFromLeaderToFollower(): Unit = {
    val localId = 1
    val otherId = localId + 1
    val numOfRecords = 3
    val topicPartition = new TopicPartition("foo", 0)
    val replicaManager = setupReplicaManagerWithMockedPurgatories(new MockTimer(time), localId)

    try {
      // Make the local replica the leader
      val leaderTopicsDelta = topicsCreateDelta(localId, true)
      val leaderMetadataImage = imageFromTopics(leaderTopicsDelta.apply())
      val topicId = leaderMetadataImage.topics().topicsByName.get("foo").id
      val topicIdPartition = new TopicIdPartition(topicId, topicPartition)
      replicaManager.applyDelta(leaderTopicsDelta, leaderMetadataImage)

      // Check the state of that partition and fetcher
      val HostedPartition.Online(leaderPartition) = replicaManager.getPartition(topicPartition)
      assertTrue(leaderPartition.isLeader)
      assertEquals(Set(localId, otherId), leaderPartition.inSyncReplicaIds)
      assertEquals(0, leaderPartition.getLeaderEpoch)

      assertEquals(None, replicaManager.replicaFetcherManager.getFetcher(topicPartition))

      // Send a produce request and advance the highwatermark
      val leaderResponse = sendProducerAppend(replicaManager, topicPartition, numOfRecords)
      fetchPartitionAsFollower(
        replicaManager,
        topicIdPartition,
        new PartitionData(Uuid.ZERO_UUID, numOfRecords, 0, Int.MaxValue, Optional.empty()),
        replicaId = otherId
      )
      assertEquals(Errors.NONE, leaderResponse.get.error)

      // Change the local replica to follower
      val followerTopicsDelta = topicsChangeDelta(leaderMetadataImage.topics(), localId, false)
      val followerMetadataImage = imageFromTopics(followerTopicsDelta.apply())
      replicaManager.applyDelta(followerTopicsDelta, followerMetadataImage)

      // Append on a follower should fail
      val followerResponse = sendProducerAppend(replicaManager, topicPartition, numOfRecords)
      assertEquals(Errors.NOT_LEADER_OR_FOLLOWER, followerResponse.get.error)

      // Check the state of that partition and fetcher
      val HostedPartition.Online(followerPartition) = replicaManager.getPartition(topicPartition)
      assertFalse(followerPartition.isLeader)
      assertEquals(1, followerPartition.getLeaderEpoch)

      val fetcher = replicaManager.replicaFetcherManager.getFetcher(topicPartition)
      assertEquals(Some(BrokerEndPoint(otherId, "localhost", 9093)), fetcher.map(_.leader.brokerEndPoint()))
    } finally {
      replicaManager.shutdown()
    }

    TestUtils.assertNoNonDaemonThreads(this.getClass.getName)
  }

  @Test
  def testDeltaFromFollowerToLeader(): Unit = {
    val localId = 1
    val otherId = localId + 1
    val numOfRecords = 3
    val topicPartition = new TopicPartition("foo", 0)
    val replicaManager = setupReplicaManagerWithMockedPurgatories(new MockTimer(time), localId)

    try {
      // Make the local replica the follower
      val followerTopicsDelta = topicsCreateDelta(localId, false)
      val followerMetadataImage = imageFromTopics(followerTopicsDelta.apply())
      replicaManager.applyDelta(followerTopicsDelta, followerMetadataImage)

      // Check the state of that partition and fetcher
      val HostedPartition.Online(followerPartition) = replicaManager.getPartition(topicPartition)
      assertFalse(followerPartition.isLeader)
      assertEquals(0, followerPartition.getLeaderEpoch)

      val fetcher = replicaManager.replicaFetcherManager.getFetcher(topicPartition)
      assertEquals(Some(BrokerEndPoint(otherId, "localhost", 9093)), fetcher.map(_.leader.brokerEndPoint()))

      // Append on a follower should fail
      val followerResponse = sendProducerAppend(replicaManager, topicPartition, numOfRecords)
      assertEquals(Errors.NOT_LEADER_OR_FOLLOWER, followerResponse.get.error)

      // Change the local replica to leader
      val leaderTopicsDelta = topicsChangeDelta(followerMetadataImage.topics(), localId, true)
      val leaderMetadataImage = imageFromTopics(leaderTopicsDelta.apply())
      val topicId = leaderMetadataImage.topics().topicsByName.get("foo").id
      val topicIdPartition = new TopicIdPartition(topicId, topicPartition)
      replicaManager.applyDelta(leaderTopicsDelta, leaderMetadataImage)

      // Send a produce request and advance the highwatermark
      val leaderResponse = sendProducerAppend(replicaManager, topicPartition, numOfRecords)
      fetchPartitionAsFollower(
        replicaManager,
        topicIdPartition,
        new PartitionData(Uuid.ZERO_UUID, numOfRecords, 0, Int.MaxValue, Optional.empty()),
        replicaId = otherId
      )
      assertEquals(Errors.NONE, leaderResponse.get.error)

      val HostedPartition.Online(leaderPartition) = replicaManager.getPartition(topicPartition)
      assertTrue(leaderPartition.isLeader)
      assertEquals(Set(localId, otherId), leaderPartition.inSyncReplicaIds)
      assertEquals(1, leaderPartition.getLeaderEpoch)

      assertEquals(None, replicaManager.replicaFetcherManager.getFetcher(topicPartition))
    } finally {
      replicaManager.shutdown()
    }

    TestUtils.assertNoNonDaemonThreads(this.getClass.getName)
  }

  @Test
  def testDeltaFollowerWithNoChange(): Unit = {
    val localId = 1
    val otherId = localId + 1
    val topicPartition = new TopicPartition("foo", 0)
    val replicaManager = setupReplicaManagerWithMockedPurgatories(new MockTimer(time), localId)

    try {
      // Make the local replica the follower
      val followerTopicsDelta = topicsCreateDelta(localId, false)
      val followerMetadataImage = imageFromTopics(followerTopicsDelta.apply())
      replicaManager.applyDelta(followerTopicsDelta, followerMetadataImage)

      // Check the state of that partition and fetcher
      val HostedPartition.Online(followerPartition) = replicaManager.getPartition(topicPartition)
      assertFalse(followerPartition.isLeader)
      assertEquals(0, followerPartition.getLeaderEpoch)

      val fetcher = replicaManager.replicaFetcherManager.getFetcher(topicPartition)
      assertEquals(Some(BrokerEndPoint(otherId, "localhost", 9093)), fetcher.map(_.leader.brokerEndPoint()))

      // Apply the same delta again
      replicaManager.applyDelta(followerTopicsDelta, followerMetadataImage)

      // Check that the state stays the same
      val HostedPartition.Online(noChangePartition) = replicaManager.getPartition(topicPartition)
      assertFalse(noChangePartition.isLeader)
      assertEquals(0, noChangePartition.getLeaderEpoch)

      val noChangeFetcher = replicaManager.replicaFetcherManager.getFetcher(topicPartition)
      assertEquals(Some(BrokerEndPoint(otherId, "localhost", 9093)), noChangeFetcher.map(_.leader.brokerEndPoint()))
    } finally {
      replicaManager.shutdown()
    }

    TestUtils.assertNoNonDaemonThreads(this.getClass.getName)
  }

  @Test
  def testDeltaFollowerToNotReplica(): Unit = {
    val localId = 1
    val otherId = localId + 1
    val topicPartition = new TopicPartition("foo", 0)
    val replicaManager = setupReplicaManagerWithMockedPurgatories(new MockTimer(time), localId)

    try {
      // Make the local replica the follower
      val followerTopicsDelta = topicsCreateDelta(localId, false)
      val followerMetadataImage = imageFromTopics(followerTopicsDelta.apply())
      replicaManager.applyDelta(followerTopicsDelta, followerMetadataImage)

      // Check the state of that partition and fetcher
      val HostedPartition.Online(followerPartition) = replicaManager.getPartition(topicPartition)
      assertFalse(followerPartition.isLeader)
      assertEquals(0, followerPartition.getLeaderEpoch)

      val fetcher = replicaManager.replicaFetcherManager.getFetcher(topicPartition)
      assertEquals(Some(BrokerEndPoint(otherId, "localhost", 9093)), fetcher.map(_.leader.brokerEndPoint()))

      // Apply changes that remove replica
      val notReplicaTopicsDelta = topicsChangeDelta(followerMetadataImage.topics(), otherId, true)
      val notReplicaMetadataImage = imageFromTopics(notReplicaTopicsDelta.apply())
      replicaManager.applyDelta(notReplicaTopicsDelta, notReplicaMetadataImage)

      // Check that the partition was removed
      assertEquals(HostedPartition.None, replicaManager.getPartition(topicPartition))
      assertEquals(None, replicaManager.replicaFetcherManager.getFetcher(topicPartition))
      assertEquals(None, replicaManager.logManager.getLog(topicPartition))
    } finally {
      replicaManager.shutdown()
    }

    TestUtils.assertNoNonDaemonThreads(this.getClass.getName)
  }

  @Test
  def testDeltaFollowerRemovedTopic(): Unit = {
    val localId = 1
    val otherId = localId + 1
    val topicPartition = new TopicPartition("foo", 0)
    val replicaManager = setupReplicaManagerWithMockedPurgatories(new MockTimer(time), localId)

    try {
      // Make the local replica the follower
      val followerTopicsDelta = topicsCreateDelta(localId, false)
      val followerMetadataImage = imageFromTopics(followerTopicsDelta.apply())
      replicaManager.applyDelta(followerTopicsDelta, followerMetadataImage)

      // Check the state of that partition and fetcher
      val HostedPartition.Online(followerPartition) = replicaManager.getPartition(topicPartition)
      assertFalse(followerPartition.isLeader)
      assertEquals(0, followerPartition.getLeaderEpoch)

      val fetcher = replicaManager.replicaFetcherManager.getFetcher(topicPartition)
      assertEquals(Some(BrokerEndPoint(otherId, "localhost", 9093)), fetcher.map(_.leader.brokerEndPoint()))

      // Apply changes that remove topic and replica
      val removeTopicsDelta = topicsDeleteDelta(followerMetadataImage.topics())
      val removeMetadataImage = imageFromTopics(removeTopicsDelta.apply())
      replicaManager.applyDelta(removeTopicsDelta, removeMetadataImage)

      // Check that the partition was removed
      assertEquals(HostedPartition.None, replicaManager.getPartition(topicPartition))
      assertEquals(None, replicaManager.replicaFetcherManager.getFetcher(topicPartition))
      assertEquals(None, replicaManager.logManager.getLog(topicPartition))
    } finally {
      replicaManager.shutdown()
    }

    TestUtils.assertNoNonDaemonThreads(this.getClass.getName)
  }

  @Test
  def testDeltaLeaderToNotReplica(): Unit = {
    val localId = 1
    val otherId = localId + 1
    val topicPartition = new TopicPartition("foo", 0)
    val replicaManager = setupReplicaManagerWithMockedPurgatories(new MockTimer(time), localId)

    try {
      // Make the local replica the leader
      val leaderTopicsDelta = topicsCreateDelta(localId, true)
      val leaderMetadataImage = imageFromTopics(leaderTopicsDelta.apply())
      replicaManager.applyDelta(leaderTopicsDelta, leaderMetadataImage)

      // Check the state of that partition and fetcher
      val HostedPartition.Online(leaderPartition) = replicaManager.getPartition(topicPartition)
      assertTrue(leaderPartition.isLeader)
      assertEquals(Set(localId, otherId), leaderPartition.inSyncReplicaIds)
      assertEquals(0, leaderPartition.getLeaderEpoch)

      assertEquals(None, replicaManager.replicaFetcherManager.getFetcher(topicPartition))

      // Apply changes that remove replica
      val notReplicaTopicsDelta = topicsChangeDelta(leaderMetadataImage.topics(), otherId, true)
      val notReplicaMetadataImage = imageFromTopics(notReplicaTopicsDelta.apply())
      replicaManager.applyDelta(notReplicaTopicsDelta, notReplicaMetadataImage)

      // Check that the partition was removed
      assertEquals(HostedPartition.None, replicaManager.getPartition(topicPartition))
      assertEquals(None, replicaManager.replicaFetcherManager.getFetcher(topicPartition))
      assertEquals(None, replicaManager.logManager.getLog(topicPartition))
    } finally {
      replicaManager.shutdown()
    }

    TestUtils.assertNoNonDaemonThreads(this.getClass.getName)
  }

  @Test
  def testDeltaLeaderToRemovedTopic(): Unit = {
    val localId = 1
    val otherId = localId + 1
    val topicPartition = new TopicPartition("foo", 0)
    val replicaManager = setupReplicaManagerWithMockedPurgatories(new MockTimer(time), localId)

    try {
      // Make the local replica the leader
      val leaderTopicsDelta = topicsCreateDelta(localId, true)
      val leaderMetadataImage = imageFromTopics(leaderTopicsDelta.apply())
      replicaManager.applyDelta(leaderTopicsDelta, leaderMetadataImage)

      // Check the state of that partition and fetcher
      val HostedPartition.Online(leaderPartition) = replicaManager.getPartition(topicPartition)
      assertTrue(leaderPartition.isLeader)
      assertEquals(Set(localId, otherId), leaderPartition.inSyncReplicaIds)
      assertEquals(0, leaderPartition.getLeaderEpoch)

      assertEquals(None, replicaManager.replicaFetcherManager.getFetcher(topicPartition))

      // Apply changes that remove topic and replica
      val removeTopicsDelta = topicsDeleteDelta(leaderMetadataImage.topics())
      val removeMetadataImage = imageFromTopics(removeTopicsDelta.apply())
      replicaManager.applyDelta(removeTopicsDelta, removeMetadataImage)

      // Check that the partition was removed
      assertEquals(HostedPartition.None, replicaManager.getPartition(topicPartition))
      assertEquals(None, replicaManager.replicaFetcherManager.getFetcher(topicPartition))
      assertEquals(None, replicaManager.logManager.getLog(topicPartition))
    } finally {
      replicaManager.shutdown()
    }

    TestUtils.assertNoNonDaemonThreads(this.getClass.getName)
  }

  @Test
  def testDeltaToFollowerCompletesProduce(): Unit = {
    val localId = 1
    val otherId = localId + 1
    val numOfRecords = 3
    val topicPartition = new TopicPartition("foo", 0)
    val replicaManager = setupReplicaManagerWithMockedPurgatories(new MockTimer(time), localId)

    try {
      // Make the local replica the leader
      val leaderTopicsDelta = topicsCreateDelta(localId, true)
      val leaderMetadataImage = imageFromTopics(leaderTopicsDelta.apply())
      replicaManager.applyDelta(leaderTopicsDelta, leaderMetadataImage)

      // Check the state of that partition and fetcher
      val HostedPartition.Online(leaderPartition) = replicaManager.getPartition(topicPartition)
      assertTrue(leaderPartition.isLeader)
      assertEquals(Set(localId, otherId), leaderPartition.inSyncReplicaIds)
      assertEquals(0, leaderPartition.getLeaderEpoch)

      assertEquals(None, replicaManager.replicaFetcherManager.getFetcher(topicPartition))

      // Send a produce request
      val leaderResponse = sendProducerAppend(replicaManager, topicPartition, numOfRecords)

      // Change the local replica to follower
      val followerTopicsDelta = topicsChangeDelta(leaderMetadataImage.topics(), localId, false)
      val followerMetadataImage = imageFromTopics(followerTopicsDelta.apply())
      replicaManager.applyDelta(followerTopicsDelta, followerMetadataImage)

      // Check that the produce failed because it changed to follower before replicating
      assertEquals(Errors.NOT_LEADER_OR_FOLLOWER, leaderResponse.get.error)
    } finally {
      replicaManager.shutdown()
    }

    TestUtils.assertNoNonDaemonThreads(this.getClass.getName)
  }

  @Test
  def testDeltaToFollowerCompletesFetch(): Unit = {
    val localId = 1
    val otherId = localId + 1
    val topicPartition = new TopicPartition("foo", 0)
    val replicaManager = setupReplicaManagerWithMockedPurgatories(new MockTimer(time), localId)

    try {
      // Make the local replica the leader
      val leaderTopicsDelta = topicsCreateDelta(localId, true)
      val leaderMetadataImage = imageFromTopics(leaderTopicsDelta.apply())
      val topicId = leaderMetadataImage.topics().topicsByName.get("foo").id
      val topicIdPartition = new TopicIdPartition(topicId, topicPartition)
      replicaManager.applyDelta(leaderTopicsDelta, leaderMetadataImage)

      // Check the state of that partition and fetcher
      val HostedPartition.Online(leaderPartition) = replicaManager.getPartition(topicPartition)
      assertTrue(leaderPartition.isLeader)
      assertEquals(Set(localId, otherId), leaderPartition.inSyncReplicaIds)
      assertEquals(0, leaderPartition.getLeaderEpoch)

      assertEquals(None, replicaManager.replicaFetcherManager.getFetcher(topicPartition))

      // Send a fetch request
      val fetchCallback = fetchPartitionAsFollower(
        replicaManager,
        topicIdPartition,
        new PartitionData(Uuid.ZERO_UUID, 0, 0, Int.MaxValue, Optional.empty()),
        replicaId = otherId,
        minBytes = Int.MaxValue,
        maxWaitMs = 1000
      )
      assertFalse(fetchCallback.hasFired)

      // Change the local replica to follower
      val followerTopicsDelta = topicsChangeDelta(leaderMetadataImage.topics(), localId, false)
      val followerMetadataImage = imageFromTopics(followerTopicsDelta.apply())
      replicaManager.applyDelta(followerTopicsDelta, followerMetadataImage)

      // Check that the produce failed because it changed to follower before replicating
      assertEquals(Errors.NOT_LEADER_OR_FOLLOWER, fetchCallback.assertFired.error)
    } finally {
      replicaManager.shutdown()
    }

    TestUtils.assertNoNonDaemonThreads(this.getClass.getName)
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testDeltaToLeaderOrFollowerMarksPartitionOfflineIfLogCantBeCreated(isStartIdLeader: Boolean): Unit = {
    val localId = 1
    val topicPartition = new TopicPartition("foo", 0)
    val dataDir = TestUtils.tempDir()
    val replicaManager = setupReplicaManagerWithMockedPurgatories(
      timer = new MockTimer(time),
      brokerId = localId,
      propsModifier = props => props.put(KafkaConfig.LogDirsProp, dataDir.getAbsolutePath)
    )

    try {
      // Delete the data directory to trigger a storage exception
      Utils.delete(dataDir)

      // Make the local replica the leader
      val topicsDelta = topicsCreateDelta(localId, isStartIdLeader)
      val leaderMetadataImage = imageFromTopics(topicsDelta.apply())
      replicaManager.applyDelta(topicsDelta, leaderMetadataImage)

      assertEquals(HostedPartition.Offline, replicaManager.getPartition(topicPartition))
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testDeltaFollowerStopFetcherBeforeCreatingInitialFetchOffset(): Unit = {
    val localId = 1
    val otherId = localId + 1
    val topicPartition = new TopicPartition("foo", 0)

    val mockReplicaFetcherManager = mock(classOf[ReplicaFetcherManager])
    val replicaManager = setupReplicaManagerWithMockedPurgatories(
      timer = new MockTimer(time),
      brokerId = localId,
      mockReplicaFetcherManager = Some(mockReplicaFetcherManager)
    )

    try {
      // The first call to removeFetcherForPartitions should be ignored.
      when(mockReplicaFetcherManager.removeFetcherForPartitions(
        Set(topicPartition))
      ).thenReturn(Map.empty[TopicPartition, PartitionFetchState])

      // Make the local replica the follower
      var followerTopicsDelta = topicsCreateDelta(localId, false)
      var followerMetadataImage = imageFromTopics(followerTopicsDelta.apply())
      replicaManager.applyDelta(followerTopicsDelta, followerMetadataImage)

      // Check the state of that partition
      val HostedPartition.Online(followerPartition) = replicaManager.getPartition(topicPartition)
      assertFalse(followerPartition.isLeader)
      assertEquals(0, followerPartition.getLeaderEpoch)
      assertEquals(0, followerPartition.localLogOrException.logEndOffset)

      // Verify that addFetcherForPartitions was called with the correct
      // init offset.
      verify(mockReplicaFetcherManager)
        .addFetcherForPartitions(
          Map(topicPartition -> InitialFetchState(
            topicId = Some(FOO_UUID),
            leader = BrokerEndPoint(otherId, "localhost", 9093),
            currentLeaderEpoch = 0,
            initOffset = 0
          ))
        )

      // The second call to removeFetcherForPartitions simulate the case
      // where the fetcher write to the log before being shutdown.
      when(mockReplicaFetcherManager.removeFetcherForPartitions(
        Set(topicPartition))
      ).thenAnswer { _ =>
        replicaManager.getPartition(topicPartition) match {
          case HostedPartition.Online(partition) =>
            partition.appendRecordsToFollowerOrFutureReplica(
              records = MemoryRecords.withRecords(CompressionType.NONE, 0,
                new SimpleRecord("first message".getBytes)),
              isFuture = false
            )

          case _ =>
        }

        Map.empty[TopicPartition, PartitionFetchState]
      }

      // Apply changes that bumps the leader epoch.
      followerTopicsDelta = topicsChangeDelta(followerMetadataImage.topics(), localId, false)
      followerMetadataImage = imageFromTopics(followerTopicsDelta.apply())
      replicaManager.applyDelta(followerTopicsDelta, followerMetadataImage)

      assertFalse(followerPartition.isLeader)
      assertEquals(1, followerPartition.getLeaderEpoch)
      assertEquals(1, followerPartition.localLogOrException.logEndOffset)

      // Verify that addFetcherForPartitions was called with the correct
      // init offset.
      verify(mockReplicaFetcherManager)
        .addFetcherForPartitions(
          Map(topicPartition -> InitialFetchState(
            topicId = Some(FOO_UUID),
            leader = BrokerEndPoint(otherId, "localhost", 9093),
            currentLeaderEpoch = 1,
            initOffset = 1
          ))
        )
    } finally {
      replicaManager.shutdown()
    }

    TestUtils.assertNoNonDaemonThreads(this.getClass.getName)
  }

  @Test
  def testFetcherAreNotRestartedIfLeaderEpochIsNotBumpedWithZkPath(): Unit = {
    val localId = 0
    val topicPartition = new TopicPartition("foo", 0)

    val mockReplicaFetcherManager = mock(classOf[ReplicaFetcherManager])
    val replicaManager = setupReplicaManagerWithMockedPurgatories(
      timer = new MockTimer(time),
      brokerId = localId,
      aliveBrokerIds = Seq(localId, localId + 1, localId + 2),
      mockReplicaFetcherManager = Some(mockReplicaFetcherManager)
    )

    try {
      when(mockReplicaFetcherManager.removeFetcherForPartitions(
        Set(topicPartition))
      ).thenReturn(Map.empty[TopicPartition, PartitionFetchState])

      // Make the local replica the follower.
      var request = makeLeaderAndIsrRequest(
        topicId = FOO_UUID,
        topicPartition = topicPartition,
        replicas = Seq(localId, localId + 1),
        leaderAndIsr = LeaderAndIsr(
          leader = localId + 1,
          leaderEpoch = 0,
          isr = List(localId, localId + 1),
          leaderRecoveryState = LeaderRecoveryState.RECOVERED,
          partitionEpoch = 0
        )
      )

      replicaManager.becomeLeaderOrFollower(0, request, (_, _) => ())

      // Check the state of that partition.
      val HostedPartition.Online(followerPartition) = replicaManager.getPartition(topicPartition)
      assertFalse(followerPartition.isLeader)
      assertEquals(0, followerPartition.getLeaderEpoch)
      assertEquals(0, followerPartition.getPartitionEpoch)

      // Verify that the partition was removed and added back.
      verify(mockReplicaFetcherManager).removeFetcherForPartitions(Set(topicPartition))
      verify(mockReplicaFetcherManager).addFetcherForPartitions(Map(topicPartition -> InitialFetchState(
        topicId = Some(FOO_UUID),
        leader = BrokerEndPoint(localId + 1, s"host${localId + 1}", localId + 1),
        currentLeaderEpoch = 0,
        initOffset = 0
      )))

      reset(mockReplicaFetcherManager)

      // Apply changes that bumps the partition epoch.
      request = makeLeaderAndIsrRequest(
        topicId = FOO_UUID,
        topicPartition = topicPartition,
        replicas = Seq(localId, localId + 1, localId + 2),
        leaderAndIsr = LeaderAndIsr(
          leader = localId + 1,
          leaderEpoch = 0,
          isr = List(localId, localId + 1),
          leaderRecoveryState = LeaderRecoveryState.RECOVERED,
          partitionEpoch = 1
        )
      )

      replicaManager.becomeLeaderOrFollower(0, request, (_, _) => ())

      assertFalse(followerPartition.isLeader)
      assertEquals(0, followerPartition.getLeaderEpoch)
      // Partition updates is fenced based on the leader epoch on the ZK path.
      assertEquals(0, followerPartition.getPartitionEpoch)

      // As the update is fenced based on the leader epoch, removeFetcherForPartitions and
      // addFetcherForPartitions are not called at all.
      reset(mockReplicaFetcherManager)

      // Apply changes that bumps the leader epoch.
      request = makeLeaderAndIsrRequest(
        topicId = FOO_UUID,
        topicPartition = topicPartition,
        replicas = Seq(localId, localId + 1, localId + 2),
        leaderAndIsr = LeaderAndIsr(
          leader = localId + 2,
          leaderEpoch = 1,
          isr = List(localId, localId + 1, localId + 2),
          leaderRecoveryState = LeaderRecoveryState.RECOVERED,
          partitionEpoch = 2
        )
      )

      replicaManager.becomeLeaderOrFollower(0, request, (_, _) => ())

      assertFalse(followerPartition.isLeader)
      assertEquals(1, followerPartition.getLeaderEpoch)
      assertEquals(2, followerPartition.getPartitionEpoch)

      // Verify that the partition was removed and added back.
      verify(mockReplicaFetcherManager).removeFetcherForPartitions(Set(topicPartition))
      verify(mockReplicaFetcherManager).addFetcherForPartitions(Map(topicPartition -> InitialFetchState(
        topicId = Some(FOO_UUID),
        leader = BrokerEndPoint(localId + 2, s"host${localId + 2}", localId + 2),
        currentLeaderEpoch = 1,
        initOffset = 0
      )))
    } finally {
      replicaManager.shutdown()
    }

    TestUtils.assertNoNonDaemonThreads(this.getClass.getName)
  }

  @Test
  def testFetcherAreNotRestartedIfLeaderEpochIsNotBumpedWithKRaftPath(): Unit = {
    val localId = 0
    val topicPartition = new TopicPartition("foo", 0)

    val mockReplicaFetcherManager = mock(classOf[ReplicaFetcherManager])
    val replicaManager = setupReplicaManagerWithMockedPurgatories(
      timer = new MockTimer(time),
      brokerId = localId,
      mockReplicaFetcherManager = Some(mockReplicaFetcherManager)
    )

    try {
      when(mockReplicaFetcherManager.removeFetcherForPartitions(
        Set(topicPartition))
      ).thenReturn(Map.empty[TopicPartition, PartitionFetchState])

      // Make the local replica the follower.
      var followerTopicsDelta = new TopicsDelta(TopicsImage.EMPTY)
      followerTopicsDelta.replay(new TopicRecord().setName("foo").setTopicId(FOO_UUID))
      followerTopicsDelta.replay(new PartitionRecord()
        .setPartitionId(0)
        .setTopicId(FOO_UUID)
        .setReplicas(util.Arrays.asList(localId, localId + 1))
        .setIsr(util.Arrays.asList(localId, localId + 1))
        .setRemovingReplicas(Collections.emptyList())
        .setAddingReplicas(Collections.emptyList())
        .setLeader(localId + 1)
        .setLeaderEpoch(0)
        .setPartitionEpoch(0)
      )
      var followerMetadataImage = imageFromTopics(followerTopicsDelta.apply())
      replicaManager.applyDelta(followerTopicsDelta, followerMetadataImage)

      // Check the state of that partition.
      val HostedPartition.Online(followerPartition) = replicaManager.getPartition(topicPartition)
      assertFalse(followerPartition.isLeader)
      assertEquals(0, followerPartition.getLeaderEpoch)
      assertEquals(0, followerPartition.getPartitionEpoch)

      // Verify that the partition was removed and added back.
      verify(mockReplicaFetcherManager).removeFetcherForPartitions(Set(topicPartition))
      verify(mockReplicaFetcherManager).addFetcherForPartitions(Map(topicPartition -> InitialFetchState(
        topicId = Some(FOO_UUID),
        leader = BrokerEndPoint(localId + 1, "localhost", 9093),
        currentLeaderEpoch = 0,
        initOffset = 0
      )))

      reset(mockReplicaFetcherManager)

      // Apply changes that bumps the partition epoch.
      followerTopicsDelta = new TopicsDelta(followerMetadataImage.topics())
      followerTopicsDelta.replay(new PartitionChangeRecord()
        .setPartitionId(0)
        .setTopicId(FOO_UUID)
        .setReplicas(util.Arrays.asList(localId, localId + 1, localId + 2))
        .setIsr(util.Arrays.asList(localId, localId + 1))
      )
      followerMetadataImage = imageFromTopics(followerTopicsDelta.apply())
      replicaManager.applyDelta(followerTopicsDelta, followerMetadataImage)

      assertFalse(followerPartition.isLeader)
      assertEquals(0, followerPartition.getLeaderEpoch)
      assertEquals(1, followerPartition.getPartitionEpoch)

      // Verify that partition's fetcher was not impacted.
      verify(mockReplicaFetcherManager, never()).removeFetcherForPartitions(any())
      verify(mockReplicaFetcherManager, never()).addFetcherForPartitions(any())

      reset(mockReplicaFetcherManager)

      // Apply changes that bumps the leader epoch.
      followerTopicsDelta = new TopicsDelta(followerMetadataImage.topics())
      followerTopicsDelta.replay(new PartitionChangeRecord()
        .setPartitionId(0)
        .setTopicId(FOO_UUID)
        .setReplicas(util.Arrays.asList(localId, localId + 1, localId + 2))
        .setIsr(util.Arrays.asList(localId, localId + 1, localId + 2))
        .setLeader(localId + 2)
      )

      followerMetadataImage = imageFromTopics(followerTopicsDelta.apply())
      replicaManager.applyDelta(followerTopicsDelta, followerMetadataImage)

      assertFalse(followerPartition.isLeader)
      assertEquals(1, followerPartition.getLeaderEpoch)
      assertEquals(2, followerPartition.getPartitionEpoch)

      // Verify that the partition was removed and added back.
      verify(mockReplicaFetcherManager).removeFetcherForPartitions(Set(topicPartition))
      verify(mockReplicaFetcherManager).addFetcherForPartitions(Map(topicPartition -> InitialFetchState(
        topicId = Some(FOO_UUID),
        leader = BrokerEndPoint(localId + 2, "localhost", 9093),
        currentLeaderEpoch = 1,
        initOffset = 0
      )))
    } finally {
      replicaManager.shutdown()
    }

    TestUtils.assertNoNonDaemonThreads(this.getClass.getName)
  }

  @Test
  def testReplicasAreStoppedWhileInControlledShutdownWithKRaft(): Unit = {
    val localId = 0
    val foo0 = new TopicPartition("foo", 0)
    val foo1 = new TopicPartition("foo", 1)
    val foo2 = new TopicPartition("foo", 2)

    val mockReplicaFetcherManager = mock(classOf[ReplicaFetcherManager])
    val isShuttingDown = new AtomicBoolean(false)
    val replicaManager = setupReplicaManagerWithMockedPurgatories(
      timer = new MockTimer(time),
      brokerId = localId,
      mockReplicaFetcherManager = Some(mockReplicaFetcherManager),
      isShuttingDown = isShuttingDown
    )

    try {
      when(mockReplicaFetcherManager.removeFetcherForPartitions(
        Set(foo0, foo1))
      ).thenReturn(Map.empty[TopicPartition, PartitionFetchState])

      var topicsDelta = new TopicsDelta(TopicsImage.EMPTY)
      topicsDelta.replay(new TopicRecord()
        .setName("foo")
        .setTopicId(FOO_UUID)
      )

      // foo0 is a follower in the ISR.
      topicsDelta.replay(new PartitionRecord()
        .setPartitionId(0)
        .setTopicId(FOO_UUID)
        .setReplicas(util.Arrays.asList(localId, localId + 1))
        .setIsr(util.Arrays.asList(localId, localId + 1))
        .setLeader(localId + 1)
        .setLeaderEpoch(0)
        .setPartitionEpoch(0)
      )

      // foo1 is a leader with only himself in the ISR.
      topicsDelta.replay(new PartitionRecord()
        .setPartitionId(1)
        .setTopicId(FOO_UUID)
        .setReplicas(util.Arrays.asList(localId, localId + 1))
        .setIsr(util.Arrays.asList(localId))
        .setLeader(localId)
        .setLeaderEpoch(0)
        .setPartitionEpoch(0)
      )

      // foo2 is a follower NOT in the ISR.
      topicsDelta.replay(new PartitionRecord()
        .setPartitionId(2)
        .setTopicId(FOO_UUID)
        .setReplicas(util.Arrays.asList(localId, localId + 1))
        .setIsr(util.Arrays.asList(localId + 1))
        .setLeader(localId + 1)
        .setLeaderEpoch(0)
        .setPartitionEpoch(0)
      )

      // Apply the delta.
      var metadataImage = imageFromTopics(topicsDelta.apply())
      replicaManager.applyDelta(topicsDelta, metadataImage)

      // Check the state of the partitions.
      val HostedPartition.Online(fooPartition0) = replicaManager.getPartition(foo0)
      assertFalse(fooPartition0.isLeader)
      assertEquals(0, fooPartition0.getLeaderEpoch)
      assertEquals(0, fooPartition0.getPartitionEpoch)

      val HostedPartition.Online(fooPartition1) = replicaManager.getPartition(foo1)
      assertTrue(fooPartition1.isLeader)
      assertEquals(0, fooPartition1.getLeaderEpoch)
      assertEquals(0, fooPartition1.getPartitionEpoch)

      val HostedPartition.Online(fooPartition2) = replicaManager.getPartition(foo2)
      assertFalse(fooPartition2.isLeader)
      assertEquals(0, fooPartition2.getLeaderEpoch)
      assertEquals(0, fooPartition2.getPartitionEpoch)

      reset(mockReplicaFetcherManager)

      // The broker transitions to SHUTTING_DOWN state. This should not have
      // any impact in KRaft mode.
      isShuttingDown.set(true)

      // The replica begins the controlled shutdown.
      replicaManager.beginControlledShutdown()

      // When the controller receives the controlled shutdown
      // request, it does the following:
      // - Shrinks the ISR of foo0 to remove this replica.
      // - Sets the leader of foo1 to NO_LEADER because it cannot elect another leader.
      // - Does nothing for foo2 because this replica is not in the ISR.
      topicsDelta = new TopicsDelta(metadataImage.topics())
      topicsDelta.replay(new PartitionChangeRecord()
        .setPartitionId(0)
        .setTopicId(FOO_UUID)
        .setReplicas(util.Arrays.asList(localId, localId + 1))
        .setIsr(util.Arrays.asList(localId + 1))
        .setLeader(localId + 1)
      )
      topicsDelta.replay(new PartitionChangeRecord()
        .setPartitionId(1)
        .setTopicId(FOO_UUID)
        .setReplicas(util.Arrays.asList(localId, localId + 1))
        .setIsr(util.Arrays.asList(localId))
        .setLeader(NO_LEADER)
      )
      metadataImage = imageFromTopics(topicsDelta.apply())
      replicaManager.applyDelta(topicsDelta, metadataImage)

      // Partition foo0 and foo1 are updated.
      assertFalse(fooPartition0.isLeader)
      assertEquals(1, fooPartition0.getLeaderEpoch)
      assertEquals(1, fooPartition0.getPartitionEpoch)
      assertFalse(fooPartition1.isLeader)
      assertEquals(1, fooPartition1.getLeaderEpoch)
      assertEquals(1, fooPartition1.getPartitionEpoch)

      // Partition foo2 is not.
      assertFalse(fooPartition2.isLeader)
      assertEquals(0, fooPartition2.getLeaderEpoch)
      assertEquals(0, fooPartition2.getPartitionEpoch)

      // Fetcher for foo0 and foo1 are stopped.
      verify(mockReplicaFetcherManager).removeFetcherForPartitions(Set(foo0, foo1))
    } finally {
      // Fetcher for foo2 is stopped when the replica manager shuts down
      // because this replica was not in the ISR.
      replicaManager.shutdown()
    }

    TestUtils.assertNoNonDaemonThreads(this.getClass.getName)
  }

  private def topicsCreateDelta(startId: Int, isStartIdLeader: Boolean): TopicsDelta = {
    val leader = if (isStartIdLeader) startId else startId + 1
    val delta = new TopicsDelta(TopicsImage.EMPTY)
    delta.replay(new TopicRecord().setName("foo").setTopicId(FOO_UUID))
    delta.replay(
      new PartitionRecord()
        .setPartitionId(0)
        .setTopicId(FOO_UUID)
        .setReplicas(util.Arrays.asList(startId, startId + 1))
        .setIsr(util.Arrays.asList(startId, startId + 1))
        .setRemovingReplicas(Collections.emptyList())
        .setAddingReplicas(Collections.emptyList())
        .setLeader(leader)
        .setLeaderEpoch(0)
        .setPartitionEpoch(0)
    )

    delta
  }

  private def topicsChangeDelta(topicsImage: TopicsImage, startId: Int, isStartIdLeader: Boolean): TopicsDelta = {
    val leader = if (isStartIdLeader) startId else startId + 1
    val delta = new TopicsDelta(topicsImage)
    delta.replay(
      new PartitionChangeRecord()
        .setPartitionId(0)
        .setTopicId(FOO_UUID)
        .setReplicas(util.Arrays.asList(startId, startId + 1))
        .setIsr(util.Arrays.asList(startId, startId + 1))
        .setLeader(leader)
    )
    delta
  }

  private def topicsDeleteDelta(topicsImage: TopicsImage): TopicsDelta = {
    val delta = new TopicsDelta(topicsImage)
    delta.replay(new RemoveTopicRecord().setTopicId(FOO_UUID))

    delta
  }

  private def imageFromTopics(topicsImage: TopicsImage): MetadataImage = {
    new MetadataImage(
      new MetadataProvenance(100L, 10, 1000L),
      FeaturesImage.EMPTY,
      ClusterImageTest.IMAGE1,
      topicsImage,
      ConfigurationsImage.EMPTY,
      ClientQuotasImage.EMPTY,
      ProducerIdsImage.EMPTY,
      AclsImage.EMPTY
    )
  }

  def assertFetcherHasTopicId[T <: AbstractFetcherThread](manager: AbstractFetcherManager[T],
                                                          tp: TopicPartition,
                                                          expectedTopicId: Option[Uuid]): Unit = {
    val fetchState = manager.getFetcher(tp).flatMap(_.fetchState(tp))
    assertTrue(fetchState.isDefined)
    assertEquals(expectedTopicId, fetchState.get.topicId)
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testPartitionFetchStateUpdatesWithTopicIdChanges(startsWithTopicId: Boolean): Unit = {
    val aliveBrokersIds = Seq(0, 1)
    val replicaManager = setupReplicaManagerWithMockedPurgatories(new MockTimer(time),
      brokerId = 0, aliveBrokersIds)
    try {
      val tp = new TopicPartition(topic, 0)
      val leaderAndIsr = LeaderAndIsr(1, aliveBrokersIds.toList)

      // This test either starts with a topic ID in the PartitionFetchState and removes it on the next request (startsWithTopicId)
      // or does not start with a topic ID in the PartitionFetchState and adds one on the next request (!startsWithTopicId)
      val startingId = if (startsWithTopicId) topicId else Uuid.ZERO_UUID
      val startingIdOpt = if (startsWithTopicId) Some(topicId) else None
      val leaderAndIsrRequest1 = makeLeaderAndIsrRequest(startingId, tp, aliveBrokersIds, leaderAndIsr)
      val leaderAndIsrResponse1 = replicaManager.becomeLeaderOrFollower(0, leaderAndIsrRequest1, (_, _) => ())
      assertEquals(Errors.NONE, leaderAndIsrResponse1.error)

      assertFetcherHasTopicId(replicaManager.replicaFetcherManager, tp, startingIdOpt)

      val endingId = if (!startsWithTopicId) topicId else Uuid.ZERO_UUID
      val endingIdOpt = if (!startsWithTopicId) Some(topicId) else None
      val leaderAndIsrRequest2 = makeLeaderAndIsrRequest(endingId, tp, aliveBrokersIds, leaderAndIsr)
      val leaderAndIsrResponse2 = replicaManager.becomeLeaderOrFollower(0, leaderAndIsrRequest2, (_, _) => ())
      assertEquals(Errors.NONE, leaderAndIsrResponse2.error)

      assertFetcherHasTopicId(replicaManager.replicaFetcherManager, tp, endingIdOpt)
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testReplicaAlterLogDirsWithAndWithoutIds(usesTopicIds: Boolean): Unit = {
    val tp = new TopicPartition(topic, 0)
    val version = if (usesTopicIds) LeaderAndIsrRequestData.HIGHEST_SUPPORTED_VERSION else 4.toShort
    val topicId = if (usesTopicIds) this.topicId else Uuid.ZERO_UUID
    val topicIdOpt = if (usesTopicIds) Some(topicId) else None

    val mockReplicaAlterLogDirsManager = mock(classOf[ReplicaAlterLogDirsManager])
    val replicaManager = setupReplicaManagerWithMockedPurgatories(
      timer = new MockTimer(time),
      mockReplicaAlterLogDirsManager = Some(mockReplicaAlterLogDirsManager)
    )

    try {
      replicaManager.createPartition(tp).createLogIfNotExists(
        isNew = false,
        isFutureReplica = false,
        offsetCheckpoints = new LazyOffsetCheckpoints(replicaManager.highWatermarkCheckpoints),
        topicId = None
      )

      val leaderAndIsrRequest = makeLeaderAndIsrRequest(
        topicId = topicId,
        topicPartition = tp,
        replicas = Seq(0, 1),
        leaderAndIsr = LeaderAndIsr(0, List(0, 1)),
        version = version
      )
      replicaManager.becomeLeaderOrFollower(0, leaderAndIsrRequest, (_, _) => ())

      // Move the replica to the second log directory.
      val partition = replicaManager.getPartitionOrException(tp)
      val newReplicaFolder = replicaManager.logManager.liveLogDirs.filterNot(_ == partition.log.get.dir.getParentFile).head
      replicaManager.alterReplicaLogDirs(Map(tp -> newReplicaFolder.getAbsolutePath))

      // Make sure the future log is created with the correct topic ID.
      val futureLog = replicaManager.futureLocalLogOrException(tp)
      assertEquals(topicIdOpt, futureLog.topicId)

      // Verify that addFetcherForPartitions was called with the correct topic ID.
      verify(mockReplicaAlterLogDirsManager, times(1))
        .addFetcherForPartitions(Map(tp -> InitialFetchState(
          topicId = topicIdOpt,
          leader = BrokerEndPoint(0, "localhost", -1),
          currentLeaderEpoch = 0,
          initOffset = 0
        )))
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testDescribeLogDirs(): Unit = {
    val topicPartition = 0
    val topicId = Uuid.randomUuid()
    val followerBrokerId = 0
    val leaderBrokerId = 1
    val leaderEpoch = 1
    val leaderEpochIncrement = 2
    val countDownLatch = new CountDownLatch(1)
    val offsetFromLeader = 5

    // Prepare the mocked components for the test
    val (replicaManager, mockLogMgr) = prepareReplicaManagerAndLogManager(new MockTimer(time),
      topicPartition, leaderEpoch + leaderEpochIncrement, followerBrokerId, leaderBrokerId, countDownLatch,
      expectTruncation = false, localLogOffset = Some(10), offsetFromLeader = offsetFromLeader, topicId = Some(topicId))

    val responses = replicaManager.describeLogDirs(Set(new TopicPartition(topic, topicPartition)))
    assertEquals(mockLogMgr.liveLogDirs.size, responses.size)
    responses.foreach { response =>
      assertEquals(Errors.NONE.code, response.errorCode)
      assertTrue(response.totalBytes > 0)
      assertTrue(response.usableBytes >= 0)
    }
  }
}

class MockReplicaSelector extends ReplicaSelector {

  private val selectionCount = new AtomicLong()
  private var partitionViewArgument: Option[PartitionView] = None

  def getSelectionCount: Long = selectionCount.get
  def getPartitionViewArgument: Option[PartitionView] = partitionViewArgument

  override def select(topicPartition: TopicPartition, clientMetadata: ClientMetadata, partitionView: PartitionView): Optional[ReplicaView] = {
    selectionCount.incrementAndGet()
    partitionViewArgument = Some(partitionView)
    Optional.of(partitionView.leader)
  }
}
