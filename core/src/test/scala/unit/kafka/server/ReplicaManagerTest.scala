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

import com.yammer.metrics.core.{Gauge, Meter, Timer}
import kafka.cluster.PartitionTest.MockPartitionListener
import kafka.cluster.Partition
import kafka.log.LogManager
import org.apache.kafka.server.log.remote.quota.RLMQuotaManagerConfig.INACTIVE_SENSOR_EXPIRATION_TIME_SECONDS
import org.apache.kafka.server.log.remote.quota.RLMQuotaMetrics
import kafka.server.QuotaFactory.{QuotaManagers, UNBOUNDED_QUOTA}
import kafka.server.epoch.util.MockBlockingSender
import kafka.server.metadata.KRaftMetadataCache
import kafka.server.share.{DelayedShareFetch, SharePartition}
import kafka.utils.TestUtils.waitUntilTrue
import kafka.utils.TestUtils
import org.apache.kafka.clients.FetchSessionHandler
import org.apache.kafka.clients.consumer.ShareAcquireMode
import org.apache.kafka.common.{DirectoryId, IsolationLevel, Node, TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.common.compress.Compression
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.errors.InvalidPidMappingException
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.message.{DeleteRecordsResponseData, FetchResponseData, ShareFetchResponseData}
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.EpochEndOffset
import org.apache.kafka.common.metadata.{PartitionChangeRecord, PartitionRecord, RemoveTopicRecord, TopicRecord}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.metrics.Monitorable
import org.apache.kafka.common.metrics.PluginMetrics
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record._
import org.apache.kafka.common.replica.ClientMetadata.DefaultClientMetadata
import org.apache.kafka.common.replica.ReplicaView.DefaultReplicaView
import org.apache.kafka.common.replica.{ClientMetadata, PartitionView, ReplicaSelector, ReplicaView}
import org.apache.kafka.common.requests.FetchRequest.PartitionData
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.requests._
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.utils.{LogContext, Time, Utils}
import org.apache.kafka.coordinator.transaction.{AddPartitionsToTxnConfig, TransactionLogConfig}
import org.apache.kafka.image._
import org.apache.kafka.metadata.LeaderConstants.NO_LEADER
import org.apache.kafka.metadata.{LeaderRecoveryState, MetadataCache, PartitionRegistration}
import org.apache.kafka.metadata.properties.{MetaProperties, MetaPropertiesEnsemble, MetaPropertiesVersion, PropertiesUtils}
import org.apache.kafka.server.common.{DirectoryEventHandler, KRaftVersion, MetadataVersion, OffsetAndEpoch, RequestLocal, StopPartition}
import org.apache.kafka.server.config.{KRaftConfigs, ReplicationConfigs, ServerLogConfigs}
import org.apache.kafka.server.log.remote.TopicPartitionLog
import org.apache.kafka.server.log.remote.storage._
import org.apache.kafka.server.metrics.{KafkaMetricsGroup, KafkaYammerMetrics}
import org.apache.kafka.server.network.BrokerEndPoint
import org.apache.kafka.server.PartitionFetchState
import org.apache.kafka.server.purgatory.{DelayedDeleteRecords, DelayedOperationPurgatory, DelayedRemoteFetch, DelayedRemoteListOffsets}
import org.apache.kafka.server.share.SharePartitionKey
import org.apache.kafka.server.share.fetch.{DelayedShareFetchGroupKey, DelayedShareFetchKey, ShareFetch}
import org.apache.kafka.server.share.metrics.ShareGroupMetrics
import org.apache.kafka.server.storage.log.{FetchIsolation, FetchParams, FetchPartitionData}
import org.apache.kafka.server.transaction.AddPartitionsToTxnManager
import org.apache.kafka.server.transaction.AddPartitionsToTxnManager.TransactionSupportedOperation
import org.apache.kafka.server.transaction.AddPartitionsToTxnManager.TransactionSupportedOperation.{ADD_PARTITION, GENERIC_ERROR_SUPPORTED}
import org.apache.kafka.server.util.timer.{MockTimer, SystemTimer}
import org.apache.kafka.server.util.{MockScheduler, MockTime, Scheduler}
import org.apache.kafka.storage.internals.checkpoint.LazyOffsetCheckpoints
import org.apache.kafka.storage.internals.epoch.LeaderEpochFileCache
import org.apache.kafka.storage.internals.log.{AppendOrigin, CleanerConfig, FetchDataInfo, LocalLog, LogAppendInfo, LogConfig, LogDirFailureChannel, LogLoader, LogOffsetMetadata, LogOffsetSnapshot, LogOffsetsListener, LogReadResult, LogSegments, ProducerStateManager, ProducerStateManagerConfig, RemoteLogReadResult, RemoteStorageFetchInfo, UnifiedLog, VerificationGuard}
import org.apache.kafka.storage.log.metrics.BrokerTopicStats
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterAll, AfterEach, BeforeEach, Test}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{EnumSource, ValueSource}
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.mockito.{ArgumentCaptor, ArgumentMatchers}

import java.io.{ByteArrayInputStream, File}
import java.net.InetAddress
import java.nio.file.{Files, Paths}
import java.util
import java.util.concurrent.atomic.{AtomicLong, AtomicReference}
import java.util.concurrent.{Callable, CompletableFuture, ConcurrentHashMap, CountDownLatch, Future, TimeUnit}
import java.util.function.{BiConsumer, Consumer}
import java.util.stream.IntStream
import java.util.{Collections, Optional, OptionalLong, Properties}
import scala.collection.{Map, Seq, mutable}
import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters.{RichOption, RichOptional}

object ReplicaManagerTest {
  @AfterAll
  def tearDownClass(): Unit = {
    TestUtils.clearYammerMetrics()
  }
}

class ReplicaManagerTest {

  private val topic = "test-topic"
  private val topic2 = "test-topic2"
  private val topicId = Uuid.fromString("YK2ed2GaTH2JpgzUaJ8tgg")
  private val topicId2 = Uuid.randomUuid()
  private val topicIds = scala.Predef.Map("test-topic" -> topicId)
  private val topicNames = topicIds.map(_.swap)
  private val topicPartition = new TopicPartition(topic, 0)
  private val transactionalId = "txn"
  private val time = new MockTime
  private val metrics = new Metrics
  private val startOffset = 0
  private val endOffset = 20
  private val highHW = 18
  private var alterPartitionManager: AlterPartitionManager = _
  private var config: KafkaConfig = _
  private var quotaManager: QuotaManagers = _
  private var mockRemoteLogManager: RemoteLogManager = _
  private var addPartitionsToTxnManager: AddPartitionsToTxnManager = _
  private var brokerTopicStats: BrokerTopicStats = _
  private val metadataCache: KRaftMetadataCache = mock(classOf[KRaftMetadataCache])
  private val quotaExceededThrottleTime = 1000
  private val quotaAvailableThrottleTime = 0

  // Constants defined for readability
  private val partitionEpoch = 0
  private val brokerEpoch = 0L

  // These metrics are static and once we remove them after each test, they won't be created and verified anymore
  private val metricsToBeDeletedInTheEnd = Set("kafka.server:type=DelayedRemoteFetchMetrics,name=ExpiresPerSec")

  @BeforeEach
  def setUp(): Unit = {
    val props = TestUtils.createBrokerConfig(1)
    config = KafkaConfig.fromProps(props)
    alterPartitionManager = mock(classOf[AlterPartitionManager])
    quotaManager = QuotaFactory.instantiate(config, metrics, time, "", "")
    mockRemoteLogManager = mock(classOf[RemoteLogManager])
    when(mockRemoteLogManager.fetchThrottleTimeSensor()).thenReturn(
      new RLMQuotaMetrics(metrics,
        "remote-fetch-throttle-time",
        classOf[RemoteLogManager].getSimpleName,
        "The %s time in millis remote fetches was throttled by a broker",
        INACTIVE_SENSOR_EXPIRATION_TIME_SECONDS)
        .sensor())
    addPartitionsToTxnManager = mock(classOf[AddPartitionsToTxnManager])

    // Anytime we try to verify, just automatically run the callback as though the transaction was verified.
    when(addPartitionsToTxnManager.addOrVerifyTransaction(any(), any(), any(), any(), any(), any())).thenAnswer { invocationOnMock =>
      val callback = invocationOnMock.getArgument(4, classOf[AddPartitionsToTxnManager.AppendCallback])
      callback.complete(util.Map.of())
    }
    // make sure metadataCache can map between topic name and id
    setupMetadataCacheWithTopicIds(topicIds, metadataCache)
  }

  private def setupMetadataCacheWithTopicIds(topicIds: Map[String, Uuid], metadataCache:MetadataCache): Unit = {
    val topicNames = topicIds.map(_.swap)
    topicNames.foreach {
      case (id, name) =>
        when(metadataCache.getTopicName(id)).thenReturn(Optional.of(name))
        when(metadataCache.getTopicId(name)).thenReturn(id)
    }
    when(metadataCache.topicIdsToNames()).thenReturn(topicNames.asJava)

    topicIds.foreach { case (topicName, topicId) =>
      when(metadataCache.getTopicId(topicName)).thenReturn(topicId)
    }
  }

  @AfterEach
  def tearDown(): Unit = {
    clearYammerMetricsExcept(metricsToBeDeletedInTheEnd)
    Option(quotaManager).foreach(_.shutdown())
    metrics.close()
    // validate that the shutdown is working correctly by ensuring no lingering threads.
    // assert at the very end otherwise the other tear down steps will not be performed
    assertNoNonDaemonThreadsWithWaiting(this.getClass.getName)
  }

  @Test
  def testHighWaterMarkDirectoryMapping(): Unit = {
    val mockLogMgr = TestUtils.createLogManager(config.logDirs.asScala.map(new File(_)))
    val rm = new ReplicaManager(
      metrics = metrics,
      config = config,
      time = time,
      scheduler = new MockScheduler(time),
      logManager = mockLogMgr,
      quotaManagers = quotaManager,
      metadataCache = new KRaftMetadataCache(config.brokerId, () => KRaftVersion.KRAFT_VERSION_0),
      logDirFailureChannel = new LogDirFailureChannel(config.logDirs.size),
      alterPartitionManager = alterPartitionManager)
    try {
      val partition = rm.createPartition(new TopicPartition(topic, 1))
      partition.createLogIfNotExists(isNew = false, isFutureReplica = false,
        new LazyOffsetCheckpoints(rm.highWatermarkCheckpoints.asJava), None)
      rm.checkpointHighWatermarks()
      config.logDirs.stream().map(s => Paths.get(s, ReplicaManager.HighWatermarkFilename))
        .forEach(checkpointFile => assertTrue(Files.exists(checkpointFile),
          s"checkpoint file does not exist at $checkpointFile"))
    } finally {
      rm.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testHighwaterMarkRelativeDirectoryMapping(): Unit = {
    val props = TestUtils.createBrokerConfig(1)
    props.put("log.dir", TestUtils.tempRelativeDir("data").getAbsolutePath)
    val config = KafkaConfig.fromProps(props)
    val mockLogMgr = TestUtils.createLogManager(config.logDirs.asScala.map(new File(_)))
    val rm = new ReplicaManager(
      metrics = metrics,
      config = config,
      time = time,
      scheduler = new MockScheduler(time),
      logManager = mockLogMgr,
      quotaManagers = quotaManager,
      metadataCache = new KRaftMetadataCache(config.brokerId, () => KRaftVersion.KRAFT_VERSION_0),
      logDirFailureChannel = new LogDirFailureChannel(config.logDirs.size),
      alterPartitionManager = alterPartitionManager)
    try {
      val partition = rm.createPartition(new TopicPartition(topic, 1))
      partition.createLogIfNotExists(isNew = false, isFutureReplica = false,
        new LazyOffsetCheckpoints(rm.highWatermarkCheckpoints.asJava), None)
      rm.checkpointHighWatermarks()
      config.logDirs.stream().map(s => Paths.get(s, ReplicaManager.HighWatermarkFilename))
        .forEach(checkpointFile => assertTrue(Files.exists(checkpointFile),
          s"checkpoint file does not exist at $checkpointFile"))
    } finally {
      rm.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testIllegalRequiredAcks(): Unit = {
    val mockLogMgr = TestUtils.createLogManager(config.logDirs.asScala.map(new File(_)))
    val rm = new ReplicaManager(
      metrics = metrics,
      config = config,
      time = time,
      scheduler = new MockScheduler(time),
      logManager = mockLogMgr,
      quotaManagers = quotaManager,
      metadataCache = new KRaftMetadataCache(config.brokerId, () => KRaftVersion.KRAFT_VERSION_0),
      logDirFailureChannel = new LogDirFailureChannel(config.logDirs.size),
      alterPartitionManager = alterPartitionManager)
    try {
      def callback(responseStatus: Map[TopicIdPartition, PartitionResponse]): Unit = {
        assert(responseStatus.values.head.error == Errors.INVALID_REQUIRED_ACKS)
      }
      rm.appendRecords(
        timeout = 0,
        requiredAcks = 3,
        internalTopicsAllowed = false,
        origin = AppendOrigin.CLIENT,
        entriesPerPartition = Map(new TopicIdPartition(Uuid.randomUuid(), 0, "test1") -> MemoryRecords.withRecords(Compression.NONE,
          new SimpleRecord("first message".getBytes))),
        responseCallback = callback)
    } finally {
      rm.shutdown(checkpointHW = false)
    }
  }

  private def mockGetAliveBrokerFunctions(cache: MetadataCache, aliveBrokers: Seq[Node]): Unit = {
    when(cache.hasAliveBroker(anyInt)).thenAnswer(new Answer[Boolean]() {
      override def answer(invocation: InvocationOnMock): Boolean = {
        aliveBrokers.map(_.id()).contains(invocation.getArgument(0).asInstanceOf[Int])
      }
    })
    when(cache.getAliveBrokerNode(anyInt, any[ListenerName])).
      thenAnswer(new Answer[Optional[Node]]() {
        override def answer(invocation: InvocationOnMock): Optional[Node] = {
          Optional.of(aliveBrokers.find(node => node.id == invocation.getArgument(0).asInstanceOf[Integer]).get)
        }
      })
    when(cache.getAliveBrokerNodes(any[ListenerName])).thenReturn(aliveBrokers.asJava)
  }

  @Test
  def testMaybeAddLogDirFetchers(): Unit = {
    val dir1 = TestUtils.tempDir()
    val dir2 = TestUtils.tempDir()
    val props = TestUtils.createBrokerConfig(0)
    props.put("log.dirs", dir1.getAbsolutePath + "," + dir2.getAbsolutePath)
    val config = KafkaConfig.fromProps(props)
    val logManager = TestUtils.createLogManager(config.logDirs.asScala.map(new File(_)), new LogConfig(new Properties()))
    mockGetAliveBrokerFunctions(metadataCache, Seq(new Node(0, "host0", 0)))
    when(metadataCache.metadataVersion()).thenReturn(MetadataVersion.MINIMUM_VERSION)
    val rm = new ReplicaManager(
      metrics = metrics,
      config = config,
      time = time,
      scheduler = new MockScheduler(time),
      logManager = logManager,
      quotaManagers = quotaManager,
      metadataCache = metadataCache,
      logDirFailureChannel = new LogDirFailureChannel(config.logDirs.size),
      alterPartitionManager = alterPartitionManager)

    try {
      val delta = topicsCreateDelta(0, isStartIdLeader = true, partitions = List(0), topicName = topic, topicId = topicIds(topic))
      val image = imageFromTopics(delta.apply())
      rm.applyDelta(delta, image)
      val partition = rm.getPartitionOrException(topicPartition)

      appendRecords(rm, topicPartition,
        MemoryRecords.withRecords(Compression.NONE, new SimpleRecord("first message".getBytes()), new SimpleRecord("second message".getBytes())))
      logManager.maybeUpdatePreferredLogDir(topicPartition, dir2.getAbsolutePath)

      partition.createLogIfNotExists(isNew = true, isFutureReplica = true,
        new LazyOffsetCheckpoints(rm.highWatermarkCheckpoints.asJava), None)

      // this method should use hw of future log to create log dir fetcher. Otherwise, it causes offset mismatch error
      rm.maybeAddLogDirFetchers(Set(partition), new LazyOffsetCheckpoints(rm.highWatermarkCheckpoints.asJava), _ => None)
      rm.replicaAlterLogDirsManager.fetcherThreadMap.values.foreach(t => t.fetchState(topicPartition).foreach(s => assertEquals(0L, s.fetchOffset)))
      // make sure alter log dir thread has processed the data
      rm.replicaAlterLogDirsManager.fetcherThreadMap.values.foreach(t => t.doWork())
      assertEquals(Set.empty, rm.replicaAlterLogDirsManager.failedPartitions.partitions())
      // the future log becomes the current log, so the partition state should get removed
      rm.replicaAlterLogDirsManager.fetcherThreadMap.values.foreach(t => assertEquals(None, t.fetchState(topicPartition)))
    } finally {
      rm.shutdown(checkpointHW = false)
    }
  }

  @ParameterizedTest(name = "testMaybeAddLogDirFetchersPausingCleaning with futureLogCreated: {0}")
  @ValueSource(booleans = Array(true, false))
  def testMaybeAddLogDirFetchersPausingCleaning(futureLogCreated: Boolean): Unit = {
    val dir1 = TestUtils.tempDir()
    val dir2 = TestUtils.tempDir()
    val props = TestUtils.createBrokerConfig(0)
    props.put("log.dirs", dir1.getAbsolutePath + "," + dir2.getAbsolutePath)
    val config = KafkaConfig.fromProps(props)
    val logManager = TestUtils.createLogManager(config.logDirs.asScala.map(new File(_)), new LogConfig(new Properties()))
    val spyLogManager = spy(logManager)
    val metadataCache: MetadataCache = mock(classOf[MetadataCache])
    mockGetAliveBrokerFunctions(metadataCache, Seq(new Node(0, "host0", 0)))
    when(metadataCache.metadataVersion()).thenReturn(MetadataVersion.MINIMUM_VERSION)
    val rm = new ReplicaManager(
      metrics = metrics,
      config = config,
      time = time,
      scheduler = new MockScheduler(time),
      logManager = spyLogManager,
      quotaManagers = quotaManager,
      metadataCache = metadataCache,
      logDirFailureChannel = new LogDirFailureChannel(config.logDirs.size),
      alterPartitionManager = alterPartitionManager)

    try {
      val delta = topicsCreateDelta(startId = 0, isStartIdLeader = true,
        partitions = List(0), topicName = topic, topicId = topicId)
      val image = imageFromTopics(delta.apply())
      rm.applyDelta(delta, image)
      val partition = rm.getPartitionOrException(topicPartition)

      spyLogManager.maybeUpdatePreferredLogDir(topicPartition, dir2.getAbsolutePath)

      if (futureLogCreated) {
        // create future log before maybeAddLogDirFetchers invoked
        partition.createLogIfNotExists(isNew = false, isFutureReplica = true,
          new LazyOffsetCheckpoints(rm.highWatermarkCheckpoints.asJava), None)
      } else {
        val mockLog = mock(classOf[UnifiedLog])
        when(spyLogManager.getLog(topicPartition, isFuture = true)).thenReturn(Option.apply(mockLog))
        when(mockLog.topicId).thenReturn(Optional.of(topicId))
        when(mockLog.parentDir).thenReturn(dir2.getAbsolutePath)
      }

      val topicIdMap: Map[String, Option[Uuid]] = Map(topic -> Option.apply(topicId))
      rm.maybeAddLogDirFetchers(Set(partition), new LazyOffsetCheckpoints(rm.highWatermarkCheckpoints.asJava), topicIdMap)
      if (futureLogCreated) {
        // since the futureLog is already created, we don't have to abort and pause the cleaning
        verify(spyLogManager, never).abortAndPauseCleaning(any[TopicPartition])
      } else {
        verify(spyLogManager, times(1)).abortAndPauseCleaning(any[TopicPartition])
      }
      rm.replicaAlterLogDirsManager.fetcherThreadMap.values.foreach(t => t.fetchState(new TopicPartition(topic, 0)).foreach(s => assertEquals(0L, s.fetchOffset)))
    } finally {
      rm.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testClearPurgatoryOnBecomingFollower(): Unit = {
    val props = TestUtils.createBrokerConfig(0)
    props.put("log.dir", TestUtils.tempRelativeDir("data").getAbsolutePath)
    val config = KafkaConfig.fromProps(props)
    val logProps = new Properties()
    val mockLogMgr = TestUtils.createLogManager(config.logDirs.asScala.map(new File(_)), new LogConfig(logProps))
    val aliveBrokers = Seq(new Node(0, "host0", 0), new Node(1, "host1", 1))
    mockGetAliveBrokerFunctions(metadataCache, aliveBrokers)
    when(metadataCache.metadataVersion()).thenReturn(MetadataVersion.MINIMUM_VERSION)
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

      val topicPartition = new TopicPartition(topic, 0)
      val partition = rm.createPartition(topicPartition)
      partition.createLogIfNotExists(isNew = false, isFutureReplica = false,
        new LazyOffsetCheckpoints(rm.highWatermarkCheckpoints.asJava), None)
      // Make this replica the leader.
      val delta = createLeaderDelta(topicId, topicPartition, brokerList.get(0), brokerList, brokerList)
      val leaderMetadataImage = imageFromTopics(delta.apply())
      rm.applyDelta(delta, leaderMetadataImage)
      rm.getPartitionOrException(new TopicPartition(topic, 0))
        .localLogOrException

      val records = MemoryRecords.withRecords(Compression.NONE, new SimpleRecord("first message".getBytes()))
      val appendResult = appendRecords(rm, new TopicPartition(topic, 0), records).onFire { response =>
        assertEquals(Errors.NOT_LEADER_OR_FOLLOWER, response.error)
      }

      // Make this replica the follower
      val delta1 = createLeaderDelta(topicId, topicPartition, brokerList.get(1), brokerList, brokerList, 1)
      val followerMetadataImage = imageFromTopics(delta1.apply())
      rm.applyDelta(delta1, followerMetadataImage)

      assertTrue(appendResult.hasFired)
    } finally {
      rm.shutdown(checkpointHW = false)
    }
  }

  @Test
  def checkRemoveMetricsCountMatchRegisterCount(): Unit = {
    val mockLogMgr = mock(classOf[LogManager])
    doReturn(Seq.empty, Seq.empty).when(mockLogMgr).liveLogDirs

    val mockMetricsGroupCtor = mockConstruction(classOf[KafkaMetricsGroup])
    try {
      val rm = new ReplicaManager(
        metrics = metrics,
        config = config,
        time = time,
        scheduler = new MockScheduler(time),
        logManager = mockLogMgr,
        quotaManagers = quotaManager,
        metadataCache = new KRaftMetadataCache(config.brokerId, () => KRaftVersion.KRAFT_VERSION_0),
        logDirFailureChannel = new LogDirFailureChannel(config.logDirs.size),
        alterPartitionManager = alterPartitionManager)

      // shutdown ReplicaManager so that metrics are removed
      rm.shutdown(checkpointHW = false)

      // Use the second instance of metrics group that is constructed. The first instance is constructed by
      // ReplicaManager constructor > BrokerTopicStats > BrokerTopicMetrics.
      val mockMetricsGroup = mockMetricsGroupCtor.constructed.get(1)
      ReplicaManager.GaugeMetricNames.foreach(metricName => verify(mockMetricsGroup).newGauge(ArgumentMatchers.eq(metricName), any()))
      ReplicaManager.MeterMetricNames.foreach(metricName => verify(mockMetricsGroup).newMeter(ArgumentMatchers.eq(metricName), anyString(), any(classOf[TimeUnit])))
      ReplicaManager.MetricNames.foreach(verify(mockMetricsGroup).removeMetric(_))

      // assert that we have verified all invocations on
      verifyNoMoreInteractions(mockMetricsGroup)
    } finally {
      if (mockMetricsGroupCtor != null) {
        mockMetricsGroupCtor.close()
      }
    }
  }

  @ParameterizedTest
  @ValueSource(ints = Array(0, 1, 10))
  def testFencedErrorCausedByBecomeLeader(loopEpochChange: Int): Unit = {
    val localId = 0
    val replicaManager = setupReplicaManagerWithMockedPurgatories(new MockTimer(time))
    try {
      val topicPartition = new TopicPartition(topic, 0)
      replicaManager.createPartition(topicPartition)
        .createLogIfNotExists(isNew = false, isFutureReplica = false,
          new LazyOffsetCheckpoints(replicaManager.highWatermarkCheckpoints.asJava), None)

      val leaderDelta = topicsCreateDelta(localId, isStartIdLeader = true, topicName = topic, topicId = topicIds(topic))
      val leaderImage = imageFromTopics(leaderDelta.apply())
      replicaManager.applyDelta(leaderDelta, leaderImage)

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
      (1 to loopEpochChange).foreach(
        epoch => {
          val leaderDelta = topicsCreateDelta(localId, isStartIdLeader = true, topicName = topic, topicId = topicIds(topic), leaderEpoch = epoch)
          replicaManager.applyDelta(leaderDelta, imageFromTopics(leaderDelta.apply()))
        }
      )
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
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testReceiveOutOfOrderSequenceExceptionWithLogStartOffset(): Unit = {
    val timer = new MockTimer(time)
    val replicaManager = setupReplicaManagerWithMockedPurgatories(timer)

    try {
      val brokerList = Seq[Integer](0, 1).asJava

      val tp = new TopicPartition(topic, 0)
      val partition = replicaManager.createPartition(tp)
      partition.createLogIfNotExists(isNew = false, isFutureReplica = false,
        new LazyOffsetCheckpoints(replicaManager.highWatermarkCheckpoints.asJava), None)

      // Make this replica the leader.
      val delta = createLeaderDelta(topicId, tp, 0, brokerList, brokerList)
      val leaderMetadataImage = imageFromTopics(delta.apply())

      replicaManager.applyDelta(delta, leaderMetadataImage)
      replicaManager.getPartitionOrException(new TopicPartition(topic, 0))
        .localLogOrException

      val producerId = 234L
      val epoch = 5.toShort

      // write a few batches as part of a transaction
      val numRecords = 3
      for (sequence <- 0 until numRecords) {
        val records = MemoryRecords.withIdempotentRecords(Compression.NONE, producerId, epoch, sequence,
          new SimpleRecord(s"message $sequence".getBytes))
        appendRecords(replicaManager, new TopicPartition(topic, 0), records).onFire { response =>
          assertEquals(Errors.NONE, response.error)
        }
      }

      assertEquals(0, partition.logStartOffset)

      // Append a record with an out of range sequence. We should get the OutOfOrderSequence error code with the log
      // start offset set.
      val outOfRangeSequence = numRecords + 10
      val record = MemoryRecords.withIdempotentRecords(Compression.NONE, producerId, epoch, outOfRangeSequence,
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
  def testProducerIdCountMetrics(): Unit = {
    val timer = new MockTimer(time)
    val replicaManager = setupReplicaManagerWithMockedPurgatories(timer)

    try {
      val brokerList = Seq[Integer](0, 1).asJava
      val tp0 = new TopicPartition(topic, 0)
      val tp1 = new TopicPartition(topic, 1)

      // Create a couple partition for the topic.
      val partition0 = replicaManager.createPartition(tp0)
      partition0.createLogIfNotExists(isNew = false, isFutureReplica = false,
        new LazyOffsetCheckpoints(replicaManager.highWatermarkCheckpoints.asJava), None)
      val partition1 = replicaManager.createPartition(tp1)
      partition1.createLogIfNotExists(isNew = false, isFutureReplica = false,
        new LazyOffsetCheckpoints(replicaManager.highWatermarkCheckpoints.asJava), None)

      // Make this replica the leader for the partitions.
      Seq(tp0, tp1).foreach { tp =>
        val delta = createLeaderDelta(
          topicId = topicId,
          partition = tp,
          leaderId = 0,
          replicas = brokerList,
          isr = brokerList
        )
        replicaManager.applyDelta(delta, imageFromTopics(delta.apply()))
        replicaManager.getPartitionOrException(tp)
      }

      def appendRecord(pid: Long, sequence: Int, partition: Int): Unit = {
        val epoch = 42.toShort
        val records = MemoryRecords.withIdempotentRecords(Compression.NONE, pid, epoch, sequence,
          new SimpleRecord(s"message $sequence".getBytes))
        appendRecords(replicaManager, new TopicPartition(topic, partition), records).onFire { response =>
          assertEquals(Errors.NONE, response.error)
        }
      }

      def replicaManagerMetricValue(): Int = {
        KafkaYammerMetrics.defaultRegistry().allMetrics().asScala.filter { case (metricName, _) =>
          metricName.getName == "ProducerIdCount" && metricName.getType == "ReplicaManager"
        }.head._2.asInstanceOf[Gauge[Int]].value
      }

      // Initially all metrics are 0.
      assertEquals(0, replicaManagerMetricValue())

      val pid1 = 123L
      // Produce a record from 1st pid to 1st partition.
      appendRecord(pid1, 0, 0)
      assertEquals(1, replicaManagerMetricValue())

      // Produce another record from 1st pid to 1st partition, metrics shouldn't change.
      appendRecord(pid1, 1, 0)
      assertEquals(1, replicaManagerMetricValue())

      // Produce a record from 2nd pid to 1st partition
      val pid2 = 456L
      appendRecord(pid2, 1, 0)
      assertEquals(2, replicaManagerMetricValue())

      // Produce a record from 1st pid to 2nd partition
      appendRecord(pid1, 0, 1)
      assertEquals(3, replicaManagerMetricValue())

      // Simulate producer id expiration.
      // We use -1 because the timestamp in this test is set to -1, so when
      // the expiration check subtracts timestamp, we get max value.
      partition0.removeExpiredProducers(Long.MaxValue - 1)
      assertEquals(1, replicaManagerMetricValue())
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testPartitionsWithLateTransactionsCount(): Unit = {
    val timer = new MockTimer(time)
    val replicaManager = setupReplicaManagerWithMockedPurgatories(timer)
    val topicPartition = new TopicPartition(topic, 0)
    setupMetadataCacheWithTopicIds(topicIds, replicaManager.metadataCache)

    def assertLateTransactionCount(expectedCount: Option[Int]): Unit = {
      assertEquals(expectedCount, yammerGaugeValue[Int]("PartitionsWithLateTransactionsCount"))
    }

    try {
      assertLateTransactionCount(Some(0))

      val partition = replicaManager.createPartition(topicPartition)
      partition.createLogIfNotExists(isNew = false, isFutureReplica = false,
        new LazyOffsetCheckpoints(replicaManager.highWatermarkCheckpoints.asJava), None)

      // Make this replica the leader.
      val brokerList = Seq[Integer](0, 1, 2).asJava
      val leaderDelta = createLeaderDelta(
        topicId = topicId,
        partition = topicPartition,
        leaderId = 0,
        replicas = brokerList,
        isr = brokerList,
      )
      replicaManager.applyDelta(leaderDelta, imageFromTopics(leaderDelta.apply()))

      // Start a transaction
      val producerId = 234L
      val epoch = 5.toShort
      val sequence = 9
      val records = MemoryRecords.withTransactionalRecords(Compression.NONE, producerId, epoch, sequence,
        new SimpleRecord(time.milliseconds(), s"message $sequence".getBytes))
      handleProduceAppend(replicaManager, new TopicPartition(topic, 0), records, transactionalId = transactionalId).onFire { response =>
        assertEquals(Errors.NONE, response.error)
      }
      assertLateTransactionCount(Some(0))

      // The transaction becomes late if not finished before the max transaction timeout passes
      time.sleep(replicaManager.logManager.maxTransactionTimeoutMs + ProducerStateManager.LATE_TRANSACTION_BUFFER_MS)
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
    setupMetadataCacheWithTopicIds(topicIds, replicaManager.metadataCache)

    try {
      val brokerList = Seq[Integer](0, 1).asJava

      val partition = replicaManager.createPartition(new TopicPartition(topic, 0))
      partition.createLogIfNotExists(isNew = false, isFutureReplica = false,
        new LazyOffsetCheckpoints(replicaManager.highWatermarkCheckpoints.asJava), None)

      // Make this replica the leader.
      val delta = createLeaderDelta(topicId, new TopicPartition(topic, 0), 0, brokerList, brokerList)
      val leaderMetadataImage = imageFromTopics(delta.apply())

      replicaManager.applyDelta(delta, leaderMetadataImage)
      replicaManager.getPartitionOrException(new TopicPartition(topic, 0))
        .localLogOrException

      val producerId = 234L
      val epoch = 5.toShort

      // write a few batches as part of a transaction
      val numRecords = 3
      for (sequence <- 0 until numRecords) {
        val records = MemoryRecords.withTransactionalRecords(Compression.NONE, producerId, epoch, sequence,
          new SimpleRecord(s"message $sequence".getBytes))
        handleProduceAppend(replicaManager, new TopicPartition(topic, 0), records, transactionalId = transactionalId).onFire { response =>
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
      assertEquals(OptionalLong.of(0), fetchData.lastStableOffset)
      assertEquals(Optional.of(Collections.emptyList()), fetchData.abortedTransactions)

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
      assertEquals(OptionalLong.of(0), fetchData.lastStableOffset)
      assertEquals(Optional.of(Collections.emptyList()), fetchData.abortedTransactions)

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
      assertEquals(OptionalLong.of(numRecords + 1), fetchData.lastStableOffset)
      assertEquals(Optional.of(Collections.emptyList()), fetchData.abortedTransactions)
      assertEquals(numRecords + 1, fetchData.records.batches.asScala.size)
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testDelayedFetchIncludesAbortedTransactions(): Unit = {
    val timer = new MockTimer(time)
    val replicaManager = setupReplicaManagerWithMockedPurgatories(timer)
    setupMetadataCacheWithTopicIds(topicIds, replicaManager.metadataCache)

    try {
      val brokerList = Seq[Integer](0, 1).asJava
      val partition = replicaManager.createPartition(new TopicPartition(topic, 0))
      partition.createLogIfNotExists(isNew = false, isFutureReplica = false,
        new LazyOffsetCheckpoints(replicaManager.highWatermarkCheckpoints.asJava), None)

      // Make this replica the leader.
      val delta = topicsCreateDelta(brokerList.get(0), isStartIdLeader = true, partitions = List(0), List.empty, topic, topicIds(topic))
      val leaderMetadataImage = imageFromTopics(delta.apply())
      replicaManager.applyDelta(delta, leaderMetadataImage)
      replicaManager.getPartitionOrException(new TopicPartition(topic, 0))
        .localLogOrException

      val producerId = 234L
      val epoch = 5.toShort

      // write a few batches as part of a transaction
      val numRecords = 3
      for (sequence <- 0 until numRecords) {
        val records = MemoryRecords.withTransactionalRecords(Compression.NONE, producerId, epoch, sequence,
          new SimpleRecord(s"message $sequence".getBytes))
        handleProduceAppend(replicaManager, new TopicPartition(topic, 0), records, transactionalId = transactionalId).onFire { response =>
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
      assertEquals(OptionalLong.of(numRecords + 1), fetchData.lastStableOffset)
      assertEquals(numRecords + 1, fetchData.records.records.asScala.size)
      assertTrue(fetchData.abortedTransactions.isPresent)
      assertEquals(1, fetchData.abortedTransactions.get.size)

      val abortedTransaction = fetchData.abortedTransactions.get.get(0)
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

      val tp = new TopicPartition(topic, 0)
      val partition = rm.createPartition(tp)
      partition.createLogIfNotExists(isNew = false, isFutureReplica = false,
        new LazyOffsetCheckpoints(rm.highWatermarkCheckpoints.asJava), None)

      // Make this replica the leader.
      val leaderDelta = createLeaderDelta(topicId, tp, leaderId = 0, replicas = brokerList, isr = brokerList)
      val leaderMetadataImage = imageFromTopics(leaderDelta.apply())
      rm.applyDelta(leaderDelta, leaderMetadataImage)

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
    val localId = 0
    val maxFetchBytes = 1024 * 1024
    val aliveBrokersIds = Seq(0, 1)
    val leaderEpoch = 5
    val replicaManager = setupReplicaManagerWithMockedPurgatories(new MockTimer(time),
      brokerId = 0, aliveBrokersIds)
    try {
      val tp = new TopicPartition(topic, 0)
      val tidp = new TopicIdPartition(topicId, tp)

      // Broker 0 becomes leader of the partition
      val leaderDelta = topicsCreateDelta(localId, isStartIdLeader = true, topicName = topic, topicId = topicIds(topic), leaderEpoch = leaderEpoch)
      val leaderImage = imageFromTopics(leaderDelta.apply())
      replicaManager.applyDelta(leaderDelta, leaderImage)

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
      assertTrue(divergingEpochResult.assertFired.divergingEpoch.isPresent)
      assertEquals(0L, followerReplica.stateSnapshot.logStartOffset)
      assertEquals(0L, followerReplica.stateSnapshot.logEndOffset)
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testFetchMessagesWithInconsistentTopicId(): Unit = {
    val localId = 0
    val maxFetchBytes = 1024 * 1024
    val aliveBrokersIds = Seq(0, 1)
    val leaderEpoch = 5
    val replicaManager = setupReplicaManagerWithMockedPurgatories(new MockTimer(time),
      brokerId = 0, aliveBrokersIds)
    try {
      val tp = new TopicPartition(topic, 0)
      val tidp = new TopicIdPartition(topicId, tp)

      // Broker 0 becomes leader of the partition
      val leaderDelta = topicsCreateDelta(localId, isStartIdLeader = true, topicName = topic, topicId = topicIds(topic), leaderEpoch = leaderEpoch)
      val leaderImage = imageFromTopics(leaderDelta.apply())
      replicaManager.applyDelta(leaderDelta, leaderImage)

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
      val leaderEpoch = 0
      // Create 2 partitions, assign replica 0 as the leader for both a different follower (1 and 2) for each
      val tp0 = new TopicPartition(topic, 0)
      val tp1 = new TopicPartition(topic, 1)
      val tidp0 = new TopicIdPartition(topicId, tp0)
      val tidp1 = new TopicIdPartition(topicId, tp1)
      val offsetCheckpoints = new LazyOffsetCheckpoints(replicaManager.highWatermarkCheckpoints.asJava)
      replicaManager.createPartition(tp0).createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints, None)
      replicaManager.createPartition(tp1).createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints, None)
      val partition0Replicas = Seq[Integer](0, 1).asJava
      val partition1Replicas = Seq[Integer](0, 2).asJava

      val leaderDelta0 = createLeaderDelta(topicIds(topic), tp0, 0, partition0Replicas, partition0Replicas)
      val leaderMetadataImage0 = imageFromTopics(leaderDelta0.apply())
      replicaManager.applyDelta(leaderDelta0, leaderMetadataImage0)

      val leaderDelta1 = createLeaderDelta(topicIds(topic), tp1, 0, partition1Replicas, partition1Replicas)
      val leaderMetadataImage1 = imageFromTopics(leaderDelta1.apply())
      replicaManager.applyDelta(leaderDelta1, leaderMetadataImage1)

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
        assertEquals(OptionalLong.of(0), tp0Status.get.lastStableOffset)
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

  /**
   * If a partition becomes a follower and the leader is unchanged it should check for truncation
   * if the epoch has increased by more than one (which suggests it has missed an update). For
   * IBP version 2.7 onwards, we don't require this since we can truncate at any time based
   * on diverging epochs returned in fetch responses.
   * This test assumes IBP >= 2.7 behavior, so `expectTruncation` is set to false and truncation is not expected.
   */
  @Test
  def testBecomeFollowerWhenLeaderIsUnchangedButMissedLeaderUpdate(): Unit = {
    val extraProps = new Properties
    val followerBrokerId = 0
    val leaderBrokerId = 1
    var leaderEpoch = 1
    val leaderEpochIncrement = 2
    val countDownLatch = new CountDownLatch(1)
    val offsetFromLeader = 5
    // Prepare the mocked components for the test
    val (replicaManager, mockLogMgr) = prepareReplicaManagerAndLogManager(new MockTimer(time),
      topicPartition.partition(), leaderEpoch + leaderEpochIncrement, followerBrokerId, leaderBrokerId, countDownLatch,
      expectTruncation = false, localLogOffset = Optional.of(10), offsetFromLeader = offsetFromLeader, extraProps = extraProps, topicId = Optional.of(topicId))

    try {
      // Initialize partition state to follower, with leader = 1, leaderEpoch = 1
      val partition = replicaManager.createPartition(topicPartition)
      val offsetCheckpoints = new LazyOffsetCheckpoints(replicaManager.highWatermarkCheckpoints.asJava)
      partition.createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints, None)
      val followerDelta = topicsCreateDelta(startId = followerBrokerId, isStartIdLeader = false, partitions = List(topicPartition.partition()), List.empty, topic, topicIds(topic), leaderEpoch)
      replicaManager.applyDelta(followerDelta, imageFromTopics(followerDelta.apply()))

      // Verify log created and partition is hosted
      val localLog = replicaManager.localLog(topicPartition)
      assertTrue(localLog.isDefined, "Log should be created for follower after applyDelta")
      val hostedPartition = replicaManager.getPartition(topicPartition)
      assertTrue(hostedPartition.isInstanceOf[HostedPartition.Online])

      // Make local partition a follower - because epoch increased by more than 1, truncation should
      // trigger even though leader does not change
      leaderEpoch += leaderEpochIncrement
      val epochJumpDelta = topicsCreateDelta(startId = followerBrokerId, isStartIdLeader = false, partitions = List(topicPartition.partition()), List.empty, topic, topicIds(topic), leaderEpoch)
      replicaManager.applyDelta(epochJumpDelta, imageFromTopics(epochJumpDelta.apply()))

      assertTrue(countDownLatch.await(1000L, TimeUnit.MILLISECONDS))


      verify(mockLogMgr).finishedInitializingLog(ArgumentMatchers.eq(topicPartition), any())
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testReplicaSelector(): Unit = {
    val topicPartition = 0
    val followerBrokerId = 0
    val leaderBrokerId = 1
    val leaderEpoch = 1
    val leaderEpochIncrement = 2
    val aliveBrokerIds = Array(followerBrokerId, leaderBrokerId)
    val countDownLatch = new CountDownLatch(1)

    // Prepare the mocked components for the test
    val (replicaManager, _) = prepareReplicaManagerAndLogManager(new MockTimer(time),
      topicPartition, leaderEpoch + leaderEpochIncrement, followerBrokerId,
      leaderBrokerId, countDownLatch, expectTruncation = true)

    try {
      val tp = new TopicPartition(topic, topicPartition)
      val partition = replicaManager.createPartition(tp)

      val offsetCheckpoints = new LazyOffsetCheckpoints(replicaManager.highWatermarkCheckpoints.asJava)
      partition.createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints, None)
      partition.makeLeader(partitionRegistration(leaderBrokerId, leaderEpoch, aliveBrokerIds, partitionEpoch, aliveBrokerIds),
        isNew = false,
        offsetCheckpoints,
        None)

      val metadata: ClientMetadata = new DefaultClientMetadata("rack-a", "client-id",
        InetAddress.getByName("localhost"), KafkaPrincipal.ANONYMOUS, "default")

      // We expect to select the leader, which means we return None
      val preferredReadReplica: Option[Int] = replicaManager.findPreferredReadReplica(
        partition, metadata, FetchRequest.ORDINARY_CONSUMER_ID, 1L, System.currentTimeMillis)
      assertFalse(preferredReadReplica.isDefined)
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
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
    val (replicaManager, _) = prepareReplicaManagerAndLogManager(new MockTimer(time),
      topicPartition, leaderEpoch + leaderEpochIncrement, followerBrokerId,
      leaderBrokerId, countDownLatch, expectTruncation = true, topicId = Optional.of(topicId))

    try {

      val tp0 = new TopicPartition(topic, 0)
      val tidp0 = new TopicIdPartition(topicId, tp0)

      // Make this replica the follower
      val followerDelta = createFollowerDelta(
        topicId = topicId,
        partition = tp0,
        followerId = 0,
        leaderId = 1,
        leaderEpoch = 1,
      )
      replicaManager.applyDelta(followerDelta, imageFromTopics(followerDelta.apply()))

      val metadata: ClientMetadata = new DefaultClientMetadata("rack-a", "client-id",
        InetAddress.getByName("localhost"), KafkaPrincipal.ANONYMOUS, "default")

      val consumerResult = fetchPartitionAsConsumer(replicaManager, tidp0,
        new PartitionData(Uuid.ZERO_UUID, 0, 0, 100000, Optional.empty()),
        clientMetadata = Some(metadata))

      // Fetch from follower succeeds
      assertTrue(consumerResult.hasFired)

      // But only leader will compute preferred replica
      assertTrue(!consumerResult.assertFired.preferredReadReplica.isPresent)
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
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
    val (replicaManager, _) = prepareReplicaManagerAndLogManager(new MockTimer(time),
      topicPartition, leaderEpoch + leaderEpochIncrement, followerBrokerId,
      leaderBrokerId, countDownLatch, expectTruncation = true, topicId = Optional.of(topicId))

    try {
      val brokerList = Seq[Integer](0, 1).asJava

      val tp0 = new TopicPartition(topic, 0)
      val tidp0 = new TopicIdPartition(topicId, tp0)

      val partition = replicaManager.createPartition(tp0)
      partition.createLogIfNotExists(isNew = false, isFutureReplica = false,
        new LazyOffsetCheckpoints(replicaManager.highWatermarkCheckpoints.asJava), None)
      // Make this replica the leader
      val leaderDelta = createLeaderDelta(
        topicId = topicId,
        partition = tp0,
        leaderId = 0,
        replicas = brokerList,
        isr = brokerList,
        leaderEpoch = 1
      )
      replicaManager.applyDelta(leaderDelta, imageFromTopics(leaderDelta.apply()))

      val metadata = new DefaultClientMetadata("rack-a", "client-id",
        InetAddress.getByName("localhost"), KafkaPrincipal.ANONYMOUS, "default")

      val consumerResult = fetchPartitionAsConsumer(replicaManager, tidp0,
        new PartitionData(Uuid.ZERO_UUID, 0, 0, 100000, Optional.empty()),
        clientMetadata = Some(metadata))

      // Fetch from leader succeeds
      assertTrue(consumerResult.hasFired)

      // Returns a preferred replica (should just be the leader, which is None)
      assertFalse(consumerResult.assertFired.preferredReadReplica.isPresent)
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testPreferredReplicaAsLeaderWhenSameRackFollowerIsOutOfIsr(): Unit = {
    val replicaManager = setupReplicaManagerWithMockedPurgatories(new MockTimer(time),
      propsModifier = props => props.put(ReplicationConfigs.REPLICA_SELECTOR_CLASS_CONFIG, classOf[MockReplicaSelector].getName))

    try {
      val leaderBrokerId = 0
      val followerBrokerId = 1
      val leaderNode = new Node(leaderBrokerId, "host1", 0, "rack-a")
      val followerNode = new Node(followerBrokerId, "host2", 1, "rack-b")
      val brokerList = Seq[Integer](leaderBrokerId, followerBrokerId).asJava
      val tp0 = new TopicPartition(topic, 0)
      val tidp0 = new TopicIdPartition(topicId, tp0)

      when(replicaManager.metadataCache.getPartitionReplicaEndpoints(
        tp0,
        new ListenerName("default")
      )).thenReturn(util.Map.of(
        leaderBrokerId, leaderNode,
        followerBrokerId, followerNode
      ))

      // Make this replica the leader and remove follower from ISR.
      val leaderDelta = createLeaderDelta(
        topicId = topicId,
        partition = tp0,
        leaderId = leaderBrokerId,
        replicas = brokerList,
        isr = util.Arrays.asList(leaderBrokerId),
        leaderEpoch = 1
      )
      replicaManager.applyDelta(leaderDelta, imageFromTopics(leaderDelta.apply()))

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
      val partitionView = replicaManager.replicaSelectorPlugin.get.get
        .asInstanceOf[MockReplicaSelector].getPartitionViewArgument

      assertTrue(partitionView.isDefined)
      assertEquals(expectedReplicaViews.asJava, partitionView.get.replicas)
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testFetchFromFollowerShouldNotRunPreferLeaderSelect(): Unit = {
    val replicaManager = setupReplicaManagerWithMockedPurgatories(new MockTimer(time),
      propsModifier = props => props.put(ReplicationConfigs.REPLICA_SELECTOR_CLASS_CONFIG, classOf[MockReplicaSelector].getName))
    try {
      val tp0 = new TopicPartition(topic, 0)
      val tidp0 = new TopicIdPartition(topicId, tp0)

      // Make this replica the follower
      val followerDelta = createFollowerDelta(topicId, tp0, 0, 1, 1)
      val followerMetadataImage = imageFromTopics(followerDelta.apply())
      replicaManager.applyDelta(followerDelta, followerMetadataImage)

      val metadata = new DefaultClientMetadata("rack-a", "client-id",
        InetAddress.getLocalHost, KafkaPrincipal.ANONYMOUS, "default")

      val consumerResult = fetchPartitionAsConsumer(replicaManager, tidp0,
        new PartitionData(Uuid.ZERO_UUID, 0, 0, 100000,
          Optional.empty()), clientMetadata = Some(metadata))

      // Fetch from follower succeeds
      assertTrue(consumerResult.hasFired)

      // Expect not run the preferred read replica selection
      assertEquals(0, replicaManager.replicaSelectorPlugin.get.get.asInstanceOf[MockReplicaSelector].getSelectionCount)

      // Only leader will compute preferred replica
      assertTrue(!consumerResult.assertFired.preferredReadReplica.isPresent)

    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testFetchShouldReturnImmediatelyWhenPreferredReadReplicaIsDefined(): Unit = {
    val localId = 0
    val replicaManager = setupReplicaManagerWithMockedPurgatories(new MockTimer(time),
      propsModifier = props => props.put(ReplicationConfigs.REPLICA_SELECTOR_CLASS_CONFIG, "org.apache.kafka.common.replica.RackAwareReplicaSelector"))

    try {
      val leaderBrokerId = 0
      val followerBrokerId = 1
      val tp0 = new TopicPartition(topic, 0)
      val tidp0 = new TopicIdPartition(topicId, tp0)

      when(replicaManager.metadataCache.getPartitionReplicaEndpoints(
        tp0,
        new ListenerName("default")
      )).thenReturn(util.Map.of(
        leaderBrokerId, new Node(leaderBrokerId, "host1", 9092, "rack-a"),
        followerBrokerId, new Node(followerBrokerId, "host2", 9092, "rack-b")
      ))

      // Make this replica the leader
      val leaderEpoch = 1
      val leaderDelta = topicsCreateDelta(localId, isStartIdLeader = true, topicName = topic, topicId = topicIds(topic), leaderEpoch = leaderEpoch)
      val leaderImage = imageFromTopics(leaderDelta.apply())
      replicaManager.applyDelta(leaderDelta, leaderImage)

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
      assertEquals(0, replicaManager.delayedFetchPurgatory.watched())

      // Returns a preferred replica
      assertTrue(consumerResult.assertFired.preferredReadReplica.isPresent)
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
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
      leaderBrokerId, countDownLatch, expectTruncation = true, topicId = Optional.of(topicId))
    try {
      val tp0 = new TopicPartition(topic, 0)
      val tidp0 = new TopicIdPartition(topicId, tp0)

      val offsetCheckpoints = new LazyOffsetCheckpoints(replicaManager.highWatermarkCheckpoints.asJava)
      replicaManager.createPartition(tp0).createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints, None)
      // Make this replica the follower
      val followerDelta = createFollowerDelta(topicId, tp0, 1, 0, 1)
      val followerImage = imageFromTopics(followerDelta.apply())
      replicaManager.applyDelta(followerDelta, followerImage)

      val simpleRecords = Seq(new SimpleRecord("a".getBytes), new SimpleRecord("b".getBytes))
      val appendResult = appendRecords(replicaManager, tp0,
        MemoryRecords.withRecords(Compression.NONE, simpleRecords.toSeq: _*), AppendOrigin.CLIENT)

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
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
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
    props.put(ReplicationConfigs.REPLICA_SELECTOR_CLASS_CONFIG, "non-a-class")
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
    try {
      assertFalse(replicaManager.replicaSelectorPlugin.isDefined)
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testFetchFollowerNotAllowedForOlderClients(): Unit = {
    val replicaManager = setupReplicaManagerWithMockedPurgatories(new MockTimer(time), aliveBrokerIds = Seq(0, 1))

    try {
      val tp0 = new TopicPartition(topic, 0)
      val tidp0 = new TopicIdPartition(topicId, tp0)
      val offsetCheckpoints = new LazyOffsetCheckpoints(replicaManager.highWatermarkCheckpoints.asJava)
      replicaManager.createPartition(tp0).createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints, None)

      val followerDelta = createFollowerDelta(topicId, tp0, 0, 1)
      val followerImage = imageFromTopics(followerDelta.apply())
      replicaManager.applyDelta(followerDelta, followerImage)

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
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testFetchRequestRateMetrics(): Unit = {
    val localId = 0
    val mockTimer = new MockTimer(time)
    val replicaManager = setupReplicaManagerWithMockedPurgatories(mockTimer, aliveBrokerIds = Seq(0, 1))

    try {
      val tp0 = new TopicPartition(topic, 0)
      val tidp0 = new TopicIdPartition(topicId, tp0)
      val offsetCheckpoints = new LazyOffsetCheckpoints(replicaManager.highWatermarkCheckpoints.asJava)
      replicaManager.createPartition(tp0).createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints, None)

      val leaderDelta = topicsCreateDelta(localId, isStartIdLeader = true, topicName = topic, topicId = topicIds(topic), leaderEpoch = 1)
      val leaderImage = imageFromTopics(leaderDelta.apply())
      replicaManager.applyDelta(leaderDelta, leaderImage)

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
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testBecomeFollowerWhileOldClientFetchInPurgatory(): Unit = {
    val mockTimer = new MockTimer(time)
    val replicaManager = setupReplicaManagerWithMockedPurgatories(mockTimer, aliveBrokerIds = Seq(0, 1))

    try {
      val tp0 = new TopicPartition(topic, 0)
      val tidp0 = new TopicIdPartition(topicId, tp0)
      val offsetCheckpoints = new LazyOffsetCheckpoints(replicaManager.highWatermarkCheckpoints.asJava)
      replicaManager.createPartition(tp0).createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints, None)

      val leaderDelta = createLeaderDelta(topicId, tp0, leaderId = 0)
      val leaderMetadataImage = imageFromTopics(leaderDelta.apply())
      replicaManager.applyDelta(leaderDelta, leaderMetadataImage)

      val partitionData = new FetchRequest.PartitionData(Uuid.ZERO_UUID, 0L, 0L, 100,
        Optional.empty())
      val fetchResult = fetchPartitionAsConsumer(replicaManager, tidp0, partitionData, maxWaitMs = 10)
      assertFalse(fetchResult.hasFired)

      // Become a follower and ensure that the delayed fetch returns immediately
      val followerDelta = createFollowerDelta(topicId, tp0, followerId = 0, leaderId = 1, leaderEpoch = 2)
      val followerMetadataImage = imageFromTopics(followerDelta.apply())
      replicaManager.applyDelta(followerDelta, followerMetadataImage)
      assertEquals(Errors.NOT_LEADER_OR_FOLLOWER, fetchResult.assertFired.error)
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testBecomeFollowerWhileNewClientFetchInPurgatory(): Unit = {
    val mockTimer = new MockTimer(time)
    val replicaManager = setupReplicaManagerWithMockedPurgatories(mockTimer, aliveBrokerIds = Seq(0, 1))

    try {
      val tp0 = new TopicPartition(topic, 0)
      val tidp0 = new TopicIdPartition(topicId, tp0)
      val offsetCheckpoints = new LazyOffsetCheckpoints(replicaManager.highWatermarkCheckpoints.asJava)
      replicaManager.createPartition(tp0).createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints, None)
      val partition0Replicas = Seq[Integer](0, 1).asJava

      val leaderDelta = createLeaderDelta(topicId, tp0, leaderId = 0, leaderEpoch = 1, replicas = partition0Replicas, isr = partition0Replicas)
      val leaderMetadataImage = imageFromTopics(leaderDelta.apply())
      replicaManager.applyDelta(leaderDelta, leaderMetadataImage)

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
      val followerDelta = createFollowerDelta(topicId, tp0, followerId = 0, leaderId = 1, leaderEpoch = 2)
      val followerMetadataImage = imageFromTopics(followerDelta.apply())
      replicaManager.applyDelta(followerDelta, followerMetadataImage)
      assertEquals(Errors.FENCED_LEADER_EPOCH, fetchResult.assertFired.error)
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testFetchFromLeaderAlwaysAllowed(): Unit = {
    val localId = 0
    val replicaManager = setupReplicaManagerWithMockedPurgatories(new MockTimer(time), aliveBrokerIds = Seq(0, 1))

    try {
      val tp0 = new TopicPartition(topic, 0)
      val tidp0 = new TopicIdPartition(topicId, tp0)
      val offsetCheckpoints = new LazyOffsetCheckpoints(replicaManager.highWatermarkCheckpoints.asJava)
      replicaManager.createPartition(tp0).createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints, None)

      val leaderDelta = topicsCreateDelta(localId, isStartIdLeader = true, topicName = topic, topicId = topicIds(topic), leaderEpoch = 1)
      val leaderImage = imageFromTopics(leaderDelta.apply())
      replicaManager.applyDelta(leaderDelta, leaderImage)

      val clientMetadata = new DefaultClientMetadata("", "", null, KafkaPrincipal.ANONYMOUS, "")
      var partitionData = new FetchRequest.PartitionData(Uuid.ZERO_UUID, 0L, 0L, 100,
        Optional.of(1))
      var fetchResult = fetchPartitionAsConsumer(replicaManager, tidp0, partitionData, clientMetadata = Some(clientMetadata))
      assertEquals(Errors.NONE, fetchResult.assertFired.error)

      partitionData = new FetchRequest.PartitionData(Uuid.ZERO_UUID, 0L, 0L, 100,
        Optional.empty())
      fetchResult = fetchPartitionAsConsumer(replicaManager, tidp0, partitionData, clientMetadata = Some(clientMetadata))
      assertEquals(Errors.NONE, fetchResult.assertFired.error)
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testVerificationForTransactionalPartitionsOnly(): Unit = {
    val tp0 = new TopicPartition(topic, 0)
    val tp1 = new TopicPartition(topic, 1)
    val producerId = 24L
    val producerEpoch = 0.toShort
    val sequence = 0
    val addPartitionsToTxnManager = mock(classOf[AddPartitionsToTxnManager])
    val replicaManager = setUpReplicaManagerWithMockedAddPartitionsToTxnManager(addPartitionsToTxnManager, List(tp0, tp1))
    val brokerList = Seq[Integer](0, 1).asJava
    try {
      val leaderDelta0 = createLeaderDelta(topicId, tp0, leaderId = 1, replicas = brokerList, isr = brokerList)
      val leaderDelta1 = createLeaderDelta(topicId, tp1, leaderId = 1, replicas = brokerList, isr = brokerList)
      val image0 = imageFromTopics(leaderDelta0.apply())
      replicaManager.applyDelta(leaderDelta0, image0)

      val image1 = imageFromTopics(leaderDelta1.apply())
      replicaManager.applyDelta(leaderDelta1, image1)

      // If we supply no transactional ID and idempotent records, we do not verify.
      val idempotentRecords = MemoryRecords.withIdempotentRecords(Compression.NONE, producerId, producerEpoch, sequence,
        new SimpleRecord("message".getBytes))
      handleProduceAppend(replicaManager, tp0, idempotentRecords, transactionalId = null)
      verify(addPartitionsToTxnManager, times(0)).addOrVerifyTransaction(any(), any(), any(), any(), any[AddPartitionsToTxnManager.AppendCallback](), any())
      assertEquals(VerificationGuard.SENTINEL, getVerificationGuard(replicaManager, tp0, producerId))

      // If we supply a transactional ID and some transactional and some idempotent records, we should only verify the topic partition with transactional records.
      val transactionalRecords = MemoryRecords.withTransactionalRecords(Compression.NONE, producerId, producerEpoch, sequence + 1,
        new SimpleRecord("message".getBytes))

      val idempotentRecords2 = MemoryRecords.withIdempotentRecords(Compression.NONE, producerId, producerEpoch, sequence,
        new SimpleRecord("message".getBytes))
      handleProduceAppendToMultipleTopics(replicaManager, Map(tp0 -> transactionalRecords, tp1 -> idempotentRecords2), transactionalId)
      verify(addPartitionsToTxnManager, times(1)).addOrVerifyTransaction(
        ArgumentMatchers.eq(transactionalId),
        ArgumentMatchers.eq(producerId),
        ArgumentMatchers.eq(producerEpoch),
        ArgumentMatchers.eq(util.List.of(tp0)),
        any[AddPartitionsToTxnManager.AppendCallback](),
        any()
      )
      assertNotEquals(VerificationGuard.SENTINEL, getVerificationGuard(replicaManager, tp0, producerId))
      assertEquals(VerificationGuard.SENTINEL, getVerificationGuard(replicaManager, tp1, producerId))
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @ParameterizedTest
  @EnumSource(value = classOf[AppendOrigin], names = Array("CLIENT", "COORDINATOR"))
  def testTransactionVerificationFlow(appendOrigin: AppendOrigin): Unit = {
    val tp0 = new TopicPartition(topic, 0)
    val producerId = 24L
    val producerEpoch = 0.toShort
    val sequence = 6
    val addPartitionsToTxnManager = mock(classOf[AddPartitionsToTxnManager])
    val brokerList = Seq[Integer](0, 1).asJava

    val replicaManager = setUpReplicaManagerWithMockedAddPartitionsToTxnManager(addPartitionsToTxnManager, List(tp0))
    try {
      val leaderDelta = createLeaderDelta(topicId, tp0, leaderId = 1, replicas = brokerList, isr = brokerList)
      val leaderMetadataImage = imageFromTopics(leaderDelta.apply())
      replicaManager.applyDelta(leaderDelta, leaderMetadataImage)

      // Append some transactional records.
      val transactionalRecords = MemoryRecords.withTransactionalRecords(Compression.NONE, producerId, producerEpoch, sequence,
        new SimpleRecord("message".getBytes))

      // We should add these partitions to the manager to verify.
      val result = handleProduceAppend(replicaManager, tp0, transactionalRecords, origin = appendOrigin, transactionalId = transactionalId)
      val appendCallback = ArgumentCaptor.forClass(classOf[AddPartitionsToTxnManager.AppendCallback])
      verify(addPartitionsToTxnManager, times(1)).addOrVerifyTransaction(
        ArgumentMatchers.eq(transactionalId),
        ArgumentMatchers.eq(producerId),
        ArgumentMatchers.eq(producerEpoch),
        ArgumentMatchers.eq(util.List.of(tp0)),
        appendCallback.capture(),
        any()
      )
      val verificationGuard = getVerificationGuard(replicaManager, tp0, producerId)
      assertEquals(verificationGuard, getVerificationGuard(replicaManager, tp0, producerId))

      // Confirm we did not write to the log and instead returned error.
      val callback: AddPartitionsToTxnManager.AppendCallback = appendCallback.getValue
      callback.complete(util.Map.of(tp0, Errors.INVALID_TXN_STATE))
      assertEquals(Errors.INVALID_TXN_STATE, result.assertFired.error)
      assertEquals(verificationGuard, getVerificationGuard(replicaManager, tp0, producerId))

      // This time verification is successful.
      handleProduceAppend(replicaManager, tp0, transactionalRecords, origin = appendOrigin, transactionalId = transactionalId)
      val appendCallback2 = ArgumentCaptor.forClass(classOf[AddPartitionsToTxnManager.AppendCallback])
      verify(addPartitionsToTxnManager, times(2)).addOrVerifyTransaction(
        ArgumentMatchers.eq(transactionalId),
        ArgumentMatchers.eq(producerId),
        ArgumentMatchers.eq(producerEpoch),
        ArgumentMatchers.eq(util.List.of(tp0)),
        appendCallback2.capture(),
        any()
      )
      assertEquals(verificationGuard, getVerificationGuard(replicaManager, tp0, producerId))

      val callback2: AddPartitionsToTxnManager.AppendCallback = appendCallback2.getValue
      callback2.complete(util.Map.of())
      assertEquals(VerificationGuard.SENTINEL, getVerificationGuard(replicaManager, tp0, producerId))
      assertTrue(replicaManager.localLog(tp0).get.hasOngoingTransaction(producerId, producerEpoch))
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @ParameterizedTest
  @EnumSource(
    value = classOf[Errors],
    names = Array(
      "NOT_COORDINATOR",
      "CONCURRENT_TRANSACTIONS"
    )
  )
  def testTransactionAddPartitionRetry(error: Errors): Unit = {
    val tp0 = new TopicPartition(topic, 0)
    val producerId = 24L
    val producerEpoch = 0.toShort
    val sequence = 0
    val addPartitionsToTxnManager = mock(classOf[AddPartitionsToTxnManager])
    val scheduler = new MockScheduler(time)
    val brokerList = Seq[Integer](0, 1).asJava

    val replicaManager = setUpReplicaManagerWithMockedAddPartitionsToTxnManager(addPartitionsToTxnManager, List(tp0), scheduler = scheduler)
    try {
      val leaderDelta = createLeaderDelta(topicId, tp0, leaderId = 1, replicas = brokerList, isr = brokerList)
      val leaderMetadataImage = imageFromTopics(leaderDelta.apply())
      replicaManager.applyDelta(leaderDelta, leaderMetadataImage)

      // Append some transactional records.
      val transactionalRecords = MemoryRecords.withTransactionalRecords(Compression.NONE, producerId, producerEpoch, sequence,
        new SimpleRecord("message".getBytes))

      // We should add these partitions to the manager to verify.
      val result = handleProduceAppend(replicaManager, tp0, transactionalRecords, origin = AppendOrigin.CLIENT,
        transactionalId = transactionalId, transactionSupportedOperation = ADD_PARTITION)
      val appendCallback = ArgumentCaptor.forClass(classOf[AddPartitionsToTxnManager.AppendCallback])
      verify(addPartitionsToTxnManager, times(1)).addOrVerifyTransaction(
        ArgumentMatchers.eq(transactionalId),
        ArgumentMatchers.eq(producerId),
        ArgumentMatchers.eq(producerEpoch),
        ArgumentMatchers.eq(util.List.of(tp0)),
        appendCallback.capture(),
        any()
      )
      val verificationGuard = getVerificationGuard(replicaManager, tp0, producerId)
      assertEquals(verificationGuard, getVerificationGuard(replicaManager, tp0, producerId))

      // Confirm we did not write to the log and instead returned error.
      var callback: AddPartitionsToTxnManager.AppendCallback = appendCallback.getValue
      callback.complete(util.Map.of(tp0, error))

      if (error != Errors.CONCURRENT_TRANSACTIONS) {
        // NOT_COORDINATOR is converted to NOT_ENOUGH_REPLICAS
        assertEquals(Errors.NOT_ENOUGH_REPLICAS, result.assertFired.error)
      } else {
        // The append should not finish with error, it should retry later.
        assertFalse(result.hasFired)
        assertEquals(verificationGuard, getVerificationGuard(replicaManager, tp0, producerId))

        time.sleep(new AddPartitionsToTxnConfig(config).addPartitionsToTxnRetryBackoffMs + 1)
        scheduler.tick()

        verify(addPartitionsToTxnManager, times(2)).addOrVerifyTransaction(
          ArgumentMatchers.eq(transactionalId),
          ArgumentMatchers.eq(producerId),
          ArgumentMatchers.eq(producerEpoch),
          ArgumentMatchers.eq(util.List.of(tp0)),
          appendCallback.capture(),
          any()
        )
        callback = appendCallback.getValue
        callback.complete(util.Map.of())
        assertEquals(VerificationGuard.SENTINEL, getVerificationGuard(replicaManager, tp0, producerId))
        assertTrue(replicaManager.localLog(tp0).get.hasOngoingTransaction(producerId, producerEpoch))
      }
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testTransactionVerificationBlocksOutOfOrderSequence(): Unit = {
    val tp0 = new TopicPartition(topic, 0)
    val producerId = 24L
    val producerEpoch = 0.toShort
    val sequence = 0
    val addPartitionsToTxnManager = mock(classOf[AddPartitionsToTxnManager])
    val brokerList = Seq[Integer](0, 1).asJava

    val replicaManager = setUpReplicaManagerWithMockedAddPartitionsToTxnManager(addPartitionsToTxnManager, List(tp0))
    try {
      val leaderDelta = createLeaderDelta(topicId, tp0, leaderId = 1, replicas = brokerList, isr = brokerList)
      val leaderMetadataImage = imageFromTopics(leaderDelta.apply())
      replicaManager.applyDelta(leaderDelta, leaderMetadataImage)

      // Start with sequence 0
      val transactionalRecords = MemoryRecords.withTransactionalRecords(Compression.NONE, producerId, producerEpoch, sequence,
        new SimpleRecord("message".getBytes))

      // We should add these partitions to the manager to verify.
      val result = handleProduceAppend(replicaManager, tp0, transactionalRecords, transactionalId = transactionalId)
      val appendCallback = ArgumentCaptor.forClass(classOf[AddPartitionsToTxnManager.AppendCallback])
      verify(addPartitionsToTxnManager, times(1)).addOrVerifyTransaction(
        ArgumentMatchers.eq(transactionalId),
        ArgumentMatchers.eq(producerId),
        ArgumentMatchers.eq(producerEpoch),
        ArgumentMatchers.eq(util.List.of(tp0)),
        appendCallback.capture(),
        any()
      )
      val verificationGuard = getVerificationGuard(replicaManager, tp0, producerId)
      assertEquals(verificationGuard, getVerificationGuard(replicaManager, tp0, producerId))

      // Confirm we did not write to the log and instead returned error.
      val callback: AddPartitionsToTxnManager.AppendCallback = appendCallback.getValue
      callback.complete(util.Map.of(tp0, Errors.INVALID_PRODUCER_ID_MAPPING))
      assertEquals(Errors.INVALID_PRODUCER_ID_MAPPING, result.assertFired.error)
      assertEquals(verificationGuard, getVerificationGuard(replicaManager, tp0, producerId))

      // Try to append a higher sequence (1) after the first one failed with a retriable error.
      val transactionalRecords2 = MemoryRecords.withTransactionalRecords(Compression.NONE, producerId, producerEpoch, sequence + 1,
        new SimpleRecord("message".getBytes))

      val result2 = handleProduceAppend(replicaManager, tp0, transactionalRecords2, transactionalId = transactionalId)
      val appendCallback2 = ArgumentCaptor.forClass(classOf[AddPartitionsToTxnManager.AppendCallback])
      verify(addPartitionsToTxnManager, times(2)).addOrVerifyTransaction(
        ArgumentMatchers.eq(transactionalId),
        ArgumentMatchers.eq(producerId),
        ArgumentMatchers.eq(producerEpoch),
        ArgumentMatchers.eq(util.List.of(tp0)),
        appendCallback2.capture(),
        any()
      )
      assertEquals(verificationGuard, getVerificationGuard(replicaManager, tp0, producerId))

      // Verification should succeed, but we expect to fail with OutOfOrderSequence and for the VerificationGuard to remain.
      val callback2: AddPartitionsToTxnManager.AppendCallback = appendCallback2.getValue
      callback2.complete(util.Map.of())
      assertEquals(verificationGuard, getVerificationGuard(replicaManager, tp0, producerId))
      assertEquals(Errors.OUT_OF_ORDER_SEQUENCE_NUMBER, result2.assertFired.error)
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testTransactionVerificationRejectsLowerProducerEpoch(): Unit = {
    val tp0               = new TopicPartition(topic, 0)
    val producerId        = 24L
    val producerEpoch     = 5.toShort
    val lowerProducerEpoch= 4.toShort
    val sequence          = 6
    val addPartitionsToTxnManager = mock(classOf[AddPartitionsToTxnManager])
    val brokerList = Seq[Integer](0, 1).asJava

    val replicaManager =
      setUpReplicaManagerWithMockedAddPartitionsToTxnManager(addPartitionsToTxnManager, List(tp0))

    try {
      val leaderDelta = createLeaderDelta(topicId, tp0, leaderId = 1, replicas = brokerList, isr = brokerList)
      val leaderMetadataImage = imageFromTopics(leaderDelta.apply())
      replicaManager.applyDelta(leaderDelta, leaderMetadataImage)

      // first append with epoch 5
      val transactionalRecords = MemoryRecords.withTransactionalRecords(
        Compression.NONE,
        producerId,
        producerEpoch,
        sequence,
        new SimpleRecord("message".getBytes)
      )

      handleProduceAppend(replicaManager, tp0, transactionalRecords, transactionalId = transactionalId)

      val appendCallback = ArgumentCaptor.forClass(classOf[AddPartitionsToTxnManager.AppendCallback])
      verify(addPartitionsToTxnManager, times(1)).addOrVerifyTransaction(
        ArgumentMatchers.eq(transactionalId),
        ArgumentMatchers.eq(producerId),
        ArgumentMatchers.eq(producerEpoch),
        ArgumentMatchers.eq(util.List.of(tp0)),
        appendCallback.capture(),
        any()
      )

      val verificationGuard = getVerificationGuard(replicaManager, tp0, producerId)
      assertEquals(verificationGuard, getVerificationGuard(replicaManager, tp0, producerId))

      // simulate successful verification
      val callback: AddPartitionsToTxnManager.AppendCallback = appendCallback.getValue
      callback.complete(util.Map.of())

      assertEquals(VerificationGuard.SENTINEL, getVerificationGuard(replicaManager, tp0, producerId))
      assertTrue(replicaManager.localLog(tp0).get.hasOngoingTransaction(producerId, producerEpoch))

      // append lower epoch 4
      val transactionalRecords2 = MemoryRecords.withTransactionalRecords(
        Compression.NONE,
        producerId,
        lowerProducerEpoch,
        sequence + 1,
        new SimpleRecord("message".getBytes)
      )

      val result2 = handleProduceAppend(replicaManager, tp0, transactionalRecords2, transactionalId = transactionalId)

      // no extra call to the txn‑manager should have been made
      verifyNoMoreInteractions(addPartitionsToTxnManager)

      // broker returns the fencing error
      assertEquals(Errors.INVALID_PRODUCER_EPOCH, result2.assertFired.error)
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testTransactionVerificationGuardOnMultiplePartitions(): Unit = {
    val localId = 0
    val mockTimer = new MockTimer(time)
    val tp0 = new TopicPartition(topic, 0)
    val tp1 = new TopicPartition(topic, 1)
    val producerId = 24L
    val producerEpoch = 0.toShort
    val sequence = 0

    val replicaManager = setupReplicaManagerWithMockedPurgatories(mockTimer)
    setupMetadataCacheWithTopicIds(topicIds, replicaManager.metadataCache)
    try {
      val leaderDelta = topicsCreateDelta(localId, isStartIdLeader = true, partitions = List(0, 1), List.empty, topic, topicIds(topic))
      val leaderImage = imageFromTopics(leaderDelta.apply())
      replicaManager.applyDelta(leaderDelta, leaderImage)

      val transactionalRecords = MemoryRecords.withTransactionalRecords(Compression.NONE, producerId, producerEpoch, sequence,
        new SimpleRecord(s"message $sequence".getBytes))

      handleProduceAppendToMultipleTopics(replicaManager, Map(tp0 -> transactionalRecords, tp1 -> transactionalRecords), transactionalId).onFire { responses =>
        responses.foreach {
          entry => assertEquals(Errors.NONE, entry._2.error)
        }
      }
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testExceptionWhenUnverifiedTransactionHasMultipleProducerIds(): Unit = {
    val localId = 1
    val tp0 = new TopicPartition(topic, 0)
    val tp1 = new TopicPartition(topic, 1)
    val transactionalId = "txn1"
    val producerId = 24L
    val producerEpoch = 0.toShort
    val sequence = 0

    val addPartitionsToTxnManager = mock(classOf[AddPartitionsToTxnManager])

    val replicaManager = setUpReplicaManagerWithMockedAddPartitionsToTxnManager(addPartitionsToTxnManager, List(tp0, tp1))

    try {
      val leaderDelta = topicsCreateDelta(localId, isStartIdLeader = true, partitions = List(0, 1), List.empty, topic, topicIds(topic))
      val leaderImage = imageFromTopics(leaderDelta.apply())
      replicaManager.applyDelta(leaderDelta, leaderImage)

      // Append some transactional records with different producer IDs
      val transactionalRecords = mutable.Map[TopicPartition, MemoryRecords]()
      transactionalRecords.put(tp0, MemoryRecords.withTransactionalRecords(Compression.NONE, producerId, producerEpoch, sequence,
        new SimpleRecord(s"message $sequence".getBytes)))
      transactionalRecords.put(tp1, MemoryRecords.withTransactionalRecords(Compression.NONE, producerId + 1, producerEpoch, sequence,
        new SimpleRecord(s"message $sequence".getBytes)))

      assertThrows(classOf[InvalidPidMappingException],
        () => handleProduceAppendToMultipleTopics(replicaManager, transactionalRecords, transactionalId = transactionalId))
      // We should not add these partitions to the manager to verify.
      verify(addPartitionsToTxnManager, times(0)).addOrVerifyTransaction(any(), any(), any(), any(), any(), any())
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testTransactionVerificationWhenNotLeader(): Unit = {
    val tp0 = new TopicPartition(topic, 0)
    val producerId = 24L
    val producerEpoch = 0.toShort
    val sequence = 0
    val addPartitionsToTxnManager = mock(classOf[AddPartitionsToTxnManager])

    val replicaManager = setUpReplicaManagerWithMockedAddPartitionsToTxnManager(addPartitionsToTxnManager, List(tp0))
    try {
      // Append some transactional records.
      val transactionalRecords = MemoryRecords.withTransactionalRecords(Compression.NONE, producerId, producerEpoch, sequence,
        new SimpleRecord("message".getBytes))

      // We should not add these partitions to the manager to verify, but instead throw an error.
      handleProduceAppend(replicaManager, tp0, transactionalRecords, transactionalId = transactionalId).onFire { response =>
        assertEquals(Errors.NOT_LEADER_OR_FOLLOWER, response.error)
      }
      verify(addPartitionsToTxnManager, times(0)).addOrVerifyTransaction(any(), any(), any(), any(), any(), any())
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testDisabledTransactionVerification(): Unit = {
    val props = TestUtils.createBrokerConfig(0)
    props.put("transaction.partition.verification.enable", "false")
    val config = KafkaConfig.fromProps(props)

    val tp = new TopicPartition(topic, 0)
    val transactionalId = "txn1"
    val producerId = 24L
    val producerEpoch = 0.toShort
    val sequence = 0

    val addPartitionsToTxnManager = mock(classOf[AddPartitionsToTxnManager])

    val replicaManager = setUpReplicaManagerWithMockedAddPartitionsToTxnManager(addPartitionsToTxnManager, List(tp), config = config)

    try {
      val delta = topicsCreateDelta(0, isStartIdLeader = true, partitions = List(0), List.empty, topic, topicIds(topic))
      val leaderMetadataImage = imageFromTopics(delta.apply())
      replicaManager.applyDelta(delta, leaderMetadataImage)

      val transactionalRecords = MemoryRecords.withTransactionalRecords(Compression.NONE, producerId, producerEpoch, sequence,
        new SimpleRecord(s"message $sequence".getBytes))
      handleProduceAppend(replicaManager, tp, transactionalRecords, transactionalId = transactionalId).onFire { response =>
        assertEquals(Errors.NONE, response.error)
      }
      assertEquals(VerificationGuard.SENTINEL, getVerificationGuard(replicaManager, tp, producerId))

      // We should not add these partitions to the manager to verify.
      verify(addPartitionsToTxnManager, times(0)).addOrVerifyTransaction(any(), any(), any(), any(), any(), any())

      // Dynamically enable verification.
      config.dynamicConfig.initialize(None)
      val props = new Properties()
      props.put(TransactionLogConfig.TRANSACTION_PARTITION_VERIFICATION_ENABLE_CONFIG, "true")
      config.dynamicConfig.updateBrokerConfig(config.brokerId, props)
      val transactionLogConfig = new TransactionLogConfig(config)
      TestUtils.waitUntilTrue(() => transactionLogConfig.transactionPartitionVerificationEnable, "Config did not dynamically update.")

      // Try to append more records. We don't need to send a request since the transaction is already ongoing.
      val moreTransactionalRecords = MemoryRecords.withTransactionalRecords(Compression.NONE, producerId, producerEpoch, sequence + 1,
        new SimpleRecord("message".getBytes))

      handleProduceAppend(replicaManager, tp, moreTransactionalRecords, transactionalId = transactionalId)
      verify(addPartitionsToTxnManager, times(0)).addOrVerifyTransaction(any(), any(), any(), any(), any(), any())
      assertEquals(VerificationGuard.SENTINEL, getVerificationGuard(replicaManager, tp, producerId))
      assertTrue(replicaManager.localLog(tp).get.hasOngoingTransaction(producerId, producerEpoch))
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testTransactionVerificationDynamicDisablement(): Unit = {
    val tp0 = new TopicPartition(topic, 0)
    val producerId = 24L
    val producerEpoch = 0.toShort
    val sequence = 6
    val addPartitionsToTxnManager = mock(classOf[AddPartitionsToTxnManager])
    val brokerList = Seq[Integer](0, 1).asJava

    val replicaManager = setUpReplicaManagerWithMockedAddPartitionsToTxnManager(addPartitionsToTxnManager, List(tp0))
    try {
      val leaderDelta = createLeaderDelta(topicId, tp0, leaderId = 1, replicas = brokerList, isr = brokerList)
      val leaderMetadataImage = imageFromTopics(leaderDelta.apply())
      replicaManager.applyDelta(leaderDelta, leaderMetadataImage)

      // Append some transactional records.
      val transactionalRecords = MemoryRecords.withTransactionalRecords(Compression.NONE, producerId, producerEpoch, sequence,
        new SimpleRecord("message".getBytes))

      // We should add these partitions to the manager to verify.
      val result = handleProduceAppend(replicaManager, tp0, transactionalRecords, transactionalId = transactionalId)
      val appendCallback = ArgumentCaptor.forClass(classOf[AddPartitionsToTxnManager.AppendCallback])
      verify(addPartitionsToTxnManager, times(1)).addOrVerifyTransaction(
        ArgumentMatchers.eq(transactionalId),
        ArgumentMatchers.eq(producerId),
        ArgumentMatchers.eq(producerEpoch),
        ArgumentMatchers.eq(util.List.of(tp0)),
        appendCallback.capture(),
        any()
      )
      val verificationGuard = getVerificationGuard(replicaManager, tp0, producerId)
      assertEquals(verificationGuard, getVerificationGuard(replicaManager, tp0, producerId))

      // Disable verification
      config.dynamicConfig.initialize(None)
      val props = new Properties()
      props.put(TransactionLogConfig.TRANSACTION_PARTITION_VERIFICATION_ENABLE_CONFIG, "false")
      config.dynamicConfig.updateBrokerConfig(config.brokerId, props)
      val transactionLogConfig = new TransactionLogConfig(config)
      TestUtils.waitUntilTrue(() => !transactionLogConfig.transactionPartitionVerificationEnable, "Config did not dynamically update.")

      // Confirm we did not write to the log and instead returned error.
      val callback: AddPartitionsToTxnManager.AppendCallback = appendCallback.getValue
      callback.complete(util.Map.of(tp0, Errors.INVALID_TXN_STATE))
      assertEquals(Errors.INVALID_TXN_STATE, result.assertFired.error)
      assertEquals(verificationGuard, getVerificationGuard(replicaManager, tp0, producerId))

      // This time we do not verify
      handleProduceAppend(replicaManager, tp0, transactionalRecords, transactionalId = transactionalId)
      verify(addPartitionsToTxnManager, times(1)).addOrVerifyTransaction(any(), any(), any(), any(), any(), any())
      assertEquals(VerificationGuard.SENTINEL, getVerificationGuard(replicaManager, tp0, producerId))
      assertTrue(replicaManager.localLog(tp0).get.hasOngoingTransaction(producerId, producerEpoch))
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @ParameterizedTest
  @EnumSource(
    value = classOf[Errors],
    names = Array(
      "NOT_COORDINATOR",
      "NETWORK_EXCEPTION",
      "COORDINATOR_LOAD_IN_PROGRESS",
      "COORDINATOR_NOT_AVAILABLE"
    )
  )
  def testVerificationErrorConversionsTV2(error: Errors): Unit = {
    val localId = 1
    val tp0 = new TopicPartition(topic, 0)
    val producerId = 24L
    val producerEpoch = 0.toShort
    val sequence = 0
    val addPartitionsToTxnManager = mock(classOf[AddPartitionsToTxnManager])

    val replicaManager = setUpReplicaManagerWithMockedAddPartitionsToTxnManager(addPartitionsToTxnManager, List(tp0))
    try {
      val leaderDelta = topicsCreateDelta(localId, isStartIdLeader = true, partitions = List(0, 1), List.empty, topic, topicIds(topic))
      replicaManager.applyDelta(leaderDelta, imageFromTopics(leaderDelta.apply()))

      val transactionalRecords = MemoryRecords.withTransactionalRecords(Compression.NONE, producerId, producerEpoch, sequence,
        new SimpleRecord("message".getBytes))

      // Start verification and return the coordinator related errors.
      val expectedMessage = s"Unable to verify the partition has been added to the transaction. Underlying error: ${error.toString}"
      val result = handleProduceAppend(replicaManager, tp0, transactionalRecords, transactionalId = transactionalId, transactionSupportedOperation = ADD_PARTITION)
      val appendCallback = ArgumentCaptor.forClass(classOf[AddPartitionsToTxnManager.AppendCallback])
      verify(addPartitionsToTxnManager, times(1)).addOrVerifyTransaction(
        ArgumentMatchers.eq(transactionalId),
        ArgumentMatchers.eq(producerId),
        ArgumentMatchers.eq(producerEpoch),
        ArgumentMatchers.eq(util.List.of(tp0)),
        appendCallback.capture(),
        any()
      )

      // Confirm we did not write to the log and instead returned the converted error with the correct error message.
      val callback: AddPartitionsToTxnManager.AppendCallback = appendCallback.getValue
      callback.complete(util.Map.of(tp0, error))
      assertEquals(Errors.NOT_ENOUGH_REPLICAS, result.assertFired.error)
      assertEquals(expectedMessage, result.assertFired.errorMessage)
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @ParameterizedTest
  @EnumSource(
    value = classOf[Errors],
    names = Array(
      "NOT_COORDINATOR",
      "CONCURRENT_TRANSACTIONS",
      "NETWORK_EXCEPTION",
      "COORDINATOR_LOAD_IN_PROGRESS",
      "COORDINATOR_NOT_AVAILABLE"
    )
  )
  def testVerificationErrorConversionsTV1(error: Errors): Unit = {
    val localId = 1
    val tp0 = new TopicPartition(topic, 0)
    val producerId = 24L
    val producerEpoch = 0.toShort
    val sequence = 0
    val addPartitionsToTxnManager = mock(classOf[AddPartitionsToTxnManager])

    val replicaManager = setUpReplicaManagerWithMockedAddPartitionsToTxnManager(addPartitionsToTxnManager, List(tp0))
    try {
      val leaderDelta = topicsCreateDelta(localId, isStartIdLeader = true, partitions = List(0, 1), List.empty, topic, topicIds(topic))
      replicaManager.applyDelta(leaderDelta, imageFromTopics(leaderDelta.apply()))

      val transactionalRecords = MemoryRecords.withTransactionalRecords(Compression.NONE, producerId, producerEpoch, sequence,
        new SimpleRecord("message".getBytes))

      // Start verification and return the coordinator related errors.
      val expectedMessage = s"Unable to verify the partition has been added to the transaction. Underlying error: ${error.toString}"
      val result = handleProduceAppend(replicaManager, tp0, transactionalRecords, transactionalId = transactionalId)
      val appendCallback = ArgumentCaptor.forClass(classOf[AddPartitionsToTxnManager.AppendCallback])
      verify(addPartitionsToTxnManager, times(1)).addOrVerifyTransaction(
        ArgumentMatchers.eq(transactionalId),
        ArgumentMatchers.eq(producerId),
        ArgumentMatchers.eq(producerEpoch),
        ArgumentMatchers.eq(util.List.of(tp0)),
        appendCallback.capture(),
        any()
      )

      // Confirm we did not write to the log and instead returned the converted error with the correct error message.
      val callback: AddPartitionsToTxnManager.AppendCallback = appendCallback.getValue
      callback.complete(util.Map.of(tp0, error))
      assertEquals(Errors.NOT_ENOUGH_REPLICAS, result.assertFired.error)
      assertEquals(expectedMessage, result.assertFired.errorMessage)
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testPreVerificationError(): Unit = {
    val tp0 = new TopicPartition(topic, 0)
    val transactionalId = "txn-id"
    val producerId = 24L
    val producerEpoch = 0.toShort
    val addPartitionsToTxnManager = mock(classOf[AddPartitionsToTxnManager])

    val replicaManager = setUpReplicaManagerWithMockedAddPartitionsToTxnManager(addPartitionsToTxnManager, List(tp0))
    try {
      val result = maybeStartTransactionVerificationForPartition(replicaManager, tp0, transactionalId, producerId, producerEpoch)
      val appendCallback = ArgumentCaptor.forClass(classOf[AddPartitionsToTxnManager.AppendCallback])
      verify(addPartitionsToTxnManager, times(0)).addOrVerifyTransaction(
        ArgumentMatchers.eq(transactionalId),
        ArgumentMatchers.eq(producerId),
        ArgumentMatchers.eq(producerEpoch),
        ArgumentMatchers.eq(util.List.of(tp0)),
        appendCallback.capture(),
        any()
      )
      assertEquals(Errors.NOT_LEADER_OR_FOLLOWER, result.assertFired.left.getOrElse(Errors.NONE))
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  private def sendProducerAppend(
    replicaManager: ReplicaManager,
    topicPartition: TopicIdPartition,
    numOfRecords: Int
  ): AtomicReference[PartitionResponse] = {
    val produceResult = new AtomicReference[PartitionResponse]()
    def callback(response: Map[TopicIdPartition, PartitionResponse]): Unit = {
      produceResult.set(response(topicPartition))
    }

    val records = MemoryRecords.withRecords(
      Compression.NONE,
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

  private def prepareReplicaManagerAndLogManager(timer: MockTimer,
                                                 topicPartition: Int,
                                                 leaderEpochInLeaderAndIsr: Int,
                                                 followerBrokerId: Int,
                                                 leaderBrokerId: Int,
                                                 countDownLatch: CountDownLatch,
                                                 expectTruncation: Boolean,
                                                 localLogOffset: Optional[Long] = Optional.empty,
                                                 offsetFromLeader: Long = 5,
                                                 leaderEpochFromLeader: Int = 3,
                                                 extraProps: Properties = new Properties(),
                                                 topicId: Optional[Uuid] = Optional.empty): (ReplicaManager, LogManager) = {
    val props = TestUtils.createBrokerConfig(0)
    props.put("log.dir", TestUtils.tempRelativeDir("data").getAbsolutePath)
    props.asScala ++= extraProps.asScala
    val config = KafkaConfig.fromProps(props)
    val logConfig = new LogConfig(new Properties)
    val logDir = new File(new File(config.logDirs.get(0)), s"$topic-$topicPartition")
    Files.createDirectories(logDir.toPath)
    val mockScheduler = new MockScheduler(time)
    val mockBrokerTopicStats = new BrokerTopicStats
    val mockLogDirFailureChannel = new LogDirFailureChannel(config.logDirs.size)
    val tp = new TopicPartition(topic, topicPartition)
    val maxTransactionTimeoutMs = 30000
    val maxProducerIdExpirationMs = 30000
    val segments = new LogSegments(tp)
    val leaderEpochCache = UnifiedLog.createLeaderEpochCache(
      logDir, tp, mockLogDirFailureChannel, Optional.empty, time.scheduler)
    val producerStateManager = new ProducerStateManager(tp, logDir,
      maxTransactionTimeoutMs, new ProducerStateManagerConfig(maxProducerIdExpirationMs, true), time)
    val offsets = new LogLoader(
      logDir,
      tp,
      logConfig,
      mockScheduler,
      time,
      mockLogDirFailureChannel,
      true,
      segments,
      0L,
      0L,
      leaderEpochCache,
      producerStateManager,
      new ConcurrentHashMap[String, Integer],
      false
    ).load()
    val localLog = new LocalLog(logDir, logConfig, segments, offsets.recoveryPoint,
      offsets.nextOffsetMetadata, mockScheduler, time, tp, mockLogDirFailureChannel)
    val mockLog = new UnifiedLog(
      offsets.logStartOffset,
      localLog,
      mockBrokerTopicStats,
      30000,
      leaderEpochCache,
      producerStateManager,
      Optional.empty,
      false,
      LogOffsetsListener.NO_OP_OFFSETS_LISTENER) {

      override def endOffsetForEpoch(leaderEpoch: Int): Optional[OffsetAndEpoch] = {
        assertEquals(leaderEpoch, leaderEpochFromLeader)
        localLogOffset.toScala.map { logOffset =>
          Optional.of(new OffsetAndEpoch(logOffset, leaderEpochFromLeader))
        }.getOrElse(super.endOffsetForEpoch(leaderEpoch))
      }

      override def latestEpoch: Optional[Integer] = Optional.of(leaderEpochFromLeader)

      override def logEndOffsetMetadata: LogOffsetMetadata = {
        localLogOffset.toScala.map { new LogOffsetMetadata(_) }.getOrElse(super.logEndOffsetMetadata)
      }

      override def logEndOffset: Long = localLogOffset.orElse(super.logEndOffset)
    }

    // Expect to call LogManager.truncateTo exactly once
    val topicPartitionObj = new TopicPartition(topic, topicPartition)
    val mockLogMgr: LogManager = mock(classOf[LogManager])
    when(mockLogMgr.liveLogDirs).thenReturn(config.logDirs.asScala.map(new File(_).getAbsoluteFile))
    when(mockLogMgr.getOrCreateLog(ArgumentMatchers.eq(topicPartitionObj), ArgumentMatchers.eq(false), ArgumentMatchers.eq(false), any(), any())).thenReturn(mockLog)
    when(mockLogMgr.getLog(topicPartitionObj, isFuture = false)).thenReturn(Some(mockLog))
    when(mockLogMgr.getLog(topicPartitionObj, isFuture = true)).thenReturn(None)
    val allLogs = new ConcurrentHashMap[TopicPartition, UnifiedLog]()
    allLogs.put(topicPartitionObj, mockLog)
    when(mockLogMgr.allLogs).thenReturn(allLogs.values.asScala)
    when(mockLogMgr.isLogDirOnline(anyString)).thenReturn(true)
    when(mockLogMgr.directoryId(anyString)).thenReturn(None)

    val aliveBrokerIds = Seq[Integer](followerBrokerId, leaderBrokerId)
    val aliveBrokers = aliveBrokerIds.map(brokerId => new Node(brokerId, s"host$brokerId", brokerId))

    mockGetAliveBrokerFunctions(metadataCache, aliveBrokers)
    when(metadataCache.getPartitionReplicaEndpoints(
      any[TopicPartition], any[ListenerName])).
        thenReturn(util.Map.of(leaderBrokerId, new Node(leaderBrokerId, "host1", 9092, "rack-a"),
          followerBrokerId, new Node(followerBrokerId, "host2", 9092, "rack-b")))
    when(metadataCache.metadataVersion()).thenReturn(MetadataVersion.MINIMUM_VERSION)
    when(metadataCache.getAliveBrokerEpoch(leaderBrokerId)).thenReturn(util.Optional.of(brokerEpoch))
    val mockProducePurgatory = new DelayedOperationPurgatory[DelayedProduce](
      "Produce", timer, 0, false)
    val mockFetchPurgatory = new DelayedOperationPurgatory[DelayedFetch](
      "Fetch", timer, 0, false)
    val mockDeleteRecordsPurgatory = new DelayedOperationPurgatory[DelayedDeleteRecords](
      "DeleteRecords", timer, 0, false)
    val mockRemoteFetchPurgatory = new DelayedOperationPurgatory[DelayedRemoteFetch](
      "RemoteFetch", timer, 0, false)
    val mockRemoteListOffsetsPurgatory = new DelayedOperationPurgatory[DelayedRemoteListOffsets](
      "RemoteListOffsets", timer, 0, false)
    val mockDelayedShareFetchPurgatory = new DelayedOperationPurgatory[DelayedShareFetch](
      "ShareFetch", timer, 0, false)

    // Mock network client to show leader offset of 5
    val blockingSend = new MockBlockingSender(
      Map(topicPartitionObj -> new EpochEndOffset()
        .setPartition(topicPartitionObj.partition)
        .setErrorCode(Errors.NONE.code)
        .setLeaderEpoch(leaderEpochFromLeader)
        .setEndOffset(offsetFromLeader)).asJava,
      new BrokerEndPoint(1, "host1" ,1), time)
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
      delayedRemoteFetchPurgatoryParam = Some(mockRemoteFetchPurgatory),
      delayedRemoteListOffsetsPurgatoryParam = Some(mockRemoteListOffsetsPurgatory),
      delayedShareFetchPurgatoryParam = Some(mockDelayedShareFetchPurgatory)) {

      override protected def createReplicaFetcherManager(metrics: Metrics,
                                                         time: Time,
                                                         replicationQuotaManager: ReplicationQuotaManager): ReplicaFetcherManager = {
        val rm = this
        new ReplicaFetcherManager(this.config, rm, metrics, time, replicationQuotaManager, () => this.metadataCache.metadataVersion(), () => 1) {

          override def createFetcherThread(fetcherId: Int, sourceBroker: BrokerEndPoint): ReplicaFetcherThread = {
            val logContext = new LogContext(s"[ReplicaFetcher replicaId=${rm.config.brokerId}, leaderId=${sourceBroker.id}, " +
              s"fetcherId=$fetcherId] ")
            val fetchSessionHandler = new FetchSessionHandler(logContext, sourceBroker.id)
            val leader = new RemoteLeaderEndPoint(logContext.logPrefix, blockingSend, fetchSessionHandler, rm.config,
              rm, quotaManager.follower, () => MetadataVersion.MINIMUM_VERSION, () => 1)
            new ReplicaFetcherThread(s"ReplicaFetcherThread-$fetcherId", leader, rm.config, failedPartitions, rm,
              quotaManager.follower, logContext.logPrefix) {
              override def doWork(): Unit = {
                // In case the thread starts before the partition is added by AbstractFetcherManager,
                // add it here (it's a no-op if already added)
                val initialOffset = InitialFetchState(
                  topicId = topicId.toScala,
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

  private def partitionRegistration(leader: Int,
                                    leaderEpoch: Int,
                                    isr: Array[Int],
                                    partitionEpoch: Int,
                                    replicas: Array[Int]): PartitionRegistration = {
    new PartitionRegistration.Builder()
      .setLeader(leader)
      .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
      .setLeaderEpoch(leaderEpoch)
      .setIsr(isr)
      .setPartitionEpoch(partitionEpoch)
      .setReplicas(replicas)
      .setDirectories(DirectoryId.unassignedArray(replicas.length))
      .build()

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
    val topicIdPartition = new TopicIdPartition(topicId, partition)
    def appendCallback(responses: Map[TopicIdPartition, PartitionResponse]): Unit = {
      val response = responses.get(topicIdPartition)
      assertTrue(response.isDefined)
      result.fire(response.get)
    }

    replicaManager.appendRecords(
      timeout = 1000,
      requiredAcks = requiredAcks,
      internalTopicsAllowed = false,
      origin = origin,
      entriesPerPartition = Map(new TopicIdPartition(topicId, partition) -> records),
      responseCallback = appendCallback,
    )

    result
  }

  private def handleProduceAppendToMultipleTopics(replicaManager: ReplicaManager,
                                                  entriesToAppend: Map[TopicPartition, MemoryRecords],
                                                  transactionalId: String,
                                                  requiredAcks: Short = -1,
                                                  transactionSupportedOperation: TransactionSupportedOperation = GENERIC_ERROR_SUPPORTED
                                                 ): CallbackResult[Map[TopicIdPartition, PartitionResponse]] = {
    val result = new CallbackResult[Map[TopicIdPartition, PartitionResponse]]()
    def appendCallback(responses: Map[TopicIdPartition, PartitionResponse]): Unit = {
      responses.foreach( response => assertTrue(responses.get(response._1).isDefined))
      result.fire(responses)
    }

    replicaManager.handleProduceAppend(
      timeout = 1000,
      requiredAcks = requiredAcks,
      internalTopicsAllowed = false,
      transactionalId = transactionalId,
      entriesPerPartition = entriesToAppend.map { case(tp, memoryRecords) => replicaManager.topicIdPartition(tp) -> memoryRecords },
      responseCallback = appendCallback,
      transactionSupportedOperation = transactionSupportedOperation
    )

    result
  }

  private def handleProduceAppend(replicaManager: ReplicaManager,
                                  partition: TopicPartition,
                                  records: MemoryRecords,
                                  origin: AppendOrigin = AppendOrigin.CLIENT,
                                  requiredAcks: Short = -1,
                                  transactionalId: String,
                                  transactionSupportedOperation: TransactionSupportedOperation = GENERIC_ERROR_SUPPORTED
                                 ): CallbackResult[PartitionResponse] = {
    val result = new CallbackResult[PartitionResponse]()

    val topicIdPartition = new TopicIdPartition(topicIds.get(partition.topic()).getOrElse(Uuid.ZERO_UUID), partition)
    def appendCallback(responses: Map[TopicIdPartition, PartitionResponse]): Unit = {
      val response = responses.get(topicIdPartition)
      assertTrue(response.isDefined)
      result.fire(response.get)
    }

    val entriesPerPartition = Map(partition -> records)
    replicaManager.handleProduceAppend(
      timeout = 1000,
      requiredAcks = requiredAcks,
      internalTopicsAllowed = false,
      transactionalId = transactionalId,
      entriesPerPartition = entriesPerPartition.map {
        case (topicPartition, records) => replicaManager.topicIdPartition(topicPartition) -> records
      },
      responseCallback = appendCallback,
      transactionSupportedOperation = transactionSupportedOperation
    )

    result
  }

  private def maybeStartTransactionVerificationForPartition(replicaManager: ReplicaManager,
                                                            topicPartition: TopicPartition,
                                                            transactionalId: String,
                                                            producerId: Long,
                                                            producerEpoch: Short,
                                                            baseSequence: Int = 0,
                                                            transactionSupportedOperation: TransactionSupportedOperation = GENERIC_ERROR_SUPPORTED
                                                           ): CallbackResult[Either[Errors, VerificationGuard]] = {
    val result = new CallbackResult[Either[Errors, VerificationGuard]]()
    def postVerificationCallback(errorAndGuard: (Errors, VerificationGuard)): Unit = {
      val (error, verificationGuard) = errorAndGuard
      val errorOrGuard = if (error != Errors.NONE) Left(error) else Right(verificationGuard)
      result.fire(errorOrGuard)
    }

    replicaManager.maybeSendPartitionToTransactionCoordinator(
      topicPartition,
      transactionalId,
      producerId,
      producerEpoch,
      baseSequence,
      postVerificationCallback,
      transactionSupportedOperation
    )
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
      case IsolationLevel.READ_COMMITTED => FetchIsolation.TXN_COMMITTED
      case IsolationLevel.READ_UNCOMMITTED => FetchIsolation.HIGH_WATERMARK
    }

    fetchPartition(
      replicaManager,
      replicaId = FetchRequest.ORDINARY_CONSUMER_ID,
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
      isolation = FetchIsolation.LOG_END,
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
    quota: ReplicaQuota = UNBOUNDED_QUOTA,
    isolation: FetchIsolation = FetchIsolation.LOG_END,
    clientMetadata: Option[ClientMetadata] = None
  ): Unit = {
    val params = new FetchParams(
      replicaId,
      1,
      maxWaitMs,
      minBytes,
      maxBytes,
      isolation,
      clientMetadata.toJava
    )

    replicaManager.fetchMessages(
      params,
      fetchInfos,
      quota,
      responseCallback
    )
  }

  private def getVerificationGuard(replicaManager: ReplicaManager,
                                   tp: TopicPartition,
                                   producerId: Long): Object = {
    replicaManager.getPartitionOrException(tp).log.get.verificationGuard(producerId)
  }

  private def setUpReplicaManagerWithMockedAddPartitionsToTxnManager(addPartitionsToTxnManager: AddPartitionsToTxnManager,
                                                                     transactionalTopicPartitions: List[TopicPartition],
                                                                     config: KafkaConfig = config,
                                                                     scheduler: Scheduler = new MockScheduler(time)): ReplicaManager = {
    val mockLogMgr = TestUtils.createLogManager(config.logDirs.asScala.map(new File(_)))
    val replicaManager = new ReplicaManager(
      metrics = metrics,
      config = config,
      time = time,
      scheduler = scheduler,
      logManager = mockLogMgr,
      quotaManagers = quotaManager,
      metadataCache = metadataCache,
      logDirFailureChannel = new LogDirFailureChannel(config.logDirs.size),
      alterPartitionManager = alterPartitionManager,
      addPartitionsToTxnManager = Some(addPartitionsToTxnManager))

    transactionalTopicPartitions.foreach(tp => when(metadataCache.contains(tp)).thenReturn(true))

    // We will attempt to schedule to the request handler thread using a non request handler thread. Set this to avoid error.
    KafkaRequestHandler.setBypassThreadCheck(true)
    replicaManager
  }

  private def setupReplicaManagerWithMockedPurgatories(
    timer: MockTimer,
    brokerId: Int = 0,
    aliveBrokerIds: Seq[Int] = Seq(0, 1),
    propsModifier: Properties => Unit = _ => {},
    mockReplicaFetcherManager: Option[ReplicaFetcherManager] = None,
    mockReplicaAlterLogDirsManager: Option[ReplicaAlterLogDirsManager] = None,
    enableRemoteStorage: Boolean = false,
    shouldMockLog: Boolean = false,
    remoteLogManager: Option[RemoteLogManager] = None,
    defaultTopicRemoteLogStorageEnable: Boolean = true,
    setupLogDirMetaProperties: Boolean = false,
    directoryEventHandler: DirectoryEventHandler = DirectoryEventHandler.NOOP,
    buildRemoteLogAuxState: Boolean = false,
    remoteFetchQuotaExceeded: Option[Boolean] = None,
    remoteFetchReaperEnabled: Boolean = false
  ): ReplicaManager = {
    val props = TestUtils.createBrokerConfig(brokerId)
    val path1 = TestUtils.tempRelativeDir("data").getAbsolutePath
    val path2 = TestUtils.tempRelativeDir("data2").getAbsolutePath
    props.put(RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, enableRemoteStorage.toString)
    props.put("log.dirs", path1 + "," + path2)
    propsModifier.apply(props)
    val config = KafkaConfig.fromProps(props)
    val logProps = new Properties()
    if (enableRemoteStorage && defaultTopicRemoteLogStorageEnable) {
      logProps.put(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true")
    }
    val logConfig = new LogConfig(logProps)
    val mockLogFn = (topicPartition: TopicPartition, topicId: Option[Uuid]) => setupMockLog(path1, logConfig, enableRemoteStorage, topicPartition, topicId)
    if (setupLogDirMetaProperties) {
      // add meta.properties file in each dir
      config.logDirs.stream().forEach(dir => {
        val metaProps = new MetaProperties.Builder().
          setVersion(MetaPropertiesVersion.V0).
          setClusterId("clusterId").
          setNodeId(brokerId).
          setDirectoryId(DirectoryId.random()).
          build()
        PropertiesUtils.writePropertiesFile(metaProps.toProperties,
          new File(new File(dir), MetaPropertiesEnsemble.META_PROPERTIES_NAME).getAbsolutePath, false)
      })
    }
    val mockLogMgr = TestUtils.createLogManager(config.logDirs.asScala.map(new File(_)), logConfig, logFn = if (shouldMockLog) Some(mockLogFn) else None, remoteStorageSystemEnable = enableRemoteStorage)
    val aliveBrokers = aliveBrokerIds.map(brokerId => new Node(brokerId, s"host$brokerId", brokerId))
    brokerTopicStats = new BrokerTopicStats(KafkaConfig.fromProps(props).remoteLogManagerConfig.isRemoteStorageSystemEnabled)

    val metadataCache: MetadataCache = mock(classOf[MetadataCache])
    when(metadataCache.topicIdsToNames()).thenReturn(topicNames.asJava)
    when(metadataCache.metadataVersion()).thenReturn(MetadataVersion.MINIMUM_VERSION)
    mockGetAliveBrokerFunctions(metadataCache, aliveBrokers)
    when(metadataCache.getAliveBrokerEpoch(brokerId+1)).thenReturn(util.Optional.of(brokerEpoch))
    val mockProducePurgatory = new DelayedOperationPurgatory[DelayedProduce](
      "Produce", timer, 0, false)
    val mockFetchPurgatory = new DelayedOperationPurgatory[DelayedFetch](
      "Fetch", timer, 0, false)
    val mockDeleteRecordsPurgatory = new DelayedOperationPurgatory[DelayedDeleteRecords](
      "DeleteRecords", timer, 0, false)
    // When enabling reaper, set the new timer instance so that other tests won't be affected.
    val mockDelayedRemoteFetchPurgatory = new DelayedOperationPurgatory[DelayedRemoteFetch](
      "DelayedRemoteFetch", if (remoteFetchReaperEnabled) new SystemTimer("DelayedRemoteFetch") else timer, 0, 0, remoteFetchReaperEnabled, true)
    val mockDelayedRemoteListOffsetsPurgatory = new DelayedOperationPurgatory[DelayedRemoteListOffsets](
      "RemoteListOffsets", timer, 0, false)
    val mockDelayedShareFetchPurgatory = new DelayedOperationPurgatory[DelayedShareFetch](
      "ShareFetch", timer, 0, false)

    when(metadataCache.contains(new TopicPartition(topic, 0))).thenReturn(true)

    if (remoteFetchQuotaExceeded.isDefined) {
      assertFalse(remoteLogManager.isDefined)
      if (remoteFetchQuotaExceeded.get) {
        when(mockRemoteLogManager.getFetchThrottleTimeMs).thenReturn(quotaExceededThrottleTime)
      } else {
        when(mockRemoteLogManager.getFetchThrottleTimeMs).thenReturn(quotaAvailableThrottleTime)
      }
    }

    // Transactional appends attempt to schedule to the request handler thread using a non request handler thread. Set this to avoid error.
    KafkaRequestHandler.setBypassThreadCheck(true)

    new ReplicaManager(
      metrics = metrics,
      config = config,
      time = time,
      scheduler = new MockScheduler(time),
      logManager = mockLogMgr,
      quotaManagers = quotaManager,
      metadataCache = metadataCache,
      logDirFailureChannel = new LogDirFailureChannel(config.logDirs.size),
      alterPartitionManager = alterPartitionManager,
      brokerTopicStats = brokerTopicStats,
      delayedProducePurgatoryParam = Some(mockProducePurgatory),
      delayedFetchPurgatoryParam = Some(mockFetchPurgatory),
      delayedDeleteRecordsPurgatoryParam = Some(mockDeleteRecordsPurgatory),
      delayedRemoteFetchPurgatoryParam = Some(mockDelayedRemoteFetchPurgatory),
      delayedRemoteListOffsetsPurgatoryParam = Some(mockDelayedRemoteListOffsetsPurgatory),
      delayedShareFetchPurgatoryParam = Some(mockDelayedShareFetchPurgatory),
      addPartitionsToTxnManager = Some(addPartitionsToTxnManager),
      directoryEventHandler = directoryEventHandler,
      remoteLogManager = if (enableRemoteStorage) {
        if (remoteLogManager.isDefined)
          remoteLogManager
        else
          Some(mockRemoteLogManager)
      } else None) {

      override protected def createReplicaFetcherManager(
        metrics: Metrics,
        time: Time,
        quotaManager: ReplicationQuotaManager
      ): ReplicaFetcherManager = {
        mockReplicaFetcherManager.getOrElse {
          if (buildRemoteLogAuxState) {
            super.createReplicaFetcherManager(
              metrics,
              time,
              quotaManager
            )
            val config = this.config
            val metadataCache = this.metadataCache
            new ReplicaFetcherManager(config, this, metrics, time, quotaManager, () => metadataCache.metadataVersion(), () => 1) {
              override def createFetcherThread(fetcherId: Int, sourceBroker: BrokerEndPoint): ReplicaFetcherThread = {
                val threadName = s"ReplicaFetcherThread-$fetcherId-${sourceBroker.id}"

                val tp = new TopicPartition(topic, 0)
                val leader = new MockLeaderEndPoint() {
                  override def fetch(fetchRequest: FetchRequest.Builder): java.util.Map[TopicPartition, FetchResponseData.PartitionData]  = {
                    Map(tp -> new FetchResponseData.PartitionData().setErrorCode(Errors.OFFSET_MOVED_TO_TIERED_STORAGE.code))
                  }.asJava
                }
                leader.setLeaderState(tp, PartitionState(leaderEpoch = 0))
                leader.setReplicaPartitionStateCallback(_ => PartitionState(leaderEpoch = 0))

                val fetcher = new ReplicaFetcherThread(threadName, leader, config, failedPartitions, replicaManager,
                  quotaManager, "")

                val initialFetchState = InitialFetchState(
                  topicId = Some(Uuid.randomUuid()),
                  leader = leader.brokerEndPoint(),
                  currentLeaderEpoch = 0,
                  initOffset = 0)

                fetcher.addPartitions(Map(tp -> initialFetchState))

                fetcher
              }
            }
          } else {
            super.createReplicaFetcherManager(
              metrics,
              time,
              quotaManager
            )
          }
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
    val leaderEpoch = 0
    val leaderEpochIncrement = 1
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

      val delta1 = createLeaderDelta(topicIds.get(topic), tp0, partition0Replicas.get(0), partition0Replicas, partition0Replicas, leaderEpoch)
      delta1.replay(new PartitionRecord()
        .setPartitionId(tp1.partition)
        .setTopicId(topicIds.get(topic))
        .setIsr(partition1Replicas)
        .setReplicas(partition1Replicas)
        .setLeader(partition1Replicas.get(0))
        .setLeaderEpoch(leaderEpoch)
        .setPartitionEpoch(0)
      )
      val leaderMetadataImage1 = imageFromTopics(delta1.apply())
      rm0.applyDelta(delta1, leaderMetadataImage1)
      rm1.applyDelta(delta1, leaderMetadataImage1)

      // make broker 0 the leader of partition 1 so broker 1 loses its leadership position
      val delta2 = createLeaderDelta(topicIds.get(topic), tp0, partition0Replicas.get(0), partition0Replicas, partition0Replicas, leaderEpoch + leaderEpochIncrement)
      delta2.replay(new PartitionRecord()
        .setPartitionId(tp1.partition)
        .setTopicId(topicIds.get(topic))
        .setIsr(partition1Replicas)
        .setReplicas(partition1Replicas)
        .setLeader(partition1Replicas.get(1))
        .setLeaderEpoch(leaderEpoch + leaderEpochIncrement)
        .setPartitionEpoch(0)
      )
      val leaderMetadataImage2 = imageFromTopics(delta2.apply())
      rm0.applyDelta(delta2, leaderMetadataImage2)
      rm1.applyDelta(delta2, leaderMetadataImage2)
    } finally {
      Utils.tryAll(util.Arrays.asList[Callable[Void]](
        () => {
          rm0.shutdown(checkpointHW = false)
          null
        },
        () => {
          rm1.shutdown(checkpointHW = false)
          null
        }
      ))
    }

    // verify that broker 1 did remove its metrics when no longer being the leader of partition 1
    verify(mockTopicStats1).removeOldLeaderMetrics(topic)
  }

  @Test
  def testOldFollowerLosesMetricsWhenReassignPartitions(): Unit = {
    val leaderEpoch = 0
    val leaderEpochIncrement = 1
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

      val delta = createLeaderDelta(topicIds.get(topic), tp0, partition0Replicas.get(0), partition0Replicas, partition0Replicas, leaderEpoch)
      delta.replay(new PartitionRecord()
        .setPartitionId(tp1.partition)
        .setTopicId(topicIds.get(topic))
        .setIsr(partition1Replicas)
        .setReplicas(partition1Replicas)
        .setLeader(partition1Replicas.get(0))
        .setLeaderEpoch(leaderEpoch)
        .setPartitionEpoch(0)
      )
      val leaderMetadataImage = imageFromTopics(delta.apply())
      rm0.applyDelta(delta, leaderMetadataImage)
      rm1.applyDelta(delta, leaderMetadataImage)

      // make broker 0 the leader of partition 1 so broker 1 loses its leadership position
      val delta2 = createLeaderDelta(topicIds.get(topic), tp0, partition0Replicas.get(1), partition0Replicas, partition0Replicas, leaderEpoch + leaderEpochIncrement)
      delta2.replay(new PartitionRecord()
        .setPartitionId(tp1.partition)
        .setTopicId(topicIds.get(topic))
        .setIsr(partition1Replicas)
        .setReplicas(partition1Replicas)
        .setLeader(partition1Replicas.get(1))
        .setLeaderEpoch(leaderEpoch + leaderEpochIncrement)
        .setPartitionEpoch(0)
      )
      val leaderMetadataImage2 = imageFromTopics(delta2.apply())
      rm0.applyDelta(delta2, leaderMetadataImage2)
      rm1.applyDelta(delta2, leaderMetadataImage2)
    } finally {
      Utils.tryAll(util.Arrays.asList[Callable[Void]](
        () => {
          rm0.shutdown(checkpointHW = false)
          null
        },
        () => {
          rm1.shutdown(checkpointHW = false)
          null
        }
      ))
    }

    // verify that broker 1 did remove its metrics when no longer being the leader of partition 1
    verify(mockTopicStats1).removeOldLeaderMetrics(topic)
  }

  private def prepareDifferentReplicaManagers(brokerTopicStats1: BrokerTopicStats,
                                              brokerTopicStats2: BrokerTopicStats): (ReplicaManager, ReplicaManager) = {
    val props0 = TestUtils.createBrokerConfig(0)
    val props1 = TestUtils.createBrokerConfig(1)

    props0.put("log0.dir", TestUtils.tempRelativeDir("data").getAbsolutePath)
    props1.put("log1.dir", TestUtils.tempRelativeDir("data").getAbsolutePath)

    val config0 = KafkaConfig.fromProps(props0)
    val config1 = KafkaConfig.fromProps(props1)

    val mockLogMgr0 = TestUtils.createLogManager(config0.logDirs.asScala.map(new File(_)))
    val mockLogMgr1 = TestUtils.createLogManager(config1.logDirs.asScala.map(new File(_)))

    val metadataCache0: MetadataCache = mock(classOf[MetadataCache])
    val metadataCache1: MetadataCache = mock(classOf[MetadataCache])
    val aliveBrokers = Seq(new Node(0, "host0", 0), new Node(1, "host1", 1))
    mockGetAliveBrokerFunctions(metadataCache0, aliveBrokers)
    mockGetAliveBrokerFunctions(metadataCache1, aliveBrokers)
    when(metadataCache0.metadataVersion()).thenReturn(MetadataVersion.MINIMUM_VERSION)
    when(metadataCache1.metadataVersion()).thenReturn(MetadataVersion.MINIMUM_VERSION)

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

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testOffsetOutOfRangeExceptionWhenReadFromLog(isFromFollower: Boolean): Unit = {
    val replicaId = if (isFromFollower) 1 else -1
    val fetchMaxBytes = 150
    val partitionMaxBytes = 100
    val tp0 = new TopicPartition(topic, 0)
    val tp02 = new TopicPartition(topic2, 0)
    val tidp0 = new TopicIdPartition(topicId, tp0)
    val tidp02 = new TopicIdPartition(topicId2, tp02)
    // create a replicaManager with remoteLog enabled
    val replicaManager = setupReplicaManagerWithMockedPurgatories(new MockTimer(time), aliveBrokerIds = Seq(0, 1, 2), enableRemoteStorage = true, shouldMockLog = true, remoteFetchQuotaExceeded = Some(false))
    try {
      val offsetCheckpoints = new LazyOffsetCheckpoints(replicaManager.highWatermarkCheckpoints.asJava)
      replicaManager.createPartition(tp0).createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints, Some(topicId))
      replicaManager.createPartition(tp02).createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints, Some(topicId2))
      val partition0Replicas = Seq[Integer](0, 1).asJava
      val topicIds = Map(tp0.topic -> topicId, tp02.topic -> topicId2).asJava
      val leaderEpoch = 0
      val delta = createLeaderDelta(topicIds.get(topic), tp0, partition0Replicas.get(0), partition0Replicas, partition0Replicas)
      val delta2 = createLeaderDelta(topicIds.get(topic2), tp02, partition0Replicas.get(0), partition0Replicas, partition0Replicas)
      val leaderMetadataImage = imageFromTopics(delta.apply())
      val leaderMetadataImage2 = imageFromTopics(delta2.apply())
      replicaManager.applyDelta(delta, leaderMetadataImage)
      replicaManager.applyDelta(delta2, leaderMetadataImage2)

      val params = new FetchParams(replicaId, 1, 100, 0, fetchMaxBytes, FetchIsolation.LOG_END, Optional.empty)
      // when reading logs from 2 partitions, they'll throw OffsetOutOfRangeException, which will be handled separately
      val results = replicaManager.readFromLog(params, Seq(
        tidp0 -> new PartitionData(topicId, 1, 0, partitionMaxBytes, Optional.of[Integer](leaderEpoch), Optional.of[Integer](leaderEpoch)),
        tidp02 -> new PartitionData(topicId2, 1, 0, partitionMaxBytes, Optional.of[Integer](leaderEpoch), Optional.of[Integer](leaderEpoch))), UNBOUNDED_QUOTA, false)

      results.foreach { case (tidp, partitionData) =>
        assertEquals(startOffset, partitionData.leaderLogStartOffset)
        assertEquals(endOffset, partitionData.leaderLogEndOffset)
        assertEquals(highHW, partitionData.highWatermark)
        if (isFromFollower) {
          // expect OFFSET_MOVED_TO_TIERED_STORAGE error returned if it's from follower, since the data is already available in remote log
          assertEquals(Errors.OFFSET_MOVED_TO_TIERED_STORAGE, partitionData.error)
          assertFalse(partitionData.info.delayedRemoteStorageFetch.isPresent)
        } else {
          assertEquals(Errors.NONE, partitionData.error)
          // for consumer fetch, we should return a delayedRemoteStorageFetch to wait for remote fetch
          assertTrue(partitionData.info.delayedRemoteStorageFetch.isPresent)
          // verify the 1st partition will set the fetchMaxBytes to partitionMaxBytes,
          // and the 2nd one will set to the remaining (fetchMaxBytes - partitionMaxBytes) to meet the "fetch.max.bytes" config.
          if (tidp.topic == topic)
            assertEquals(partitionMaxBytes, partitionData.info.delayedRemoteStorageFetch.get().fetchMaxBytes)
          else
            assertEquals(fetchMaxBytes - partitionMaxBytes, partitionData.info.delayedRemoteStorageFetch.get().fetchMaxBytes)
        }
      }
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testOffsetOutOfRangeExceptionWhenFetchMessages(isFromFollower: Boolean): Unit = {
    val brokerList = Seq[Integer](0, 1).asJava
    val replicaId = if (isFromFollower) 1 else -1
    val tp0 = new TopicPartition(topic, 0)
    val tidp0 = new TopicIdPartition(topicId, tp0)
    // create a replicaManager with remoteLog enabled
    val replicaManager = setupReplicaManagerWithMockedPurgatories(new MockTimer(time), aliveBrokerIds = Seq(0, 1, 2), enableRemoteStorage = true, shouldMockLog= true, remoteFetchQuotaExceeded = Some(false))
    try {
      val offsetCheckpoints = new LazyOffsetCheckpoints(replicaManager.highWatermarkCheckpoints.asJava)
      replicaManager.createPartition(tp0).createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints, Some(topicId))
      val topicIds = Map(tp0.topic -> topicId).asJava
      val leaderEpoch = 0
      val delta = createLeaderDelta(topicIds.get(topic), tp0, brokerList.get(0), brokerList, brokerList)
      val leaderMetadataImage = imageFromTopics(delta.apply())
      replicaManager.applyDelta(delta, leaderMetadataImage)

      val params = new FetchParams(replicaId, 1, 1000, 10, 100, FetchIsolation.LOG_END, Optional.empty)
      val fetchOffset = 1

      def fetchCallback(responseStatus: Seq[(TopicIdPartition, FetchPartitionData)]): Unit = {
        assertEquals(1, responseStatus.size)
        assertEquals(tidp0, responseStatus.toMap.keySet.head)
        val fetchPartitionData: FetchPartitionData = responseStatus.toMap.get(tidp0).get
        // should only follower fetch enter callback since consumer fetch will enter remoteFetch purgatory
        assertTrue(isFromFollower)
        assertEquals(Errors.OFFSET_MOVED_TO_TIERED_STORAGE, fetchPartitionData.error)
        assertEquals(startOffset, fetchPartitionData.logStartOffset)
        assertEquals(highHW, fetchPartitionData.highWatermark)
      }

      // when reading log, it'll throw OffsetOutOfRangeException, which will be handled separately
      replicaManager.fetchMessages(params, Seq(tidp0 -> new PartitionData(topicId, fetchOffset, 0, 100000, Optional.of[Integer](leaderEpoch), Optional.of[Integer](leaderEpoch))), UNBOUNDED_QUOTA, fetchCallback)

      val remoteStorageFetchInfoArg: ArgumentCaptor[RemoteStorageFetchInfo] = ArgumentCaptor.forClass(classOf[RemoteStorageFetchInfo])
      if (isFromFollower) {
        verify(mockRemoteLogManager, never()).asyncRead(remoteStorageFetchInfoArg.capture(), any())
      } else {
        verify(mockRemoteLogManager).asyncRead(remoteStorageFetchInfoArg.capture(), any())
        val remoteStorageFetchInfo = remoteStorageFetchInfoArg.getValue
        assertEquals(tp0, remoteStorageFetchInfo.topicIdPartition.topicPartition)
        assertEquals(fetchOffset, remoteStorageFetchInfo.fetchInfo.fetchOffset)
        assertEquals(topicId, remoteStorageFetchInfo.fetchInfo.topicId)
        assertEquals(startOffset, remoteStorageFetchInfo.fetchInfo.logStartOffset)
        assertEquals(leaderEpoch, remoteStorageFetchInfo.fetchInfo.currentLeaderEpoch.get())
      }
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testRemoteLogReaderMetrics(): Unit = {
    val replicaId = -1
    val tp0 = new TopicPartition(topic, 0)
    val tidp0 = new TopicIdPartition(topicId, tp0)

    val props = new Properties()
    props.setProperty(KRaftConfigs.PROCESS_ROLES_CONFIG, "controller")
    props.setProperty(KRaftConfigs.NODE_ID_CONFIG, "0")
    props.setProperty(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "CONTROLLER")
    props.setProperty("controller.quorum.bootstrap.servers", "localhost:9093")
    props.setProperty("listeners", "CONTROLLER://:9093")
    props.setProperty("advertised.listeners", "CONTROLLER://127.0.0.1:9093")
    props.put(RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, true.toString)
    props.put(RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CLASS_NAME_PROP, classOf[NoOpRemoteStorageManager].getName)
    props.put(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP, classOf[NoOpRemoteLogMetadataManager].getName)
    // set log reader threads number to 2
    props.put(RemoteLogManagerConfig.REMOTE_LOG_READER_THREADS_PROP, 2.toString)
    val config = KafkaConfig.fromProps(props)
    val mockLog = mock(classOf[UnifiedLog])
    val brokerTopicStats = new BrokerTopicStats(config.remoteLogManagerConfig.isRemoteStorageSystemEnabled)
    val remoteLogManager = new RemoteLogManager(
      config.remoteLogManagerConfig,
      0,
      TestUtils.tempRelativeDir("data").getAbsolutePath,
      "clusterId",
      time,
      _ => Optional.of(mockLog),
      (TopicPartition, Long) => {},
      brokerTopicStats,
      metrics,
      Optional.empty)
    val spyRLM = spy(remoteLogManager)

    val replicaManager = setupReplicaManagerWithMockedPurgatories(new MockTimer(time), aliveBrokerIds = Seq(0, 1, 2),
      // Increased the `remote.max.wait.ms` to avoid flaky test due to usage of SystemTimer
      propsModifier = props => props.put(RemoteLogManagerConfig.REMOTE_FETCH_MAX_WAIT_MS_PROP, 120000.toString),
      enableRemoteStorage = true, shouldMockLog = true, remoteLogManager = Some(spyRLM), remoteFetchReaperEnabled = true)
    try {
      val offsetCheckpoints = new LazyOffsetCheckpoints(replicaManager.highWatermarkCheckpoints.asJava)
      replicaManager.createPartition(tp0).createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints, Some(topicId))

      val leaderEpoch = 0
      val leaderDelta = createLeaderDelta(topicId, tp0, leaderId = 0, leaderEpoch = leaderEpoch)
      val leaderMetadataImage = imageFromTopics(leaderDelta.apply())
      replicaManager.applyDelta(leaderDelta, leaderMetadataImage)

      val params = new FetchParams(replicaId, 1, 1000, 10, 100, FetchIsolation.LOG_END, Optional.empty)
      val fetchOffset = 1
      val responseLatch = new CountDownLatch(5)

      def fetchCallback(responseStatus: Seq[(TopicIdPartition, FetchPartitionData)]): Unit = {
        assertEquals(1, responseStatus.size)
        assertEquals(tidp0, responseStatus.toMap.keySet.head)
        responseLatch.countDown()
      }

      assertEquals(1.0, yammerMetricValue("RemoteLogReaderAvgIdlePercent").asInstanceOf[Double])
      assertEquals(0, yammerMetricValue("RemoteLogReaderTaskQueueSize").asInstanceOf[Int])
      assertEquals(0L, yammerMetricValue("RemoteLogReaderFetchRateAndTimeMs").asInstanceOf[Long])

      // our thread number is 2
      val queueLatch = new CountDownLatch(2)
      val doneLatch = new CountDownLatch(1)

      doAnswer(_ => {
        queueLatch.countDown()
        // wait until verification completed
        doneLatch.await(5000, TimeUnit.MILLISECONDS)
        new FetchDataInfo(new LogOffsetMetadata(startOffset), mock(classOf[Records]))
      }).when(spyRLM).read(any())

      // create 5 asyncRead tasks, which should enqueue 3 task
      for (i <- 1 to 5)
        replicaManager.fetchMessages(params, Seq(tidp0 -> new PartitionData(topicId, fetchOffset, 0, 100000, Optional.of[Integer](leaderEpoch), Optional.of[Integer](leaderEpoch))), UNBOUNDED_QUOTA, fetchCallback)

      // wait until at least 2 task submitted to use all the available threads
      queueLatch.await(5000, TimeUnit.MILLISECONDS)
      // RemoteLogReader should not be all idle
      assertTrue(yammerMetricValue("RemoteLogReaderAvgIdlePercent").asInstanceOf[Double] < 1.0)
      // RemoteLogReader should queue some tasks
      assertEquals(3, yammerMetricValue("RemoteLogReaderTaskQueueSize").asInstanceOf[Int])
      // unlock all tasks
      doneLatch.countDown()
      responseLatch.await(5000, TimeUnit.MILLISECONDS)
      assertEquals(5L, yammerMetricValue("RemoteLogReaderFetchRateAndTimeMs").asInstanceOf[Long])
    } finally {
      Utils.tryAll(util.Arrays.asList[Callable[Void]](
        () => {
          replicaManager.shutdown(checkpointHW = false)
          null
        },
        () => {
          remoteLogManager.close()
          null
        }
      ))
      val allMetrics = KafkaYammerMetrics.defaultRegistry.allMetrics.asScala
      assertFalse(allMetrics.exists { case (n, _) => n.getMBeanName.endsWith("RemoteLogReaderFetchRateAndTimeMs") })
    }
  }

  @Test
  def testRemoteFetchExpiresPerSecMetric(): Unit = {
    val replicaId = -1
    val tp0 = new TopicPartition(topic, 0)
    val tidp0 = new TopicIdPartition(topicId, tp0)

    val props = new Properties()
    props.setProperty("process.roles", "controller")
    props.setProperty("node.id", "0")
    props.setProperty("controller.listener.names", "CONTROLLER")
    props.setProperty("controller.quorum.bootstrap.servers", "localhost:9093")
    props.setProperty("listeners", "CONTROLLER://:9093")
    props.setProperty("advertised.listeners", "CONTROLLER://127.0.0.1:9093")
    props.put(RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, true.toString)
    props.put(RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CLASS_NAME_PROP, classOf[NoOpRemoteStorageManager].getName)
    props.put(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP, classOf[NoOpRemoteLogMetadataManager].getName)
    val config = KafkaConfig.fromProps(props)
    val dummyLog = mock(classOf[UnifiedLog])
    val brokerTopicStats = new BrokerTopicStats(config.remoteLogManagerConfig.isRemoteStorageSystemEnabled)
    val remoteLogManager = new RemoteLogManager(
      config.remoteLogManagerConfig,
      0,
      TestUtils.tempRelativeDir("data").getAbsolutePath,
      "clusterId",
      time,
      _ => Optional.of(dummyLog),
      (TopicPartition, Long) => {},
      brokerTopicStats,
      metrics,
      Optional.empty)
    val spyRLM = spy(remoteLogManager)
    val timer = new MockTimer(time)

    val replicaManager = setupReplicaManagerWithMockedPurgatories(timer, aliveBrokerIds = Seq(0, 1, 2), enableRemoteStorage = true, shouldMockLog = true, remoteLogManager = Some(spyRLM))

    try {
      val offsetCheckpoints = new LazyOffsetCheckpoints(replicaManager.highWatermarkCheckpoints.asJava)
      replicaManager.createPartition(tp0).createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints, Some(topicId))

      val leaderEpoch = 0
      val leaderDelta = createLeaderDelta(topicId, tp0, leaderId = 0, leaderEpoch = leaderEpoch)
      val leaderMetadataImage = imageFromTopics(leaderDelta.apply())
      replicaManager.applyDelta(leaderDelta, leaderMetadataImage)

      val mockLog = replicaManager.getPartitionOrException(tp0).log.get
      when(mockLog.endOffsetForEpoch(anyInt())).thenReturn(Optional.of(new OffsetAndEpoch(1, 1)))
      when(mockLog.read(anyLong(), anyInt(), any(), anyBoolean())).thenReturn(new FetchDataInfo(
        new LogOffsetMetadata(0L, 0L, 0),
        MemoryRecords.EMPTY
      ))
      val endOffsetMetadata = new LogOffsetMetadata(100L, 0L, 500)
      when(mockLog.fetchOffsetSnapshot).thenReturn(new LogOffsetSnapshot(
        0L,
        endOffsetMetadata,
        endOffsetMetadata,
        endOffsetMetadata))

      val params = new FetchParams(replicaId, 1, 1000, 10, 100, FetchIsolation.LOG_END, Optional.empty)
      val fetchOffset = 1

      def fetchCallback(responseStatus: Seq[(TopicIdPartition, FetchPartitionData)]): Unit = {
        assertEquals(1, responseStatus.size)
        assertEquals(tidp0, responseStatus.toMap.keySet.head)
      }

      val latch = new CountDownLatch(1)
      doAnswer(_ => {
        // wait until verification completes
        latch.await(5000, TimeUnit.MILLISECONDS)
        mock(classOf[FetchDataInfo])
      }).when(spyRLM).read(any())

      val curExpiresPerSec = DelayedRemoteFetch.expiredRequestCount()
      replicaManager.fetchMessages(params, Seq(tidp0 -> new PartitionData(topicId, fetchOffset, 0, 100000, Optional.of[Integer](leaderEpoch), Optional.of[Integer](leaderEpoch))), UNBOUNDED_QUOTA, fetchCallback)
      // advancing the clock to expire the delayed remote fetch
      timer.advanceClock(2000L)

      // verify the DelayedRemoteFetchMetrics.expiredRequestMeter.mark is called since the delayed remote fetch is expired
      TestUtils.waitUntilTrue(() => (curExpiresPerSec + 1) == DelayedRemoteFetch.expiredRequestCount(), "DelayedRemoteFetchMetrics.expiredRequestMeter.count() should be 1, but got: " + DelayedRemoteFetch.expiredRequestCount(), 10000L)
      latch.countDown()
    } finally {
      Utils.tryAll(util.Arrays.asList[Callable[Void]](
        () => {
          replicaManager.shutdown(checkpointHW = false)
          null
        },
        () => {
          remoteLogManager.close()
          null
        }
      ))
    }
  }

  @Test
  def testMultipleRemoteFetchesInOneFetchRequest(): Unit = {
    val replicaId = -1
    val tp0 = new TopicPartition(topic, 0)
    val tp1 = new TopicPartition(topic, 1)
    val tidp0 = new TopicIdPartition(topicId, tp0)
    val tidp1 = new TopicIdPartition(topicId, tp1)
    val replicaManager = setupReplicaManagerWithMockedPurgatories(new MockTimer(time), aliveBrokerIds = Seq(0, 1, 2),
      // Increased the `remote.max.wait.ms` to avoid flaky test due to usage of SystemTimer
      propsModifier = props => props.put(RemoteLogManagerConfig.REMOTE_FETCH_MAX_WAIT_MS_PROP, 120000),
      enableRemoteStorage = true, shouldMockLog = true, remoteFetchQuotaExceeded = Some(false), remoteFetchReaperEnabled = true)
    try {
      val offsetCheckpoints = new LazyOffsetCheckpoints(replicaManager.highWatermarkCheckpoints.asJava)
      replicaManager.createPartition(tp0).createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints, Some(topicId))
      replicaManager.createPartition(tp1).createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints, Some(topicId))

      val leaderEpoch = 0
      val leaderDelta0 = createLeaderDelta(topicId, tp0, leaderId = 0, leaderEpoch = leaderEpoch)
      val leaderDelta1 = createLeaderDelta(topicId, tp1, leaderId = 0, leaderEpoch = leaderEpoch)
      val leaderMetadataImage0 = imageFromTopics(leaderDelta0.apply())
      val leaderMetadataImage1 = imageFromTopics(leaderDelta1.apply())
      replicaManager.applyDelta(leaderDelta0, leaderMetadataImage0)
      replicaManager.applyDelta(leaderDelta1, leaderMetadataImage1)

      val params = new FetchParams(replicaId, 1, 1000, 10, 200000, FetchIsolation.LOG_END, Optional.empty)
      val fetchOffsetTp0 = 1
      val fetchOffsetTp1 = 2

      val responseSeq = new AtomicReference[Seq[(TopicIdPartition, FetchPartitionData)]]()
      val responseLatch = new CountDownLatch(1)

      def fetchCallback(responseStatus: Seq[(TopicIdPartition, FetchPartitionData)]): Unit = {
        responseSeq.set(responseStatus)
        responseLatch.countDown()
      }

      val callbacks: util.Set[Consumer[RemoteLogReadResult]] = new util.HashSet[Consumer[RemoteLogReadResult]]()
      when(mockRemoteLogManager.asyncRead(any(), any())).thenAnswer(ans => {
        callbacks.add(ans.getArgument(1, classOf[Consumer[RemoteLogReadResult]]))
        mock(classOf[Future[Void]])
      })

      // Start the fetch request for both partitions - this should trigger remote fetches since
      // the default mocked log behavior throws OffsetOutOfRangeException
      replicaManager.fetchMessages(params, Seq(
        tidp0 -> new PartitionData(topicId, fetchOffsetTp0, startOffset, 100000, Optional.of[Integer](leaderEpoch), Optional.of[Integer](leaderEpoch)),
        tidp1 -> new PartitionData(topicId, fetchOffsetTp1, startOffset, 100000, Optional.of[Integer](leaderEpoch), Optional.of[Integer](leaderEpoch))
      ), UNBOUNDED_QUOTA, fetchCallback)

      // Verify that exactly two asyncRead calls were made (one for each partition)
      val remoteStorageFetchInfoArg: ArgumentCaptor[RemoteStorageFetchInfo] = ArgumentCaptor.forClass(classOf[RemoteStorageFetchInfo])
      verify(mockRemoteLogManager, times(2)).asyncRead(remoteStorageFetchInfoArg.capture(), any())

      // Verify that remote fetch operations were properly set up for both partitions
      assertTrue(replicaManager.delayedRemoteFetchPurgatory.watched == 2, "DelayedRemoteFetch purgatory should have operations")

      // Verify both partitions were captured in the remote fetch requests
      val capturedFetchInfos = remoteStorageFetchInfoArg.getAllValues.asScala
      assertEquals(2, capturedFetchInfos.size, "Should have 2 remote storage fetch info calls")

      val capturedTopicPartitions = capturedFetchInfos.map(_.topicIdPartition.topicPartition)
      assertEquals(tp0, capturedTopicPartitions.head, "Should contain " + tp0 + " as first item")
      assertTrue(capturedTopicPartitions.contains(tp1), "Should contain " + tp1)

      // Verify the fetch info details are correct for both partitions
      capturedFetchInfos.foreach { fetchInfo =>
        assertEquals(topicId, fetchInfo.fetchInfo.topicId)
        assertEquals(startOffset, fetchInfo.fetchInfo.logStartOffset)
        assertEquals(leaderEpoch, fetchInfo.fetchInfo.currentLeaderEpoch.get())
        if (fetchInfo.topicIdPartition.topicPartition == tp0) {
          assertEquals(fetchOffsetTp0, fetchInfo.fetchInfo.fetchOffset)
          assertTrue(fetchInfo.minOneMessage())
        } else {
          assertEquals(fetchOffsetTp1, fetchInfo.fetchInfo.fetchOffset)
          assertFalse(fetchInfo.minOneMessage())
        }
      }

      // Complete the 2 asyncRead tasks
      callbacks.forEach(callback => callback.accept(buildRemoteReadResult(Errors.NONE)))

      // Wait for the fetch callback to complete and verify responseSeq content
      assertTrue(responseLatch.await(5, TimeUnit.SECONDS), "Fetch callback should complete")

      val responseData = responseSeq.get()
      assertNotNull(responseData, "Response sequence should not be null")
      assertEquals(2, responseData.size, "Response should contain data for both partitions")

      // Verify that response contains both tidp0 and tidp1 and have no errors
      val responseTopicIdPartitions = responseData.map(_._1).toSet
      assertTrue(responseTopicIdPartitions.contains(tidp0), "Response should contain " + tidp0)
      assertTrue(responseTopicIdPartitions.contains(tidp1), "Response should contain " + tidp1)
      responseData.foreach { case (_, fetchPartitionData) =>
        assertEquals(Errors.NONE, fetchPartitionData.error)
      }
      TestUtils.waitUntilTrue(() => replicaManager.delayedRemoteFetchPurgatory.watched == 0,
        "DelayedRemoteFetch purgatory should not have any pending / completed operation")
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testMultipleRemoteFetchCountInOneFetchRequestWhenBreachingFetchMaxBytes(): Unit = {
    val replicaId = -1
    val tp0 = new TopicPartition(topic, 0)
    val tp1 = new TopicPartition(topic, 1)
    val tp2 = new TopicPartition(topic, 2)
    val tidp0 = new TopicIdPartition(topicId, tp0)
    val tidp1 = new TopicIdPartition(topicId, tp1)
    val tidp2 = new TopicIdPartition(topicId, tp2)
    val replicaManager = setupReplicaManagerWithMockedPurgatories(new MockTimer(time), aliveBrokerIds = Seq(0, 1, 2),
      // Increased the `remote.max.wait.ms` to avoid flaky test due to usage of SystemTimer
      propsModifier = props => props.put(RemoteLogManagerConfig.REMOTE_FETCH_MAX_WAIT_MS_PROP, 120000),
      enableRemoteStorage = true, shouldMockLog = true, remoteFetchQuotaExceeded = Some(false), remoteFetchReaperEnabled = true)
    try {
      val offsetCheckpoints = new LazyOffsetCheckpoints(replicaManager.highWatermarkCheckpoints.asJava)
      val leaderEpoch = 0

      def createPartition(tp: TopicPartition): Unit = {
        replicaManager.createPartition(tp).createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints, Some(topicId))
        val leaderDelta = createLeaderDelta(topicId, tp, leaderId = 0, leaderEpoch = leaderEpoch)
        val leaderMetadataImage = imageFromTopics(leaderDelta.apply())
        replicaManager.applyDelta(leaderDelta, leaderMetadataImage)
      }
      createPartition(tp0)
      createPartition(tp1)
      createPartition(tp2)

      val params = new FetchParams(replicaId, 1, 1000, 10, 200000, FetchIsolation.LOG_END, Optional.empty)
      val fetchOffsetTp0 = 1
      val fetchOffsetTp1 = 2
      val fetchOffsetTp2 = 3

      val responseSeq = new AtomicReference[Seq[(TopicIdPartition, FetchPartitionData)]]()
      val responseLatch = new CountDownLatch(1)

      def fetchCallback(responseStatus: Seq[(TopicIdPartition, FetchPartitionData)]): Unit = {
        responseSeq.set(responseStatus)
        responseLatch.countDown()
      }

      val callbacks: util.Set[Consumer[RemoteLogReadResult]] = new util.HashSet[Consumer[RemoteLogReadResult]]()
      when(mockRemoteLogManager.asyncRead(any(), any())).thenAnswer(ans => {
        callbacks.add(ans.getArgument(1, classOf[Consumer[RemoteLogReadResult]]))
        mock(classOf[Future[Void]])
      })

      // Start the fetch request for 3 partitions - this should trigger remote fetches since the default mocked log
      // behavior throws OffsetOutOfRangeException
      replicaManager.fetchMessages(params, Seq(
        tidp0 -> new PartitionData(topicId, fetchOffsetTp0, startOffset, 100000, Optional.of[Integer](leaderEpoch), Optional.of[Integer](leaderEpoch)),
        tidp1 -> new PartitionData(topicId, fetchOffsetTp1, startOffset, 100000, Optional.of[Integer](leaderEpoch), Optional.of[Integer](leaderEpoch)),
        tidp2 -> new PartitionData(topicId, fetchOffsetTp2, startOffset, 100000, Optional.of[Integer](leaderEpoch), Optional.of[Integer](leaderEpoch))
      ), UNBOUNDED_QUOTA, fetchCallback)

      // Verify that exactly two asyncRead calls were made.
      // tidp2 should not send remote request since the FETCH max bytes limit is already exceeded.
      val remoteStorageFetchInfoArg: ArgumentCaptor[RemoteStorageFetchInfo] = ArgumentCaptor.forClass(classOf[RemoteStorageFetchInfo])
      verify(mockRemoteLogManager, times(2)).asyncRead(remoteStorageFetchInfoArg.capture(), any())

      // Verify that remote fetch operations were properly set up for both partitions
      assertTrue(replicaManager.delayedRemoteFetchPurgatory.watched == 2, "DelayedRemoteFetch purgatory should have operations")

      // Verify both partitions were captured in the remote fetch requests
      val capturedFetchInfos = remoteStorageFetchInfoArg.getAllValues.asScala
      assertEquals(2, capturedFetchInfos.size, "Should have 2 remote storage fetch info calls")

      val capturedTopicPartitions = capturedFetchInfos.map(_.topicIdPartition.topicPartition)
      assertEquals(tp0, capturedTopicPartitions.head, "Should contain " + tp0 + " as first item")
      assertTrue(capturedTopicPartitions.contains(tp1), "Should contain " + tp1)

      // Verify the fetch info details are correct for both partitions
      capturedFetchInfos.foreach { fetchInfo =>
        assertEquals(topicId, fetchInfo.fetchInfo.topicId)
        assertEquals(startOffset, fetchInfo.fetchInfo.logStartOffset)
        assertEquals(leaderEpoch, fetchInfo.fetchInfo.currentLeaderEpoch.get())
        if (fetchInfo.topicIdPartition.topicPartition == tp0) {
          assertEquals(fetchOffsetTp0, fetchInfo.fetchInfo.fetchOffset)
          assertTrue(fetchInfo.minOneMessage())
        } else {
          assertEquals(fetchOffsetTp1, fetchInfo.fetchInfo.fetchOffset)
          assertFalse(fetchInfo.minOneMessage())
        }
      }

      // Complete the 2 asyncRead tasks
      callbacks.forEach(callback => callback.accept(buildRemoteReadResult(Errors.NONE)))

      // Wait for the fetch callback to complete and verify responseSeq content
      assertTrue(responseLatch.await(5, TimeUnit.SECONDS), "Fetch callback should complete")

      val responseData = responseSeq.get()
      assertNotNull(responseData, "Response sequence should not be null")
      assertEquals(3, responseData.size, "Response should contain data for both partitions")

      // Verify that response contains all partitions (tidp0, tidp1, and tidp2) and have no errors
      val responseTopicIdPartitions = responseData.map(_._1).toSet
      assertTrue(responseTopicIdPartitions.contains(tidp0), "Response should contain " + tidp0)
      assertTrue(responseTopicIdPartitions.contains(tidp1), "Response should contain " + tidp1)
      assertTrue(responseTopicIdPartitions.contains(tidp2), "Response should contain " + tidp2)
      responseData.foreach { case (_, fetchPartitionData) =>
        assertEquals(Errors.NONE, fetchPartitionData.error)
      }
      TestUtils.waitUntilTrue(() => replicaManager.delayedRemoteFetchPurgatory.watched == 0,
        "DelayedRemoteFetch purgatory should not have any pending / completed operation")
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  private def buildRemoteReadResult(error: Errors): RemoteLogReadResult = {
    new RemoteLogReadResult(
      Optional.of(new FetchDataInfo(LogOffsetMetadata.UNKNOWN_OFFSET_METADATA, MemoryRecords.EMPTY)),
      if (error != Errors.NONE) Optional.of[Throwable](error.exception) else Optional.empty[Throwable]())
  }

  private def yammerMetricValue(name: String): Any = {
    val allMetrics = KafkaYammerMetrics.defaultRegistry.allMetrics.asScala
    val (_, metric) = allMetrics.find { case (n, _) => n.getMBeanName.endsWith(name) }
      .getOrElse(fail(s"Unable to find broker metric $name: allMetrics: ${allMetrics.keySet.map(_.getMBeanName)}"))
    metric match {
      case m: Gauge[_] => m.value
      case m: Meter => m.count()
      case m: Timer => m.count()
      case m => fail(s"Unexpected broker metric of class ${m.getClass}")
    }
  }

  @Test
  def testSuccessfulBuildRemoteLogAuxStateMetrics(): Unit = {
    val remoteLogManager = mock(classOf[RemoteLogManager])
    val remoteLogSegmentMetadata = mock(classOf[RemoteLogSegmentMetadata])
    when(remoteLogManager.fetchRemoteLogSegmentMetadata(any(), anyInt(), anyLong())).thenReturn(
      Optional.of(remoteLogSegmentMetadata)
    )
    val storageManager = mock(classOf[RemoteStorageManager])
    when(storageManager.fetchIndex(any(), any())).thenReturn(new ByteArrayInputStream("0".getBytes()))
    when(remoteLogManager.storageManager()).thenReturn(storageManager)
    when(remoteLogManager.isPartitionReady(any())).thenReturn(true)

    val replicaManager = setupReplicaManagerWithMockedPurgatories(new MockTimer(time), aliveBrokerIds = Seq(0, 1, 2), enableRemoteStorage = true, shouldMockLog = true, remoteLogManager = Some(remoteLogManager), buildRemoteLogAuxState = true)
    try {
      val offsetCheckpoints = new LazyOffsetCheckpoints(replicaManager.highWatermarkCheckpoints.asJava)
      replicaManager.createPartition(topicPartition).createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints, Some(topicId))
      val partition0Replicas = Seq[Integer](0, 1).asJava

      // Verify the metrics for build remote log state and for failures is zero before replicas start to fetch
      assertEquals(0, brokerTopicStats.topicStats(topicPartition.topic()).buildRemoteLogAuxStateRequestRate.count)
      assertEquals(0, brokerTopicStats.topicStats(topicPartition.topic()).failedBuildRemoteLogAuxStateRate.count)
      // Verify aggregate metrics
      assertEquals(0, brokerTopicStats.allTopicsStats.buildRemoteLogAuxStateRequestRate.count)
      assertEquals(0, brokerTopicStats.allTopicsStats.failedBuildRemoteLogAuxStateRate.count)

      val leaderDelta = createLeaderDelta(topicId, topicPartition, leaderId = 1, replicas = partition0Replicas, isr = partition0Replicas)
      val leaderMetadataImage = imageFromTopics(leaderDelta.apply())
      replicaManager.applyDelta(leaderDelta, leaderMetadataImage)

      // Replicas fetch from the leader periodically, therefore we check that the metric value is increasing
      waitUntilTrue(() => brokerTopicStats.topicStats(topicPartition.topic()).buildRemoteLogAuxStateRequestRate.count > 0,
        "Should have buildRemoteLogAuxStateRequestRate count > 0, but got:" + brokerTopicStats.topicStats(topicPartition.topic()).buildRemoteLogAuxStateRequestRate.count)
      assertEquals(0, brokerTopicStats.topicStats(topicPartition.topic()).failedBuildRemoteLogAuxStateRate.count)
      // Verify aggregate metrics
      waitUntilTrue(() => brokerTopicStats.allTopicsStats.buildRemoteLogAuxStateRequestRate.count > 0,
        "Should have all topic buildRemoteLogAuxStateRequestRate count > 0, but got:" + brokerTopicStats.allTopicsStats.buildRemoteLogAuxStateRequestRate.count)
      assertEquals(0, brokerTopicStats.allTopicsStats.failedBuildRemoteLogAuxStateRate.count)
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testFailedBuildRemoteLogAuxStateMetrics(): Unit = {
    val tp0 = new TopicPartition(topic, 0)

    val remoteLogManager = mock(classOf[RemoteLogManager])
    val storageManager = mock(classOf[RemoteStorageManager])
    when(storageManager.fetchIndex(any(), any())).thenReturn(new ByteArrayInputStream("0".getBytes()))
    when(remoteLogManager.storageManager()).thenReturn(storageManager)

    val replicaManager = setupReplicaManagerWithMockedPurgatories(new MockTimer(time), aliveBrokerIds = Seq(0, 1, 2), enableRemoteStorage = true, shouldMockLog = true, remoteLogManager = Some(remoteLogManager), buildRemoteLogAuxState = true)
    try {
      val offsetCheckpoints = new LazyOffsetCheckpoints(replicaManager.highWatermarkCheckpoints.asJava)
      replicaManager.createPartition(tp0).createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints, Some(topicId))

      // Verify the metrics for build remote log state and for failures is zero before replicas start to fetch
      assertEquals(0, brokerTopicStats.topicStats(tp0.topic()).buildRemoteLogAuxStateRequestRate.count)
      assertEquals(0, brokerTopicStats.topicStats(tp0.topic()).failedBuildRemoteLogAuxStateRate.count)
      // Verify aggregate metrics
      assertEquals(0, brokerTopicStats.allTopicsStats.buildRemoteLogAuxStateRequestRate.count)
      assertEquals(0, brokerTopicStats.allTopicsStats.failedBuildRemoteLogAuxStateRate.count)

      val brokerList = Seq[Integer](0, 1).asJava
      val delta = createLeaderDelta(topicId, new TopicPartition(topic, 0), brokerList.get(1), brokerList, brokerList)
      val leaderMetadataImage = imageFromTopics(delta.apply())
      replicaManager.applyDelta(delta, leaderMetadataImage)

      // Replicas fetch from the leader periodically, therefore we check that the metric value is increasing
      // We expect failedBuildRemoteLogAuxStateRate to increase because the RemoteLogManager is not ready for the tp0
      // when attempting to build log aux state
      TestUtils.waitUntilTrue(() => brokerTopicStats.topicStats(tp0.topic()).buildRemoteLogAuxStateRequestRate.count > 0,
        "Should have buildRemoteLogAuxStateRequestRate count > 0, but got:" + brokerTopicStats.topicStats(tp0.topic()).buildRemoteLogAuxStateRequestRate.count)
      TestUtils.waitUntilTrue(() => brokerTopicStats.topicStats(tp0.topic()).failedBuildRemoteLogAuxStateRate.count > 0,
        "Should have failedBuildRemoteLogAuxStateRate count > 0, but got:" + brokerTopicStats.topicStats(tp0.topic()).failedBuildRemoteLogAuxStateRate.count)
      // Verify aggregate metrics
      TestUtils.waitUntilTrue(() => brokerTopicStats.allTopicsStats.buildRemoteLogAuxStateRequestRate.count > 0,
        "Should have all topic buildRemoteLogAuxStateRequestRate count > 0, but got:" + brokerTopicStats.allTopicsStats.buildRemoteLogAuxStateRequestRate.count)
      TestUtils.waitUntilTrue(() => brokerTopicStats.allTopicsStats.failedBuildRemoteLogAuxStateRate.count > 0,
        "Should have all topic failedBuildRemoteLogAuxStateRate count > 0, but got:" + brokerTopicStats.allTopicsStats.failedBuildRemoteLogAuxStateRate.count)
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testBuildRemoteLogAuxStateMetricsThrowsException(): Unit = {
    val tp0 = new TopicPartition(topic, 0)

    val remoteLogManager = mock(classOf[RemoteLogManager])
    when(remoteLogManager.fetchRemoteLogSegmentMetadata(any(), anyInt(), anyLong())).thenThrow(new RemoteStorageException("Failed to build remote log aux"))

    val storageManager = mock(classOf[RemoteStorageManager])
    when(storageManager.fetchIndex(any(), any())).thenReturn(new ByteArrayInputStream("0".getBytes()))
    when(remoteLogManager.storageManager()).thenReturn(storageManager)

    val replicaManager = setupReplicaManagerWithMockedPurgatories(new MockTimer(time), aliveBrokerIds = Seq(0, 1, 2), enableRemoteStorage = true, shouldMockLog = true, remoteLogManager = Some(remoteLogManager), buildRemoteLogAuxState = true)
    try {
      val offsetCheckpoints = new LazyOffsetCheckpoints(replicaManager.highWatermarkCheckpoints.asJava)
      replicaManager.createPartition(tp0).createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints, Some(topicId))
      val partition0Replicas = Seq[Integer](0, 1).asJava

      // Verify the metrics for build remote log state and for failures is zero before replicas start to fetch
      assertEquals(0, brokerTopicStats.topicStats(tp0.topic()).buildRemoteLogAuxStateRequestRate.count)
      assertEquals(0, brokerTopicStats.topicStats(tp0.topic()).failedBuildRemoteLogAuxStateRate.count)
      // Verify aggregate metrics
      assertEquals(0, brokerTopicStats.allTopicsStats.buildRemoteLogAuxStateRequestRate.count)
      assertEquals(0, brokerTopicStats.allTopicsStats.failedBuildRemoteLogAuxStateRate.count)

      val leaderDelta = createLeaderDelta(topicId, tp0, leaderId = 1, replicas = partition0Replicas, isr = partition0Replicas)
      val leaderMetadataImage = imageFromTopics(leaderDelta.apply())
      replicaManager.applyDelta(leaderDelta, leaderMetadataImage)

      // Replicas fetch from the leader periodically, therefore we check that the metric value is increasing
      // We expect failedBuildRemoteLogAuxStateRate to increase because fetchRemoteLogSegmentMetadata returns RemoteStorageException
      TestUtils.waitUntilTrue(() => brokerTopicStats.topicStats(tp0.topic()).buildRemoteLogAuxStateRequestRate.count > 0,
        "Should have buildRemoteLogAuxStateRequestRate count > 0, but got:" + brokerTopicStats.topicStats(tp0.topic()).buildRemoteLogAuxStateRequestRate.count)
      TestUtils.waitUntilTrue(() => brokerTopicStats.topicStats(tp0.topic()).failedBuildRemoteLogAuxStateRate.count > 0,
        "Should have failedBuildRemoteLogAuxStateRate count > 0, but got:" + brokerTopicStats.topicStats(tp0.topic()).failedBuildRemoteLogAuxStateRate.count)
      // Verify aggregate metrics
      TestUtils.waitUntilTrue(() => brokerTopicStats.allTopicsStats.buildRemoteLogAuxStateRequestRate.count > 0,
        "Should have all topic buildRemoteLogAuxStateRequestRate count > 0, but got:" + brokerTopicStats.allTopicsStats.buildRemoteLogAuxStateRequestRate.count)
      TestUtils.waitUntilTrue(() => brokerTopicStats.allTopicsStats.failedBuildRemoteLogAuxStateRate.count > 0,
        "Should have all topic failedBuildRemoteLogAuxStateRate count > 0, but got:" + brokerTopicStats.allTopicsStats.failedBuildRemoteLogAuxStateRate.count)
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  private def setupMockLog(path: String, logConfig: LogConfig, enableRemoteStorage: Boolean, topicPartition: TopicPartition, topicId: Option[Uuid]): UnifiedLog = {
    val mockLog = mock(classOf[UnifiedLog])
    val partitionDir = new File(path, s"$topicPartition")
    partitionDir.mkdir()
    when(mockLog.dir).thenReturn(partitionDir)
    when(mockLog.parentDir).thenReturn(path)
    when(mockLog.topicId).thenReturn(topicId.toJava)
    when(mockLog.topicPartition).thenReturn(topicPartition)
    when(mockLog.highWatermark).thenReturn(highHW)
    when(mockLog.updateHighWatermark(anyLong())).thenReturn(0L)
    when(mockLog.logEndOffsetMetadata).thenReturn(new LogOffsetMetadata(10))
    when(mockLog.maybeIncrementHighWatermark(any(classOf[LogOffsetMetadata]))).thenReturn(Optional.empty)
    when(mockLog.endOffsetForEpoch(anyInt())).thenReturn(Optional.empty)
    // try to return a high start offset to cause OffsetOutOfRangeException at the 1st time
    when(mockLog.logStartOffset).thenReturn(endOffset).thenReturn(startOffset)
    when(mockLog.logEndOffset).thenReturn(endOffset)
    when(mockLog.localLogStartOffset()).thenReturn(endOffset - 10)
    when(mockLog.leaderEpochCache).thenReturn(mock(classOf[LeaderEpochFileCache]))
    when(mockLog.latestEpoch).thenReturn(Optional.of(0))
    val producerStateManager = mock(classOf[ProducerStateManager])
    when(mockLog.producerStateManager).thenReturn(producerStateManager)
    when(mockLog.config).thenReturn(logConfig)
    when(mockLog.remoteLogEnabled()).thenReturn(enableRemoteStorage)

    mockLog
  }

  @Test
  def testReplicaNotAvailable(): Unit = {

    def createReplicaManager(): ReplicaManager = {
      val props = TestUtils.createBrokerConfig(1)
      val config = KafkaConfig.fromProps(props)
      val mockLogMgr = TestUtils.createLogManager(config.logDirs.asScala.map(new File(_)))
      new ReplicaManager(
        metrics = metrics,
        config = config,
        time = time,
        scheduler = new MockScheduler(time),
        logManager = mockLogMgr,
        quotaManagers = quotaManager,
        metadataCache = new KRaftMetadataCache(config.brokerId, () => KRaftVersion.KRAFT_VERSION_0),
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
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testPartitionMetadataFile(): Unit = {
    val replicaManager = setupReplicaManagerWithMockedPurgatories(new MockTimer(time))
    try {
      val leaderDelta = topicsCreateDelta(0, isStartIdLeader = true, partitions = List(0),
        topicName = topic, topicId = topicIds(topic))
      val leaderImage = imageFromTopics(leaderDelta.apply())
      replicaManager.applyDelta(leaderDelta, leaderImage)

      assertTrue(replicaManager.getPartition(topicPartition).isInstanceOf[HostedPartition.Online])
      assertFalse(replicaManager.localLog(topicPartition).isEmpty)
      val id = topicIds(topicPartition.topic)
      val log = replicaManager.localLog(topicPartition).get
      assertTrue(log.partitionMetadataFile.get.exists())
      val partitionMetadata = log.partitionMetadataFile.get.read()

      // Current version of PartitionMetadataFile is 0.
      assertEquals(0, partitionMetadata.version)
      assertEquals(id, partitionMetadata.topicId)
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testInconsistentIdReturnsError(): Unit = {
    val replicaManager = setupReplicaManagerWithMockedPurgatories(new MockTimer(time))
    try {
      val invalidTopicId = Uuid.randomUuid()

      val initialDelta = topicsCreateDelta(0, isStartIdLeader = true,
        partitions = List(0), topicName = topic, topicId = topicIds(topic))
      val initialImage = imageFromTopics(initialDelta.apply())
      replicaManager.applyDelta(initialDelta, initialImage)

      val updateDelta = topicsCreateDelta(0, isStartIdLeader = true,
        partitions = List(0), topicName = topic, topicId = topicIds(topic), leaderEpoch = 1)
      val updateImage = imageFromTopics(updateDelta.apply())
      replicaManager.applyDelta(updateDelta, updateImage)

      // Send request with inconsistent ID.
      val inconsistentDelta1 = topicsCreateDelta(0, isStartIdLeader = true,
        partitions = List(0), topicName = topic, topicId = invalidTopicId, leaderEpoch = 1)
      val inconsistentImage1 = imageFromTopics(inconsistentDelta1.apply())
      val exception1 = assertThrows(classOf[IllegalStateException], () => {
        replicaManager.applyDelta(inconsistentDelta1, inconsistentImage1)
      })
      assertEquals(s"Topic ${topic}-0 exists, but its ID is ${topicId}, not ${invalidTopicId} as expected", exception1.getMessage)

      val inconsistentDelta2 = topicsCreateDelta(0, isStartIdLeader = true,
        partitions = List(0), topicName = topic, topicId = invalidTopicId, leaderEpoch = 2)
      val inconsistentImage2 = imageFromTopics(inconsistentDelta2.apply())
      val exception2 = assertThrows(classOf[IllegalStateException], () => {
        replicaManager.applyDelta(inconsistentDelta2, inconsistentImage2)
      })
      assertEquals(s"Topic ${topic}-0 exists, but its ID is ${topicId}, not ${invalidTopicId} as expected", exception2.getMessage)

    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testPartitionMetadataFileCreated(): Unit = {
    val replicaManager = setupReplicaManagerWithMockedPurgatories(new MockTimer(time))
    try {
      val brokerList = Seq[Integer](0, 1).asJava
      val topicPartition = new TopicPartition(topic, 0)

      val leaderDelta = createLeaderDelta(
        topicId = Uuid.ZERO_UUID,
        partition = topicPartition,
        leaderId = 0,
        replicas = brokerList,
        isr = brokerList,
      )

      // The file exists if the topic has the default UUID.
      replicaManager.applyDelta(leaderDelta, imageFromTopics(leaderDelta.apply()))
      assertTrue(replicaManager.localLog(topicPartition).isDefined)
      val log = replicaManager.localLog(topicPartition).get
      assertTrue(log.partitionMetadataFile.get.exists())

    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testPartitionMarkedOfflineIfLogCantBeCreated(becomeLeader: Boolean): Unit = {
    val dataDir = TestUtils.tempDir()
    val topicPartition = new TopicPartition(topic, 0)
    val replicaManager = setupReplicaManagerWithMockedPurgatories(
      timer = new MockTimer(time),
      propsModifier = props => props.put(ServerLogConfigs.LOG_DIRS_CONFIG, dataDir.getAbsolutePath)
    )

    try {
      // Delete the data directory to trigger a storage exception
      Utils.delete(dataDir)

      val leaderDelta = createLeaderDelta(
        topicId = topicId,
        partition = topicPartition,
        leaderId = if (becomeLeader) 0 else 1,
        replicas = util.Arrays.asList(0 , 1),
        isr = util.Arrays.asList(0, 1),
      )
      replicaManager.applyDelta(leaderDelta, imageFromTopics(leaderDelta.apply()))

      val hostedPartition = replicaManager.getPartition(topicPartition)
      assertEquals(
        classOf[HostedPartition.Offline],
        hostedPartition.getClass
      )
      assertEquals(
        topicId,
        hostedPartition.asInstanceOf[HostedPartition.Offline].partition.flatMap(p => p.topicId).get
      )
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
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

      val barPartition = new TopicPartition("bar", 0)
      val barTopicId = Uuid.randomUuid()

      val leaderDelta = createLeaderDelta(barTopicId, barPartition, brokerId)
      val leaderMetadataImage = imageFromTopics(leaderDelta.apply())
      replicaManager.applyDelta(leaderDelta, leaderMetadataImage)

      val barProducerState = replicaManager.activeProducerState(barPartition)
      assertEquals(Errors.NONE, Errors.forCode(barProducerState.errorCode))

      val bazPartition = new TopicPartition("baz", 0)
      val bazTopicId = Uuid.randomUuid()
      val otherBrokerId = 1

      val followerDelta = createFollowerDelta(bazTopicId, bazPartition, brokerId, otherBrokerId)
      val followerMetadataImage = imageFromTopics(followerDelta.apply())
      replicaManager.applyDelta(followerDelta, followerMetadataImage)

      val bazProducerState = replicaManager.activeProducerState(bazPartition)
      assertEquals(Errors.NONE, Errors.forCode(bazProducerState.errorCode))

    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  private def createLeaderDelta(
    topicId: Uuid,
    partition: TopicPartition,
    leaderId: Integer,
    replicas: util.List[Integer] = null,
    isr: util.List[Integer] = null,
    leaderEpoch: Int = 0): TopicsDelta = {
    val delta = new TopicsDelta(TopicsImage.EMPTY)
    val effectiveReplicas = Option(replicas).getOrElse(java.util.List.of(leaderId))
    val effectiveIsr = Option(isr).getOrElse(java.util.List.of(leaderId))

    delta.replay(new TopicRecord()
      .setName(partition.topic)
      .setTopicId(topicId)
    )

    delta.replay(new PartitionRecord()
      .setPartitionId(partition.partition)
      .setTopicId(topicId)
      .setReplicas(effectiveReplicas)
      .setIsr(effectiveIsr)
      .setLeader(leaderId)
      .setLeaderEpoch(leaderEpoch)
      .setPartitionEpoch(0)
    )

    delta
  }

  private def createFollowerDelta(
    topicId: Uuid,
    partition: TopicPartition,
    followerId: Int,
    leaderId: Int,
    leaderEpoch: Int = 0): TopicsDelta = {
    val delta = new TopicsDelta(TopicsImage.EMPTY)

    delta.replay(new TopicRecord()
      .setName(partition.topic)
      .setTopicId(topicId)
    )

    delta.replay(new PartitionRecord()
      .setPartitionId(partition.partition)
      .setTopicId(topicId)
      .setReplicas(util.Arrays.asList(followerId, leaderId))
      .setIsr(util.Arrays.asList(followerId, leaderId))
      .setLeader(leaderId)
      .setLeaderEpoch(leaderEpoch)
      .setPartitionEpoch(0)
    )

    delta
  }

  val FOO_UUID = Uuid.fromString("fFJBx0OmQG-UqeaT6YaSwA")

  val BAR_UUID = Uuid.fromString("vApAP6y7Qx23VOfKBzbOBQ")

  @Test
  def testGetOrCreatePartition(): Unit = {
    val brokerId = 0
    val replicaManager = setupReplicaManagerWithMockedPurgatories(new MockTimer(time), brokerId, shouldMockLog = true)
    try {
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
      val (barPart, barNew) = replicaManager.getOrCreatePartition(bar1, emptyDelta, BAR_UUID).get
      assertTrue(barNew)
      assertEquals(bar1, barPart.topicPartition)

      val mockLog = mock(classOf[UnifiedLog])
      when(replicaManager.logManager.getLog(bar1)).thenReturn(Some(mockLog))
      when(mockLog.topicId).thenReturn(Optional.of(BAR_UUID))
      replicaManager.markPartitionOffline(bar1)

      assertTrue(replicaManager.getOrCreatePartition(bar1, emptyDelta, BAR_UUID).isEmpty)
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testGetOrCreatePartitionShouldNotCreateOfflinePartition(): Unit = {
    val localId = 1
    val topicPartition0 = new TopicIdPartition(FOO_UUID, 0, "foo")
    val directoryEventHandler = mock(classOf[DirectoryEventHandler])
    val aliveBrokerIds = Array(1, 2)

    val replicaManager = setupReplicaManagerWithMockedPurgatories(new MockTimer(time), localId, setupLogDirMetaProperties = true, directoryEventHandler = directoryEventHandler)
    try {
      val directoryIds = replicaManager.logManager.directoryIdsSet.toList
      assertEquals(directoryIds.size, 2)
      val leaderTopicsDelta: TopicsDelta = topicsCreateDelta(localId, true, partitions = List(0), directoryIds = directoryIds)
      val (partition: Partition, isNewWhenCreatedForFirstTime: Boolean) = replicaManager.getOrCreatePartition(topicPartition0.topicPartition(), leaderTopicsDelta, FOO_UUID).get
      partition.makeLeader(partitionRegistration(localId, 1, aliveBrokerIds, partitionEpoch, aliveBrokerIds),
        isNew = false,
        new LazyOffsetCheckpoints(replicaManager.highWatermarkCheckpoints.asJava),
        None)

      assertTrue(isNewWhenCreatedForFirstTime)
      // mark topic partition as offline
      replicaManager.markPartitionOffline(topicPartition0.topicPartition())

      // recreate the partition again shouldn't create new partition
      val recreateResults = replicaManager.getOrCreatePartition(topicPartition0.topicPartition(), leaderTopicsDelta, FOO_UUID)
      assertTrue(recreateResults.isEmpty)
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  private def verifyRLMOnLeadershipChange(leaderPartitions: util.Set[Partition], followerPartitions: util.Set[Partition]): Unit = {
    val leaderCapture: ArgumentCaptor[util.Set[TopicPartitionLog]] = ArgumentCaptor.forClass(classOf[util.Set[TopicPartitionLog]])
    val followerCapture: ArgumentCaptor[util.Set[TopicPartitionLog]] = ArgumentCaptor.forClass(classOf[util.Set[TopicPartitionLog]])
    val topicIdsCapture: ArgumentCaptor[util.Map[String, Uuid]] = ArgumentCaptor.forClass(classOf[util.Map[String, Uuid]])
    verify(mockRemoteLogManager).onLeadershipChange(leaderCapture.capture(), followerCapture.capture(), topicIdsCapture.capture())

    val actualLeaderPartitions = leaderCapture.getValue
    val actualFollowerPartitions = followerCapture.getValue

    assertEquals(leaderPartitions, actualLeaderPartitions)
    assertEquals(followerPartitions, actualFollowerPartitions)
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testApplyDeltaShouldHandleReplicaAssignedToOnlineDirectory(enableRemoteStorage: Boolean): Unit = {
    val localId = 1
    val topicPartition0 = new TopicPartition("foo", 0)
    val topicPartition1 = new TopicPartition("foo", 1)
    val directoryEventHandler = mock(classOf[DirectoryEventHandler])

    val replicaManager = setupReplicaManagerWithMockedPurgatories(new MockTimer(time), localId,
      enableRemoteStorage = enableRemoteStorage, setupLogDirMetaProperties = true, directoryEventHandler = directoryEventHandler)

    try {

      // Test applying delta as leader
      val directoryIds = replicaManager.logManager.directoryIdsSet.toList
      // Make the local replica the leader
      val leaderTopicsDelta = topicsCreateDelta(localId, true, partitions = List(0), directoryIds = directoryIds)
      val leaderMetadataImage = imageFromTopics(leaderTopicsDelta.apply())
      replicaManager.applyDelta(leaderTopicsDelta, leaderMetadataImage)

      // Check the broker shouldn't updated the controller with the correct assignment.
      verifyNoInteractions(replicaManager.directoryEventHandler)
      val logDirIdHostingPartition0 = replicaManager.logManager.directoryId(replicaManager.logManager.getLog(topicPartition0).get.dir.getParent).get
      assertEquals(directoryIds.head, logDirIdHostingPartition0)

      // Test applying delta as follower
      val followerTopicsDelta = topicsCreateDelta(localId, false, partitions = List(1), directoryIds = directoryIds)
      val followerMetadataImage = imageFromTopics(followerTopicsDelta.apply())

      replicaManager.applyDelta(followerTopicsDelta, followerMetadataImage)

      // Check the broker shouldn't updated the controller with the correct assignment.
      verifyNoInteractions(replicaManager.directoryEventHandler)
      val logDirIdHostingPartition1 = replicaManager.logManager.directoryId(replicaManager.logManager.getLog(topicPartition1).get.dir.getParent).get
      assertEquals(directoryIds.head, logDirIdHostingPartition1)

    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testApplyDeltaShouldHandleReplicaAssignedToUnassignedDirectory(enableRemoteStorage: Boolean): Unit = {
    val localId = 1
    val topicPartition0 = new TopicPartition("foo", 0)
    val topicPartition1 = new TopicPartition("foo", 1)
    val directoryEventHandler = mock(classOf[DirectoryEventHandler])

    val replicaManager = setupReplicaManagerWithMockedPurgatories(new MockTimer(time), localId,
      enableRemoteStorage = enableRemoteStorage, setupLogDirMetaProperties = true, directoryEventHandler = directoryEventHandler)

    try {
      // Make the local replica the leader
      val leaderTopicsDelta = topicsCreateDelta(localId, true, partitions = List(0), directoryIds = List(DirectoryId.UNASSIGNED, DirectoryId.UNASSIGNED))
      val leaderMetadataImage = imageFromTopics(leaderTopicsDelta.apply())
      val topicId = leaderMetadataImage.topics().topicsByName.get("foo").id
      val topicIdPartition0 = new TopicIdPartition(topicId, topicPartition0)
      val topicIdPartition1 = new TopicIdPartition(topicId, topicPartition1)
      replicaManager.applyDelta(leaderTopicsDelta, leaderMetadataImage)

      // Make the local replica the as follower
      val followerTopicsDelta = topicsCreateDelta(localId, false, partitions = List(1), directoryIds = List(DirectoryId.UNASSIGNED, DirectoryId.UNASSIGNED))
      val followerMetadataImage = imageFromTopics(followerTopicsDelta.apply())
      replicaManager.applyDelta(followerTopicsDelta, followerMetadataImage)

      // Check the broker updated the controller with the correct assignment.
      val topicIdPartitionCapture: ArgumentCaptor[org.apache.kafka.server.common.TopicIdPartition] =
        ArgumentCaptor.forClass(classOf[org.apache.kafka.server.common.TopicIdPartition])
      val logIdCaptureForPartition: ArgumentCaptor[Uuid] = ArgumentCaptor.forClass(classOf[Uuid])
      verify(replicaManager.directoryEventHandler, atLeastOnce()).handleAssignment(topicIdPartitionCapture.capture(), logIdCaptureForPartition.capture(),
        ArgumentMatchers.eq("Applying metadata delta"), any())

      assertEquals(topicIdPartitionCapture.getAllValues.asScala,
        List(
          new org.apache.kafka.server.common.TopicIdPartition(topicId, topicIdPartition0.partition()),
          new org.apache.kafka.server.common.TopicIdPartition(topicId, topicIdPartition1.partition())
        )
      )
      val logDirIdHostingPartition0 = replicaManager.logManager.directoryId(replicaManager.logManager.getLog(topicPartition0).get.dir.getParent).get
      val logDirIdHostingPartition1 = replicaManager.logManager.directoryId(replicaManager.logManager.getLog(topicPartition1).get.dir.getParent).get
      assertEquals(logIdCaptureForPartition.getAllValues.asScala, List(logDirIdHostingPartition0, logDirIdHostingPartition1))
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testApplyDeltaShouldHandleReplicaAssignedToLostDirectory(enableRemoteStorage: Boolean): Unit = {
    val localId = 1
    val topicPartition0 = new TopicPartition("foo", 0)
    val topicPartition1 = new TopicPartition("foo", 1)
    val directoryEventHandler = mock(classOf[DirectoryEventHandler])

    val replicaManager = setupReplicaManagerWithMockedPurgatories(new MockTimer(time), localId,
      enableRemoteStorage = enableRemoteStorage, setupLogDirMetaProperties = true, directoryEventHandler = directoryEventHandler)

    try {
      // Make the local replica the leader
      val leaderTopicsDelta = topicsCreateDelta(localId, true, directoryIds = List(DirectoryId.LOST, DirectoryId.LOST))
      val leaderMetadataImage = imageFromTopics(leaderTopicsDelta.apply())
      val topicId = leaderMetadataImage.topics().topicsByName.get("foo").id
      val topicIdPartition0 = new TopicIdPartition(topicId, topicPartition0)
      val topicIdPartition1 = new TopicIdPartition(topicId, topicPartition1)
      replicaManager.applyDelta(leaderTopicsDelta, leaderMetadataImage)

      // Make the local replica the as follower
      val followerTopicsDelta = topicsCreateDelta(localId, false, partitions = List(1), directoryIds = List(DirectoryId.LOST, DirectoryId.LOST))
      val followerMetadataImage = imageFromTopics(followerTopicsDelta.apply())
      replicaManager.applyDelta(followerTopicsDelta, followerMetadataImage)

      // Check the broker updated the controller with the correct assignment.
      val topicIdPartitionCapture: ArgumentCaptor[org.apache.kafka.server.common.TopicIdPartition] =
        ArgumentCaptor.forClass(classOf[org.apache.kafka.server.common.TopicIdPartition])
      val logIdCaptureForPartition: ArgumentCaptor[Uuid] = ArgumentCaptor.forClass(classOf[Uuid])
      verify(replicaManager.directoryEventHandler, atLeastOnce()).handleAssignment(topicIdPartitionCapture.capture(), logIdCaptureForPartition.capture(),
        ArgumentMatchers.eq("Applying metadata delta"), any())

      assertEquals(topicIdPartitionCapture.getAllValues.asScala,
        List(
          new org.apache.kafka.server.common.TopicIdPartition(topicId, topicIdPartition0.partition()),
          new org.apache.kafka.server.common.TopicIdPartition(topicId, topicIdPartition1.partition())
        )
      )
      val logDirIdHostingPartition0 = replicaManager.logManager.directoryId(replicaManager.logManager.getLog(topicPartition0).get.dir.getParent).get
      val logDirIdHostingPartition1 = replicaManager.logManager.directoryId(replicaManager.logManager.getLog(topicPartition1).get.dir.getParent).get
      assertEquals(logIdCaptureForPartition.getAllValues.asScala, List(logDirIdHostingPartition0, logDirIdHostingPartition1))

    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testDeltaFromLeaderToFollower(enableRemoteStorage: Boolean): Unit = {
    val localId = 1
    val otherId = localId + 1
    val numOfRecords = 3
    val topicPartition = new TopicPartition("foo", 0)
    val replicaManager = setupReplicaManagerWithMockedPurgatories(new MockTimer(time), localId, enableRemoteStorage = enableRemoteStorage)

    try {
      // Make the local replica the leader
      val leaderTopicsDelta = topicsCreateDelta(localId, true)
      val leaderMetadataImage = imageFromTopics(leaderTopicsDelta.apply())
      val topicIdPartition = new TopicIdPartition(FOO_UUID, topicPartition)

      replicaManager.applyDelta(leaderTopicsDelta, leaderMetadataImage)

      // Check the state of that partition and fetcher
      val HostedPartition.Online(leaderPartition) = replicaManager.getPartition(topicPartition)
      assertTrue(leaderPartition.isLeader)
      assertEquals(Set(localId, otherId), leaderPartition.inSyncReplicaIds)
      assertEquals(0, leaderPartition.getLeaderEpoch)

      assertEquals(None, replicaManager.replicaFetcherManager.getFetcher(topicPartition))

      if (enableRemoteStorage) {
        verifyRLMOnLeadershipChange(Collections.singleton(leaderPartition), Collections.emptySet())
        reset(mockRemoteLogManager)
      }

      // Send a produce request and advance the highwatermark
      val leaderResponse = sendProducerAppend(replicaManager, topicIdPartition, numOfRecords)
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
      val followerResponse = sendProducerAppend(replicaManager, topicIdPartition, numOfRecords)
      assertEquals(Errors.NOT_LEADER_OR_FOLLOWER, followerResponse.get.error)

      // Check the state of that partition and fetcher
      val HostedPartition.Online(followerPartition) = replicaManager.getPartition(topicPartition)
      assertFalse(followerPartition.isLeader)
      assertEquals(1, followerPartition.getLeaderEpoch)

      if (enableRemoteStorage) {
        verifyRLMOnLeadershipChange(Collections.emptySet(), Collections.singleton(followerPartition))
      }

      val fetcher = replicaManager.replicaFetcherManager.getFetcher(topicPartition)
      val otherEndpoint = ClusterImageTest.IMAGE1.broker(otherId).listeners().get("PLAINTEXT")
      assertEquals(Some(new BrokerEndPoint(otherId, otherEndpoint.host(), otherEndpoint.port())), fetcher.map(_.leader.brokerEndPoint()))
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testDeltaFromFollowerToLeader(enableRemoteStorage: Boolean): Unit = {
    val localId = 1
    val otherId = localId + 1
    val numOfRecords = 3
    val topicPartition = new TopicPartition("foo", 0)
    val replicaManager = setupReplicaManagerWithMockedPurgatories(new MockTimer(time), localId, enableRemoteStorage = enableRemoteStorage)

    try {
      // Make the local replica the follower
      val followerTopicsDelta = topicsCreateDelta(localId, false)
      val followerMetadataImage = imageFromTopics(followerTopicsDelta.apply())
      replicaManager.applyDelta(followerTopicsDelta, followerMetadataImage)

      // Check the state of that partition and fetcher
      val HostedPartition.Online(followerPartition) = replicaManager.getPartition(topicPartition)
      assertFalse(followerPartition.isLeader)
      assertEquals(0, followerPartition.getLeaderEpoch)

      if (enableRemoteStorage) {
        verifyRLMOnLeadershipChange(Collections.emptySet(), Collections.singleton(followerPartition))
        reset(mockRemoteLogManager)
      }

      val fetcher = replicaManager.replicaFetcherManager.getFetcher(topicPartition)
      val otherEndpoint = ClusterImageTest.IMAGE1.broker(otherId).listeners().get("PLAINTEXT")
      assertEquals(Some(new BrokerEndPoint(otherId, otherEndpoint.host(), otherEndpoint.port())), fetcher.map(_.leader.brokerEndPoint()))

      // Append on a follower should fail
      val followerResponse = sendProducerAppend(replicaManager,
        new TopicIdPartition(followerMetadataImage.topics().topicsByName().get("foo").id, topicPartition),
        numOfRecords)
      assertEquals(Errors.NOT_LEADER_OR_FOLLOWER, followerResponse.get.error)

      // Change the local replica to leader
      val leaderTopicsDelta = topicsChangeDelta(followerMetadataImage.topics(), localId, true)
      val leaderMetadataImage = imageFromTopics(leaderTopicsDelta.apply())
      replicaManager.applyDelta(leaderTopicsDelta, leaderMetadataImage)

      val topicIdPartition = new TopicIdPartition(leaderMetadataImage.topics().topicsByName().get("foo").id, topicPartition)
      // Send a produce request and advance the highwatermark
      val leaderResponse = sendProducerAppend(replicaManager, topicIdPartition, numOfRecords)
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
      if (enableRemoteStorage) {
        verifyRLMOnLeadershipChange(Collections.singleton(leaderPartition), Collections.emptySet())
      }

      assertEquals(None, replicaManager.replicaFetcherManager.getFetcher(topicPartition))
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testDeltaFollowerWithNoChange(enableRemoteStorage: Boolean): Unit = {
    val localId = 1
    val otherId = localId + 1
    val topicPartition = new TopicPartition("foo", 0)
    val replicaManager = setupReplicaManagerWithMockedPurgatories(new MockTimer(time), localId, enableRemoteStorage = enableRemoteStorage)

    try {
      // Make the local replica the follower
      val followerTopicsDelta = topicsCreateDelta(localId, false)
      val followerMetadataImage = imageFromTopics(followerTopicsDelta.apply())
      replicaManager.applyDelta(followerTopicsDelta, followerMetadataImage)

      // Check the state of that partition and fetcher
      val HostedPartition.Online(followerPartition) = replicaManager.getPartition(topicPartition)
      assertFalse(followerPartition.isLeader)
      assertEquals(0, followerPartition.getLeaderEpoch)

      if (enableRemoteStorage) {
        verifyRLMOnLeadershipChange(Collections.emptySet(), Collections.singleton(followerPartition))
        reset(mockRemoteLogManager)
      }

      val fetcher = replicaManager.replicaFetcherManager.getFetcher(topicPartition)
      val otherEndpoint = ClusterImageTest.IMAGE1.broker(otherId).listeners().get("PLAINTEXT")
      assertEquals(Some(new BrokerEndPoint(otherId, otherEndpoint.host(), otherEndpoint.port())), fetcher.map(_.leader.brokerEndPoint()))

      // Apply the same delta again
      replicaManager.applyDelta(followerTopicsDelta, followerMetadataImage)

      // Check that the state stays the same
      val HostedPartition.Online(noChangePartition) = replicaManager.getPartition(topicPartition)
      assertFalse(noChangePartition.isLeader)
      assertEquals(0, noChangePartition.getLeaderEpoch)

      if (enableRemoteStorage) {
        verifyRLMOnLeadershipChange(Collections.emptySet(), Collections.singleton(followerPartition))
      }

      val noChangeFetcher = replicaManager.replicaFetcherManager.getFetcher(topicPartition)
      assertEquals(Some(new BrokerEndPoint(otherId, otherEndpoint.host(), otherEndpoint.port())), noChangeFetcher.map(_.leader.brokerEndPoint()))
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testDeltaFollowerToNotReplica(enableRemoteStorage: Boolean): Unit = {
    val localId = 1
    val otherId = localId + 1
    val topicPartition = new TopicPartition("foo", 0)
    val replicaManager = setupReplicaManagerWithMockedPurgatories(new MockTimer(time), localId, enableRemoteStorage = enableRemoteStorage)

    try {
      // Make the local replica the follower
      val followerTopicsDelta = topicsCreateDelta(localId, false)
      val followerMetadataImage = imageFromTopics(followerTopicsDelta.apply())
      replicaManager.applyDelta(followerTopicsDelta, followerMetadataImage)

      // Check the state of that partition and fetcher
      val HostedPartition.Online(followerPartition) = replicaManager.getPartition(topicPartition)
      assertFalse(followerPartition.isLeader)
      assertEquals(0, followerPartition.getLeaderEpoch)

      if (enableRemoteStorage) {
        verifyRLMOnLeadershipChange(Collections.emptySet(), Collections.singleton(followerPartition))
        reset(mockRemoteLogManager)
      }

      val fetcher = replicaManager.replicaFetcherManager.getFetcher(topicPartition)
      val otherEndpoint = ClusterImageTest.IMAGE1.broker(otherId).listeners().get("PLAINTEXT")
      assertEquals(Some(new BrokerEndPoint(otherId, otherEndpoint.host(), otherEndpoint.port())), fetcher.map(_.leader.brokerEndPoint()))

      // Apply changes that remove replica
      val notReplicaTopicsDelta = topicsChangeDelta(followerMetadataImage.topics(), otherId, true)
      val notReplicaMetadataImage = imageFromTopics(notReplicaTopicsDelta.apply())
      replicaManager.applyDelta(notReplicaTopicsDelta, notReplicaMetadataImage)

      if (enableRemoteStorage) {
        verify(mockRemoteLogManager, never()).onLeadershipChange(anySet(), anySet(), anyMap())
        verify(mockRemoteLogManager, times(1))
          .stopPartitions(ArgumentMatchers.eq(Collections.singleton(new StopPartition(topicPartition, true, false, false))), any())
      }

      // Check that the partition was removed
      assertEquals(HostedPartition.None, replicaManager.getPartition(topicPartition))
      assertEquals(None, replicaManager.replicaFetcherManager.getFetcher(topicPartition))
      assertEquals(None, replicaManager.logManager.getLog(topicPartition))
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testDeltaFollowerRemovedTopic(enableRemoteStorage: Boolean): Unit = {
    val localId = 1
    val otherId = localId + 1
    val topicPartition = new TopicPartition("foo", 0)
    val replicaManager = setupReplicaManagerWithMockedPurgatories(new MockTimer(time), localId, enableRemoteStorage = enableRemoteStorage)

    try {
      // Make the local replica the follower
      val followerTopicsDelta = topicsCreateDelta(localId, false)
      val followerMetadataImage = imageFromTopics(followerTopicsDelta.apply())
      replicaManager.applyDelta(followerTopicsDelta, followerMetadataImage)

      // Check the state of that partition and fetcher
      val HostedPartition.Online(followerPartition) = replicaManager.getPartition(topicPartition)
      assertFalse(followerPartition.isLeader)
      assertEquals(0, followerPartition.getLeaderEpoch)

      if (enableRemoteStorage) {
        verifyRLMOnLeadershipChange(Collections.emptySet(), Collections.singleton(followerPartition))
        reset(mockRemoteLogManager)
      }

      val fetcher = replicaManager.replicaFetcherManager.getFetcher(topicPartition)
      val otherEndpoint = ClusterImageTest.IMAGE1.broker(otherId).listeners().get("PLAINTEXT")
      assertEquals(Some(new BrokerEndPoint(otherId, otherEndpoint.host(), otherEndpoint.port())), fetcher.map(_.leader.brokerEndPoint()))

      // Apply changes that remove topic and replica
      val removeTopicsDelta = topicsDeleteDelta(followerMetadataImage.topics())
      val removeMetadataImage = imageFromTopics(removeTopicsDelta.apply())
      replicaManager.applyDelta(removeTopicsDelta, removeMetadataImage)

      if (enableRemoteStorage) {
        verify(mockRemoteLogManager, never()).onLeadershipChange(anySet(), anySet(), anyMap())
        verify(mockRemoteLogManager, times(1))
          .stopPartitions(ArgumentMatchers.eq(Collections.singleton(new StopPartition(topicPartition, true, false, false))), any())
      }

      // Check that the partition was removed
      assertEquals(HostedPartition.None, replicaManager.getPartition(topicPartition))
      assertEquals(None, replicaManager.replicaFetcherManager.getFetcher(topicPartition))
      assertEquals(None, replicaManager.logManager.getLog(topicPartition))
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testDeltaLeaderToNotReplica(enableRemoteStorage: Boolean): Unit = {
    val localId = 1
    val otherId = localId + 1
    val topicPartition = new TopicPartition("foo", 0)
    val replicaManager = setupReplicaManagerWithMockedPurgatories(new MockTimer(time), localId, enableRemoteStorage = enableRemoteStorage)

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

      if (enableRemoteStorage) {
        verifyRLMOnLeadershipChange(Collections.singleton(leaderPartition), Collections.emptySet())
        reset(mockRemoteLogManager)
      }

      assertEquals(None, replicaManager.replicaFetcherManager.getFetcher(topicPartition))

      // Apply changes that remove replica
      val notReplicaTopicsDelta = topicsChangeDelta(leaderMetadataImage.topics(), otherId, true)
      val notReplicaMetadataImage = imageFromTopics(notReplicaTopicsDelta.apply())
      replicaManager.applyDelta(notReplicaTopicsDelta, notReplicaMetadataImage)

      if (enableRemoteStorage) {
        verify(mockRemoteLogManager, never()).onLeadershipChange(anySet(), anySet(), anyMap())
        verify(mockRemoteLogManager, times(1))
          .stopPartitions(ArgumentMatchers.eq(Collections.singleton(new StopPartition(topicPartition, true, false, false))), any())
      }

      // Check that the partition was removed
      assertEquals(HostedPartition.None, replicaManager.getPartition(topicPartition))
      assertEquals(None, replicaManager.replicaFetcherManager.getFetcher(topicPartition))
      assertEquals(None, replicaManager.logManager.getLog(topicPartition))
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testDeltaLeaderToRemovedTopic(enableRemoteStorage: Boolean): Unit = {
    val localId = 1
    val otherId = localId + 1
    val topicPartition = new TopicPartition("foo", 0)
    val replicaManager = setupReplicaManagerWithMockedPurgatories(new MockTimer(time), localId, enableRemoteStorage = enableRemoteStorage)

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

      if (enableRemoteStorage) {
        verifyRLMOnLeadershipChange(Collections.singleton(leaderPartition), Collections.emptySet())
        reset(mockRemoteLogManager)
      }

      assertEquals(None, replicaManager.replicaFetcherManager.getFetcher(topicPartition))

      // Apply changes that remove topic and replica
      val removeTopicsDelta = topicsDeleteDelta(leaderMetadataImage.topics())
      val removeMetadataImage = imageFromTopics(removeTopicsDelta.apply())
      replicaManager.applyDelta(removeTopicsDelta, removeMetadataImage)

      if (enableRemoteStorage) {
        verify(mockRemoteLogManager, never()).onLeadershipChange(anySet(), anySet(), anyMap())
        verify(mockRemoteLogManager, times(1))
          .stopPartitions(ArgumentMatchers.eq(Collections.singleton(new StopPartition(topicPartition, true, true, false))), any())
      }

      // Check that the partition was removed
      assertEquals(HostedPartition.None, replicaManager.getPartition(topicPartition))
      assertEquals(None, replicaManager.replicaFetcherManager.getFetcher(topicPartition))
      assertEquals(None, replicaManager.logManager.getLog(topicPartition))
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testDeltaToFollowerCompletesProduce(enableRemoteStorage: Boolean): Unit = {
    val localId = 1
    val otherId = localId + 1
    val numOfRecords = 3
    val topicIdPartition = new TopicIdPartition(FOO_UUID, 0, "foo")
    val topicPartition = topicIdPartition.topicPartition()
    val replicaManager = setupReplicaManagerWithMockedPurgatories(new MockTimer(time), localId, enableRemoteStorage = enableRemoteStorage)

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

      if (enableRemoteStorage) {
        verifyRLMOnLeadershipChange(Collections.singleton(leaderPartition), Collections.emptySet())
        reset(mockRemoteLogManager)
      }

      assertEquals(None, replicaManager.replicaFetcherManager.getFetcher(topicPartition))

      // Send a produce request
      val leaderResponse = sendProducerAppend(replicaManager, topicIdPartition, numOfRecords)

      // Change the local replica to follower
      val followerTopicsDelta = topicsChangeDelta(leaderMetadataImage.topics(), localId, false)
      val followerMetadataImage = imageFromTopics(followerTopicsDelta.apply())
      replicaManager.applyDelta(followerTopicsDelta, followerMetadataImage)

      val HostedPartition.Online(followerPartition) = replicaManager.getPartition(topicPartition)
      assertFalse(followerPartition.isLeader)
      assertEquals(1, followerPartition.getLeaderEpoch)

      if (enableRemoteStorage) {
        verifyRLMOnLeadershipChange(Collections.emptySet(), Collections.singleton(followerPartition))
        reset(mockRemoteLogManager)
      }

      // Check that the produce failed because it changed to follower before replicating
      assertEquals(Errors.NOT_LEADER_OR_FOLLOWER, leaderResponse.get.error)
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testDeltaToFollowerCompletesFetch(enableRemoteStorage: Boolean): Unit = {
    val localId = 1
    val otherId = localId + 1
    val topicPartition = new TopicPartition("foo", 0)
    val replicaManager = setupReplicaManagerWithMockedPurgatories(new MockTimer(time), localId, enableRemoteStorage = enableRemoteStorage)

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

      if (enableRemoteStorage) {
        verifyRLMOnLeadershipChange(Collections.singleton(leaderPartition), Collections.emptySet())
        reset(mockRemoteLogManager)
      }

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

      val HostedPartition.Online(followerPartition) = replicaManager.getPartition(topicPartition)
      assertFalse(followerPartition.isLeader)
      assertEquals(1, followerPartition.getLeaderEpoch)

      if (enableRemoteStorage) {
        verifyRLMOnLeadershipChange(Collections.emptySet(), Collections.singleton(followerPartition))
        reset(mockRemoteLogManager)
      }

      // Check that the produce failed because it changed to follower before replicating
      assertEquals(Errors.NOT_LEADER_OR_FOLLOWER, fetchCallback.assertFired.error)
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
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
      propsModifier = props => props.put(ServerLogConfigs.LOG_DIRS_CONFIG, dataDir.getAbsolutePath),
      enableRemoteStorage = true
    )

    try {
      // Delete the data directory to trigger a storage exception
      Utils.delete(dataDir)

      // Make the local replica the leader
      val topicsDelta = topicsCreateDelta(localId, isStartIdLeader)
      val leaderMetadataImage = imageFromTopics(topicsDelta.apply())
      replicaManager.applyDelta(topicsDelta, leaderMetadataImage)
      verifyRLMOnLeadershipChange(Collections.emptySet(), Collections.emptySet())

      val hostedPartition = replicaManager.getPartition(topicPartition)
      assertEquals(
        classOf[HostedPartition.Offline],
        hostedPartition.getClass
      )
      assertEquals(
        FOO_UUID,
        hostedPartition.asInstanceOf[HostedPartition.Offline].partition.flatMap(p => p.topicId).get
      )
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testDeltaFollowerStopFetcherBeforeCreatingInitialFetchOffset(enableRemoteStorage: Boolean): Unit = {
    val localId = 1
    val otherId = localId + 1
    val topicPartition = new TopicPartition("foo", 0)

    val mockReplicaFetcherManager = mock(classOf[ReplicaFetcherManager])
    val replicaManager = setupReplicaManagerWithMockedPurgatories(
      timer = new MockTimer(time),
      brokerId = localId,
      mockReplicaFetcherManager = Some(mockReplicaFetcherManager),
      enableRemoteStorage = enableRemoteStorage
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
      if (enableRemoteStorage) {
        verifyRLMOnLeadershipChange(Collections.emptySet(), Collections.singleton(followerPartition))
        reset(mockRemoteLogManager)
      }

      // Verify that addFetcherForPartitions was called with the correct
      // init offset.
      val otherEndpoint = ClusterImageTest.IMAGE1.broker(otherId).listeners().get("PLAINTEXT")
      verify(mockReplicaFetcherManager)
        .addFetcherForPartitions(
          Map(topicPartition -> InitialFetchState(
            topicId = Some(FOO_UUID),
            leader = new BrokerEndPoint(otherId, otherEndpoint.host(), otherEndpoint.port()),
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
              records = MemoryRecords.withRecords(
                Compression.NONE, 0,
                new SimpleRecord("first message".getBytes)
              ),
              isFuture = false,
              partitionLeaderEpoch = 0
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

      if (enableRemoteStorage) {
        verifyRLMOnLeadershipChange(Collections.emptySet(), Collections.singleton(followerPartition))
      }

      // Verify that addFetcherForPartitions was called with the correct
      // init offset.
      verify(mockReplicaFetcherManager)
        .addFetcherForPartitions(
          Map(topicPartition -> InitialFetchState(
            topicId = Some(FOO_UUID),
            leader = new BrokerEndPoint(otherId, otherEndpoint.host(), otherEndpoint.port()),
            currentLeaderEpoch = 1,
            initOffset = 1
          ))
        )
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testFetcherAreNotRestartedIfLeaderEpochIsNotBumped(enableRemoteStorage: Boolean): Unit = {
    val localId = 0
    val topicPartition = new TopicPartition("foo", 0)

    val mockReplicaFetcherManager = mock(classOf[ReplicaFetcherManager])
    val replicaManager = setupReplicaManagerWithMockedPurgatories(
      timer = new MockTimer(time),
      brokerId = localId,
      mockReplicaFetcherManager = Some(mockReplicaFetcherManager),
      enableRemoteStorage = enableRemoteStorage
    )

    try {
      when(mockReplicaFetcherManager.removeFetcherForPartitions(
        Set(topicPartition))
      ).thenReturn(Map.empty[TopicPartition, PartitionFetchState])

      // Make the local replica the follower.
      var followerTopicsDelta = new TopicsDelta(TopicsImage.EMPTY)
      followerTopicsDelta.replay(new TopicRecord().setName("foo").setTopicId(FOO_UUID))
      followerTopicsDelta.replay(partitionRecord(localId, localId + 1))
      var followerMetadataImage = imageFromTopics(followerTopicsDelta.apply())
      replicaManager.applyDelta(followerTopicsDelta, followerMetadataImage)

      // Check the state of that partition.
      val HostedPartition.Online(followerPartition) = replicaManager.getPartition(topicPartition)
      assertFalse(followerPartition.isLeader)
      assertEquals(0, followerPartition.getLeaderEpoch)
      assertEquals(0, followerPartition.getPartitionEpoch)

      if (enableRemoteStorage) {
        verifyRLMOnLeadershipChange(Collections.emptySet(), Collections.singleton(followerPartition))
        reset(mockRemoteLogManager)
      }

      // Verify that the partition was removed and added back.
      val localIdPlus1Endpoint = ClusterImageTest.IMAGE1.broker(localId + 1).listeners().get("PLAINTEXT")
      verify(mockReplicaFetcherManager).removeFetcherForPartitions(Set(topicPartition))
      verify(mockReplicaFetcherManager).addFetcherForPartitions(Map(topicPartition -> InitialFetchState(
        topicId = Some(FOO_UUID),
        leader = new BrokerEndPoint(localId + 1, localIdPlus1Endpoint.host(), localIdPlus1Endpoint.port()),
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
        .setDirectories(util.Arrays.asList(Uuid.fromString("fKgQ2axkQiuzt4ANqKbPkQ"), DirectoryId.UNASSIGNED, DirectoryId.UNASSIGNED))
        .setIsr(util.Arrays.asList(localId, localId + 1))
      )
      followerMetadataImage = imageFromTopics(followerTopicsDelta.apply())
      replicaManager.applyDelta(followerTopicsDelta, followerMetadataImage)

      assertFalse(followerPartition.isLeader)
      assertEquals(0, followerPartition.getLeaderEpoch)
      assertEquals(1, followerPartition.getPartitionEpoch)

      if (enableRemoteStorage) {
        verifyRLMOnLeadershipChange(Collections.emptySet(), Collections.singleton(followerPartition))
        reset(mockRemoteLogManager)
      }

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

      if (enableRemoteStorage) {
        verifyRLMOnLeadershipChange(Collections.emptySet(), Collections.singleton(followerPartition))
        reset(mockRemoteLogManager)
      }

      // Verify that the partition was removed and added back.
      val localIdPlus2Endpoint = ClusterImageTest.IMAGE1.broker(localId + 2).listeners().get("PLAINTEXT")
      verify(mockReplicaFetcherManager).removeFetcherForPartitions(Set(topicPartition))
      verify(mockReplicaFetcherManager).addFetcherForPartitions(Map(topicPartition -> InitialFetchState(
        topicId = Some(FOO_UUID),
        leader = new BrokerEndPoint(localId + 2, localIdPlus2Endpoint.host(), localIdPlus2Endpoint.port()),
        currentLeaderEpoch = 1,
        initOffset = 0
      )))
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testReplicasAreStoppedWhileInControlledShutdown(enableRemoteStorage: Boolean): Unit = {
    val localId = 0
    val foo0 = new TopicPartition("foo", 0)
    val foo1 = new TopicPartition("foo", 1)
    val foo2 = new TopicPartition("foo", 2)

    val mockReplicaFetcherManager = mock(classOf[ReplicaFetcherManager])
    val replicaManager = setupReplicaManagerWithMockedPurgatories(
      timer = new MockTimer(time),
      brokerId = localId,
      mockReplicaFetcherManager = Some(mockReplicaFetcherManager),
      enableRemoteStorage = enableRemoteStorage
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

      if (enableRemoteStorage) {
        val followers: util.Set[Partition] = new util.HashSet[Partition]()
        followers.add(fooPartition0)
        followers.add(fooPartition2)
        verifyRLMOnLeadershipChange(Collections.singleton(fooPartition1), followers)
        reset(mockRemoteLogManager)
      }

      reset(mockReplicaFetcherManager)

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

      if (enableRemoteStorage) {
        val followers: util.Set[Partition] = new util.HashSet[Partition]()
        followers.add(fooPartition0)
        followers.add(fooPartition1)
        verifyRLMOnLeadershipChange(Collections.emptySet(), followers)
        reset(mockRemoteLogManager)
      }

      // Fetcher for foo0 and foo1 are stopped.
      verify(mockReplicaFetcherManager).removeFetcherForPartitions(Set(foo0, foo1))
    } finally {
      // Fetcher for foo2 is stopped when the replica manager shuts down
      // because this replica was not in the ISR.
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testPartitionListener(): Unit = {
    val maxFetchBytes = 1024 * 1024
    val aliveBrokersIds = Seq(0, 1)
    val leaderEpoch = 5
    val replicaManager = setupReplicaManagerWithMockedPurgatories(new MockTimer(time),
      brokerId = 0, aliveBrokersIds)
    try {
      val tp = new TopicPartition(topic, 0)
      val tidp = new TopicIdPartition(topicId, tp)
      val replicas = aliveBrokersIds.toList.map(Int.box).asJava

      val listener = new MockPartitionListener
      listener.verify()

      // Registering a listener should fail because the partition does not exist yet.
      assertFalse(replicaManager.maybeAddListener(tp, listener))

      // Broker 0 becomes leader of the partition
      val leaderDelta = createLeaderDelta(
        topicId = topicId,
        partition = tp,
        leaderId = 0,
        replicas = replicas,
        isr = replicas,
        leaderEpoch = leaderEpoch
      )
      replicaManager.applyDelta(leaderDelta, imageFromTopics(leaderDelta.apply()))

      // Registering it should succeed now.
      assertTrue(replicaManager.maybeAddListener(tp, listener))
      listener.verify()

      // Leader appends some data
      for (i <- 1 to 5) {
        appendRecords(replicaManager, tp, TestUtils.singletonRecords(s"message $i".getBytes)).onFire { response =>
          assertEquals(Errors.NONE, response.error)
        }
      }

      // Follower fetches up to offset 2.
      fetchPartitionAsFollower(
        replicaManager,
        tidp,
        new FetchRequest.PartitionData(
          Uuid.ZERO_UUID,
          2L,
          0L,
          maxFetchBytes,
          Optional.of(leaderEpoch)
        ),
        replicaId = 1
      )

      // Listener is updated.
      listener.verify(expectedHighWatermark = 2L)

      // Listener is removed.
      replicaManager.removeListener(tp, listener)

      // Follower fetches up to offset 4.
      fetchPartitionAsFollower(
        replicaManager,
        tidp,
        new FetchRequest.PartitionData(
          Uuid.ZERO_UUID,
          4L,
          0L,
          maxFetchBytes,
          Optional.of(leaderEpoch)
        ),
        replicaId = 1
      )

      // Listener is not updated anymore.
      listener.verify()
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  private def topicsCreateDelta(startId: Int, isStartIdLeader: Boolean, partitions:List[Int] = List(0), directoryIds: List[Uuid] = List.empty, topicName: String = "foo", topicId: Uuid = FOO_UUID, leaderEpoch: Int = 0): TopicsDelta = {
    val leader = if (isStartIdLeader) startId else startId + 1
    val delta = new TopicsDelta(TopicsImage.EMPTY)
    delta.replay(new TopicRecord().setName(topicName).setTopicId(topicId))

    partitions.foreach { partition =>
      val record = partitionRecord(startId, leader, partition, topicId, leaderEpoch)
      if (directoryIds.nonEmpty) {
        record.setDirectories(directoryIds.asJava)
      }
      delta.replay(record)
    }

    delta
  }

  private def partitionRecord(startId: Int, leader: Int, partition: Int = 0, topicId: Uuid = FOO_UUID, leaderEpoch: Int = 0) = {
    new PartitionRecord()
      .setPartitionId(partition)
      .setTopicId(topicId)
      .setReplicas(util.Arrays.asList(startId, startId + 1))
      .setIsr(util.Arrays.asList(startId, startId + 1))
      .setLeader(leader)
      .setLeaderEpoch(leaderEpoch)
      .setPartitionEpoch(0)
  }

  private def topicsChangeDelta(topicsImage: TopicsImage, startId: Int, isStartIdLeader: Boolean): TopicsDelta = {
    val leader = if (isStartIdLeader) startId else startId + 1
    val delta = new TopicsDelta(topicsImage)
    delta.replay(partitionChangeRecord(startId, leader))
    delta
  }

  private def partitionChangeRecord(startId: Int, leader: Int) = {
    new PartitionChangeRecord()
      .setPartitionId(0)
      .setTopicId(FOO_UUID)
      .setReplicas(util.Arrays.asList(startId, startId + 1))
      .setIsr(util.Arrays.asList(startId, startId + 1))
      .setLeader(leader)
  }

  private def topicsDeleteDelta(topicsImage: TopicsImage): TopicsDelta = {
    val delta = new TopicsDelta(topicsImage)
    delta.replay(new RemoveTopicRecord().setTopicId(FOO_UUID))

    delta
  }

  private def imageFromTopics(topicsImage: TopicsImage): MetadataImage = {
    val featuresImageLatest = new FeaturesImage(
      Collections.emptyMap(),
      MetadataVersion.latestProduction())
    new MetadataImage(
      new MetadataProvenance(100L, 10, 1000L, true),
      featuresImageLatest,
      ClusterImageTest.IMAGE1,
      topicsImage,
      ConfigurationsImage.EMPTY,
      ClientQuotasImage.EMPTY,
      ProducerIdsImage.EMPTY,
      AclsImage.EMPTY,
      ScramImage.EMPTY,
      DelegationTokenImage.EMPTY
    )
  }

  def assertFetcherHasTopicId[T <: AbstractFetcherThread](manager: AbstractFetcherManager[T],
                                                          tp: TopicPartition,
                                                          expectedTopicId: Option[Uuid]): Unit = {
    val fetchState = manager.getFetcher(tp).flatMap(_.fetchState(tp))
    assertTrue(fetchState.isDefined)
    assertEquals(expectedTopicId, fetchState.get.topicId)
  }

  @Test
  def testReplicaAlterLogDirsMultipleReassignmentDoesNotBlockLogCleaner(): Unit = {
    val localId = 0
    val tp = new TopicPartition(topic, 0)
    val tpId = new TopicIdPartition(topicId, tp)

    val props = TestUtils.createBrokerConfig(localId)
    val path1 = TestUtils.tempRelativeDir("data").getAbsolutePath
    val path2 = TestUtils.tempRelativeDir("data2").getAbsolutePath
    val path3 = TestUtils.tempRelativeDir("data3").getAbsolutePath
    props.put("log.dirs", Seq(path1, path2, path3).mkString(","))
    val config = KafkaConfig.fromProps(props)
    val mockLogMgr = TestUtils.createLogManager(config.logDirs.asScala.map(new File(_)), cleanerConfig = new CleanerConfig(true))
    mockLogMgr.startup(Set())
    val replicaManager = new ReplicaManager(
      metrics = metrics,
      config = config,
      time = time,
      scheduler = new MockScheduler(time),
      logManager = mockLogMgr,
      quotaManagers = quotaManager,
      metadataCache = metadataCache,
      logDirFailureChannel = new LogDirFailureChannel(config.logDirs.size),
      alterPartitionManager = alterPartitionManager,
      addPartitionsToTxnManager = Some(addPartitionsToTxnManager))

    try {
      val spiedPartition = spy(Partition(tpId, time, replicaManager))
      replicaManager.addOnlinePartition(tp, spiedPartition)

      val leaderDelta = topicsCreateDelta(localId, isStartIdLeader = true, partitions = List(0, 1), List.empty, topic, topicIds(topic))
      val leaderImage = imageFromTopics(leaderDelta.apply())
      replicaManager.applyDelta(leaderDelta, leaderImage)

      // Move the replica to the second log directory.
      val partition = replicaManager.getPartitionOrException(tp)
      val firstLogDir = partition.log.get.dir.getParentFile
      val newReplicaFolder = replicaManager.logManager.liveLogDirs.filterNot(_ == firstLogDir).head
      replicaManager.alterReplicaLogDirs(Map(tp -> newReplicaFolder.getAbsolutePath))

      // Prevent promotion of future replica
      doReturn(false).when(spiedPartition).maybeReplaceCurrentWithFutureReplica()

      // Make sure the future log is created with the correct topic ID.
      val futureLog = replicaManager.futureLocalLogOrException(tp)
      assertEquals(Optional.of(topicId), futureLog.topicId)

      // Move the replica to the third log directory
      val finalReplicaFolder = replicaManager.logManager.liveLogDirs.filterNot(it => it == firstLogDir || it == newReplicaFolder).head
      replicaManager.alterReplicaLogDirs(Map(tp -> finalReplicaFolder.getAbsolutePath))

      reset(spiedPartition)

      TestUtils.waitUntilTrue(() => {
        replicaManager.replicaAlterLogDirsManager.shutdownIdleFetcherThreads()
        replicaManager.replicaAlterLogDirsManager.fetcherThreadMap.isEmpty
      }, s"ReplicaAlterLogDirsThread should be gone", waitTimeMs = 60_000)

      verify(replicaManager.logManager.cleaner, times(2)).resumeCleaning(Set(tp).asJava)
    } finally {
      replicaManager.shutdown(checkpointHW = false)
      mockLogMgr.shutdown()
    }
  }

  @Test
  def testReplicaAlterLogDirs(): Unit = {
    val localId = 0
    val tp = new TopicPartition(topic, 0)

    val mockReplicaAlterLogDirsManager = mock(classOf[ReplicaAlterLogDirsManager])
    val replicaManager = setupReplicaManagerWithMockedPurgatories(
      timer = new MockTimer(time),
      mockReplicaAlterLogDirsManager = Some(mockReplicaAlterLogDirsManager)
    )

    try {
      replicaManager.createPartition(tp).createLogIfNotExists(
        isNew = false,
        isFutureReplica = false,
        offsetCheckpoints = new LazyOffsetCheckpoints(replicaManager.highWatermarkCheckpoints.asJava),
        topicId = None
      )

      val leaderDelta = topicsCreateDelta(localId, isStartIdLeader = true, partitions = List(0, 1), List.empty, topic, topicIds(topic))
      val leaderImage = imageFromTopics(leaderDelta.apply())
      replicaManager.applyDelta(leaderDelta, leaderImage)

      // Move the replica to the second log directory.
      val partition = replicaManager.getPartitionOrException(tp)
      val newReplicaFolder = replicaManager.logManager.liveLogDirs.filterNot(_ == partition.log.get.dir.getParentFile).head
      replicaManager.alterReplicaLogDirs(Map(tp -> newReplicaFolder.getAbsolutePath))

      // Make sure the future log is created with the correct topic ID.
      val futureLog = replicaManager.futureLocalLogOrException(tp)
      assertEquals(Optional.of(topicId), futureLog.topicId)

      // Verify that addFetcherForPartitions was called with the correct topic ID.
      verify(mockReplicaAlterLogDirsManager, times(1)).addFetcherForPartitions(Map(tp -> InitialFetchState(
        topicId = Some(topicId),
        leader = new BrokerEndPoint(0, "localhost", -1),
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
      expectTruncation = false, localLogOffset = Optional.of(10), offsetFromLeader = offsetFromLeader, topicId = Optional.of(topicId))

    try {
      val responses = replicaManager.describeLogDirs(Set(new TopicPartition(topic, topicPartition)))
      assertEquals(mockLogMgr.liveLogDirs.size, responses.size)
      responses.forEach { response =>
        assertEquals(Errors.NONE.code, response.errorCode)
        assertTrue(response.totalBytes > 0)
        assertTrue(response.usableBytes >= 0)
        assertFalse(response.topics().isEmpty)
        response.topics().forEach(t => assertFalse(t.partitions().isEmpty))
      }
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testDescribeLogDirsWithoutAnyPartitionTopic(): Unit = {
    val noneTopic = "none-topic"
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
      expectTruncation = false, localLogOffset = Optional.of(10), offsetFromLeader = offsetFromLeader, topicId = Optional.of(topicId))

    try {
      val responses = replicaManager.describeLogDirs(Set(new TopicPartition(noneTopic, topicPartition)))
      assertEquals(mockLogMgr.liveLogDirs.size, responses.size)
      responses.forEach { response =>
        assertEquals(Errors.NONE.code, response.errorCode)
        assertTrue(response.totalBytes > 0)
        assertTrue(response.usableBytes >= 0)
        assertTrue(response.topics().isEmpty)
      }
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testCheckpointHwOnShutdown(): Unit = {
    val mockLogMgr = TestUtils.createLogManager(config.logDirs.asScala.map(new File(_)))
    val spyRm = spy(new ReplicaManager(
      metrics = metrics,
      config = config,
      time = time,
      scheduler = new MockScheduler(time),
      logManager = mockLogMgr,
      quotaManagers = quotaManager,
      metadataCache = new KRaftMetadataCache(config.brokerId, () => KRaftVersion.KRAFT_VERSION_0),
      logDirFailureChannel = new LogDirFailureChannel(config.logDirs.size),
      alterPartitionManager = alterPartitionManager))

    spyRm.shutdown(checkpointHW = true)

    verify(spyRm).checkpointHighWatermarks()
  }

  @Test
  def testNotCallStopPartitionsForNonTieredTopics(): Unit = {
    val localId = 1
    val otherId = localId + 1
    val topicPartition = new TopicPartition("foo", 0)
    val replicaManager = setupReplicaManagerWithMockedPurgatories(
      new MockTimer(time),
      localId,
      enableRemoteStorage = true,
      defaultTopicRemoteLogStorageEnable = false
    )

    try {
      // Make the local replica the leader.
      val leaderTopicsDelta = topicsCreateDelta(localId, true)
      val leaderMetadataImage = imageFromTopics(leaderTopicsDelta.apply())

      replicaManager.applyDelta(leaderTopicsDelta, leaderMetadataImage)

      // Check the state of that partition and fetcher.
      val HostedPartition.Online(partition) = replicaManager.getPartition(topicPartition)
      assertTrue(partition.isLeader)
      assertEquals(Set(localId, otherId), partition.inSyncReplicaIds)
      assertEquals(0, partition.getLeaderEpoch)

      verifyRLMOnLeadershipChange(Collections.singleton(partition), Collections.emptySet())
      reset(mockRemoteLogManager)

      // Change the local replica to follower.
      val followerTopicsDelta = topicsChangeDelta(leaderMetadataImage.topics(), localId, false)
      val followerMetadataImage = imageFromTopics(followerTopicsDelta.apply())
      replicaManager.applyDelta(followerTopicsDelta, followerMetadataImage)

      verifyRLMOnLeadershipChange(Collections.emptySet(), Collections.singleton(partition))
      verify(mockRemoteLogManager, times(0)).stopPartitions(any(), any())
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  val foo0 = new TopicIdPartition(Uuid.fromString("Sl08ZXU2QW6uF5hIoSzc8w"), new TopicPartition("foo", 0))
  val foo1 = new TopicIdPartition(Uuid.fromString("Sl08ZXU2QW6uF5hIoSzc8w"), new TopicPartition("foo", 1))
  val newFoo0 = new TopicIdPartition(Uuid.fromString("JRCmVxWxQamFs4S8NXYufg"), new TopicPartition("foo", 0))
  val bar0 = new TopicIdPartition(Uuid.fromString("69O438ZkTSeqqclTtZO2KA"), new TopicPartition("bar", 0))

  def verifyPartitionIsOnlineAndHasId(
    replicaManager: ReplicaManager,
    topicIdPartition: TopicIdPartition
  ): Unit = {
    val partition = replicaManager.getPartition(topicIdPartition.topicPartition())
    assertTrue(partition.isInstanceOf[HostedPartition.Online],
      s"Expected ${topicIdPartition} to be in state: HostedPartition.Online. But was in state: ${partition}")
    val hostedPartition = partition.asInstanceOf[HostedPartition.Online]
    assertTrue(hostedPartition.partition.log.isDefined,
      s"Expected ${topicIdPartition} to have a log set in ReplicaManager, but it did not.")
    assertTrue(hostedPartition.partition.log.get.topicId.isPresent,
      s"Expected the log for ${topicIdPartition} to topic ID set in LogManager, but it did not.")
    assertEquals(topicIdPartition.topicId(), hostedPartition.partition.log.get.topicId.get)
    assertEquals(topicIdPartition.topicPartition(), hostedPartition.partition.topicPartition)
  }

  def verifyPartitionIsOffline(
    replicaManager: ReplicaManager,
    topicIdPartition: TopicIdPartition
  ): Unit = {
    val partition = replicaManager.getPartition(topicIdPartition.topicPartition())
    assertEquals(HostedPartition.None, partition, s"Expected ${topicIdPartition} to be offline, but it was: ${partition}")
  }

  @Test
  def testRemoteReadQuotaExceeded(): Unit = {
    when(mockRemoteLogManager.getFetchThrottleTimeMs).thenReturn(quotaExceededThrottleTime)

    val tp0 = new TopicPartition(topic, 0)
    val tpId0 = new TopicIdPartition(topicId, tp0)
    val fetch: Seq[(TopicIdPartition, LogReadResult)] = readFromLogWithOffsetOutOfRange(tp0)

    assertEquals(1, fetch.size)
    assertEquals(tpId0, fetch.head._1)
    val fetchInfo = fetch.head._2.info
    assertEquals(LogOffsetMetadata.UNKNOWN_OFFSET_METADATA, fetchInfo.fetchOffsetMetadata)
    assertFalse(fetchInfo.records.records().iterator().hasNext)
    assertFalse(fetchInfo.firstEntryIncomplete)
    assertFalse(fetchInfo.abortedTransactions.isPresent)
    assertFalse(fetchInfo.delayedRemoteStorageFetch.isPresent)

    val allMetrics = metrics.metrics()
    val avgMetric = allMetrics.get(metrics.metricName("remote-fetch-throttle-time-avg", "RemoteLogManager"))
    val maxMetric = allMetrics.get(metrics.metricName("remote-fetch-throttle-time-max", "RemoteLogManager"))
    assertEquals(quotaExceededThrottleTime, avgMetric.metricValue.asInstanceOf[Double].toLong)
    assertEquals(quotaExceededThrottleTime, maxMetric.metricValue.asInstanceOf[Double].toLong)
  }

  @Test
  def testRemoteReadQuotaNotExceeded(): Unit = {
    when(mockRemoteLogManager.getFetchThrottleTimeMs).thenReturn(quotaAvailableThrottleTime)

    val tp0 = new TopicPartition(topic, 0)
    val tpId0 = new TopicIdPartition(topicId, tp0)
    val fetch: Seq[(TopicIdPartition, LogReadResult)] = readFromLogWithOffsetOutOfRange(tp0)

    assertEquals(1, fetch.size)
    assertEquals(tpId0, fetch.head._1)
    val fetchInfo = fetch.head._2.info
    assertEquals(1L, fetchInfo.fetchOffsetMetadata.messageOffset)
    assertEquals(UnifiedLog.UNKNOWN_OFFSET, fetchInfo.fetchOffsetMetadata.segmentBaseOffset)
    assertEquals(-1, fetchInfo.fetchOffsetMetadata.relativePositionInSegment)
    assertEquals(MemoryRecords.EMPTY, fetchInfo.records)
    assertTrue(fetchInfo.delayedRemoteStorageFetch.isPresent)

    val allMetrics = metrics.metrics()
    val avgMetric = allMetrics.get(metrics.metricName("remote-fetch-throttle-time-avg", "RemoteLogManager"))
    val maxMetric = allMetrics.get(metrics.metricName("remote-fetch-throttle-time-max", "RemoteLogManager"))
    assertEquals(Double.NaN, avgMetric.metricValue)
    assertEquals(Double.NaN, maxMetric.metricValue)
  }

  @Test
  def testBecomeFollowerInvokeOnBecomingFollowerListener(): Unit = {
    val localId = 1
    val topicPartition = new TopicPartition("foo", 0)
    val replicaManager = setupReplicaManagerWithMockedPurgatories(new MockTimer(time), localId)
    // Attach listener to partition.
    val offsetCheckpoints = new LazyOffsetCheckpoints(replicaManager.highWatermarkCheckpoints.asJava)
    replicaManager.createPartition(topicPartition).createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints, None)
    val listener = new MockPartitionListener
    assertTrue(replicaManager.maybeAddListener(topicPartition, listener))
    listener.verify()

    try {
      // Make the local replica the leader
      val leaderTopicsDelta = topicsCreateDelta(localId, true)
      val leaderMetadataImage = imageFromTopics(leaderTopicsDelta.apply())

      replicaManager.applyDelta(leaderTopicsDelta, leaderMetadataImage)

      // Check the state of that partition and fetcher
      val HostedPartition.Online(leaderPartition) = replicaManager.getPartition(topicPartition)
      assertTrue(leaderPartition.isLeader)
      assertEquals(0, leaderPartition.getLeaderEpoch)
      // On becoming follower listener should not be invoked yet.
      listener.verify()

      // Change the local replica to follower
      val followerTopicsDelta = topicsChangeDelta(leaderMetadataImage.topics(), localId, false)
      val followerMetadataImage = imageFromTopics(followerTopicsDelta.apply())
      replicaManager.applyDelta(followerTopicsDelta, followerMetadataImage)

      // On becoming follower listener should be invoked.
      listener.verify(expectedFollower = true)

      // Check the state of that partition.
      val HostedPartition.Online(followerPartition) = replicaManager.getPartition(topicPartition)
      assertFalse(followerPartition.isLeader)
      assertEquals(1, followerPartition.getLeaderEpoch)
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testDeleteRecordsInternalTopicDeleteDisallowed(): Unit = {
    val localId = 1
    val topicPartition0 = new TopicIdPartition(FOO_UUID, 0, Topic.GROUP_METADATA_TOPIC_NAME)
    val directoryEventHandler = mock(classOf[DirectoryEventHandler])
    val aliveBrokerIds = Array(1, 2)

    val rm = setupReplicaManagerWithMockedPurgatories(new MockTimer(time), localId, setupLogDirMetaProperties = true, directoryEventHandler = directoryEventHandler)
    try {
      val directoryIds = rm.logManager.directoryIdsSet.toList
      assertEquals(directoryIds.size, 2)
      val leaderTopicsDelta: TopicsDelta = topicsCreateDelta(localId, isStartIdLeader = true, directoryIds = directoryIds)
      val (partition: Partition, _) = rm.getOrCreatePartition(topicPartition0.topicPartition(), leaderTopicsDelta, FOO_UUID).get
      partition.makeLeader(partitionRegistration(localId, 1, aliveBrokerIds, partitionEpoch, aliveBrokerIds),
        isNew = false,
        new LazyOffsetCheckpoints(rm.highWatermarkCheckpoints.asJava),
        None)

      def callback(responseStatus: Map[TopicPartition, DeleteRecordsResponseData.DeleteRecordsPartitionResult]): Unit = {
        assert(responseStatus.values.head.errorCode == Errors.INVALID_TOPIC_EXCEPTION.code)
      }

      // default internal topics delete disabled
      rm.deleteRecords(
        timeout = 0L,
        Map[TopicPartition, Long](topicPartition0.topicPartition() -> 10L),
        responseCallback = callback
      )
    } finally {
        rm.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testDeleteRecordsInternalTopicDeleteAllowed(): Unit = {
    val localId = 1
    val topicPartition0 = new TopicIdPartition(FOO_UUID, 0, Topic.GROUP_METADATA_TOPIC_NAME)
    val directoryEventHandler = mock(classOf[DirectoryEventHandler])
    val aliveBrokerIds = Array(1, 2)

    val rm = setupReplicaManagerWithMockedPurgatories(new MockTimer(time), localId, setupLogDirMetaProperties = true, directoryEventHandler = directoryEventHandler)
    try {
      val directoryIds = rm.logManager.directoryIdsSet.toList
      assertEquals(directoryIds.size, 2)
      val leaderTopicsDelta: TopicsDelta = topicsCreateDelta(localId, isStartIdLeader = true, directoryIds = directoryIds)
      val (partition: Partition, _) = rm.getOrCreatePartition(topicPartition0.topicPartition(), leaderTopicsDelta, FOO_UUID).get
      partition.makeLeader(partitionRegistration(localId, 1, aliveBrokerIds, partitionEpoch, aliveBrokerIds),
        isNew = false,
        new LazyOffsetCheckpoints(rm.highWatermarkCheckpoints.asJava),
        None)

      def callback(responseStatus: Map[TopicPartition, DeleteRecordsResponseData.DeleteRecordsPartitionResult]): Unit = {
        assert(responseStatus.values.head.errorCode == Errors.NONE.code)
      }

      // internal topics delete allowed
      rm.deleteRecords(
        timeout = 0L,
        Map[TopicPartition, Long](topicPartition0.topicPartition() -> 0L),
        responseCallback = callback,
        allowInternalTopicDeletion = true
      )
    } finally {
      rm.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testDelayedShareFetchPurgatoryOperationExpiration(): Unit = {
    val mockLogMgr = TestUtils.createLogManager(config.logDirs.asScala.map(new File(_)))
    val rm = new ReplicaManager(
      metrics = metrics,
      config = config,
      time = time,
      scheduler = new MockScheduler(time),
      logManager = mockLogMgr,
      quotaManagers = quotaManager,
      metadataCache = new KRaftMetadataCache(config.brokerId, () => KRaftVersion.KRAFT_VERSION_0),
      logDirFailureChannel = new LogDirFailureChannel(config.logDirs.size),
      alterPartitionManager = alterPartitionManager)

    try {
      val groupId = "grp"
      val tp1 = new TopicIdPartition(Uuid.randomUuid, new TopicPartition("foo1", 0))
      val topicPartitions = util.List.of(tp1)

      val sp1 = mock(classOf[SharePartition])
      val sharePartitions = new util.LinkedHashMap[TopicIdPartition, SharePartition]
      sharePartitions.put(tp1, sp1)

      val future = new CompletableFuture[util.Map[TopicIdPartition, ShareFetchResponseData.PartitionData]]
      val shareFetch = new ShareFetch(
        new FetchParams(FetchRequest.ORDINARY_CONSUMER_ID, -1, 500, 1, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, Optional.empty, true),
        groupId,
        Uuid.randomUuid.toString,
        future,
        topicPartitions,
        ShareAcquireMode.BATCH_OPTIMIZED.id(),
        500,
        100,
        brokerTopicStats)

      val delayedShareFetch = spy(new DelayedShareFetch(
        shareFetch,
        rm,
        mock(classOf[BiConsumer[SharePartitionKey, Throwable]]),
        sharePartitions,
        mock(classOf[ShareGroupMetrics]),
        time,
        500))

      val delayedShareFetchWatchKeys : util.List[DelayedShareFetchKey] = new util.ArrayList[DelayedShareFetchKey]
      topicPartitions.forEach((topicIdPartition: TopicIdPartition) => delayedShareFetchWatchKeys.add(new DelayedShareFetchGroupKey(groupId, topicIdPartition.topicId, topicIdPartition.partition)))

      // You cannot acquire records for sp1, so request will be stored in purgatory waiting for timeout.
      when(sp1.maybeAcquireFetchLock(any())).thenReturn(false)

      rm.addDelayedShareFetchRequest(delayedShareFetch = delayedShareFetch, delayedShareFetchKeys = delayedShareFetchWatchKeys)
      verify(delayedShareFetch, times(0)).forceComplete()
      assertEquals(1, rm.delayedShareFetchPurgatory.watched)

      // Future is not complete initially.
      assertFalse(future.isDone)
      // Post timeout, share fetch request will timeout and the future should complete. The timeout is set at 500ms but
      // kept a buffer of additional 500ms so the task can always timeout.
      waitUntilTrue(() => future.isDone, "Processing in delayed share fetch purgatory never ended.", 1000)
      verify(delayedShareFetch, times(1)).forceComplete()
      assertFalse(future.isCompletedExceptionally)
      // Since no partition could be acquired, the future should be empty.
      assertEquals(0, future.join.size)
    } finally {
      rm.shutdown()
    }

  }

  @Test
  def testAppendRecordsToLeader(): Unit = {
    val localId = 0
    val foo = new TopicIdPartition(Uuid.randomUuid, 0, "foo")
    val bar = new TopicIdPartition(Uuid.randomUuid, 0, "bar")

    val replicaManager = setupReplicaManagerWithMockedPurgatories(
      timer = new MockTimer(time),
      brokerId = localId
    )

    try {
      val topicDelta = new TopicsDelta(TopicsImage.EMPTY)
      topicDelta.replay(new TopicRecord()
        .setName(foo.topic)
        .setTopicId(foo.topicId)
      )
      topicDelta.replay(new PartitionRecord()
        .setTopicId(foo.topicId)
        .setPartitionId(foo.partition)
        .setLeader(localId)
        .setLeaderEpoch(0)
        .setPartitionEpoch(0)
        .setReplicas(List[Integer](localId).asJava)
        .setIsr(List[Integer](localId).asJava)
      )

      val metadataImage = imageFromTopics(topicDelta.apply())
      replicaManager.applyDelta(topicDelta, metadataImage)

      // Create test records.
      val records = TestUtils.singletonRecords(
        value = "test".getBytes,
        timestamp = time.milliseconds
      )

      // Append records to both foo and bar.
      val result = replicaManager.appendRecordsToLeader(
        requiredAcks = 1,
        internalTopicsAllowed = true,
        origin = AppendOrigin.CLIENT,
        entriesPerPartition = Map(
          foo -> records,
          bar -> records
        ),
        requestLocal = RequestLocal.noCaching
      )

      assertNotNull(result)
      assertEquals(2, result.size)

      val fooResult = result(foo)
      assertEquals(Errors.NONE, fooResult.error)
      assertEquals(0, fooResult.info.logStartOffset)
      assertEquals(0, fooResult.info.firstOffset)
      assertEquals(0, fooResult.info.lastOffset)

      val barResult = result(bar)
      assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION, barResult.error)
      assertEquals(LogAppendInfo.UNKNOWN_LOG_APPEND_INFO, barResult.info)
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  @Test
  def testMonitorableReplicaSelector(): Unit = {
    val replicaManager = setupReplicaManagerWithMockedPurgatories(new MockTimer(time),
      propsModifier = props => props.put(ReplicationConfigs.REPLICA_SELECTOR_CLASS_CONFIG, classOf[MonitorableReplicaSelector].getName))
    try {
      assertTrue(replicaManager.replicaSelectorPlugin.get.get.asInstanceOf[MonitorableReplicaSelector].pluginMetrics)
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  private def readFromLogWithOffsetOutOfRange(tp: TopicPartition): Seq[(TopicIdPartition, LogReadResult)] = {
    val replicaManager = setupReplicaManagerWithMockedPurgatories(new MockTimer(time), aliveBrokerIds = Seq(0, 1, 2), enableRemoteStorage = true, shouldMockLog = true)
    try {
      val offsetCheckpoints = new LazyOffsetCheckpoints(replicaManager.highWatermarkCheckpoints.asJava)
      replicaManager.createPartition(tp).createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints = offsetCheckpoints, Some(topicId))
      val partition0Replicas = Seq[Integer](0, 1).asJava
      val leaderEpoch = 0
      val leaderDelta = createLeaderDelta(topicId, tp, leaderId = 0, leaderEpoch = leaderEpoch, replicas = partition0Replicas, isr = partition0Replicas)
      val leaderMetadataImage = imageFromTopics(leaderDelta.apply())
      replicaManager.applyDelta(leaderDelta, leaderMetadataImage)

      val params = new FetchParams(-1, 1, 1000, 0, 100, FetchIsolation.HIGH_WATERMARK, Optional.empty)
      replicaManager.readFromLog(
        params,
        Seq(new TopicIdPartition(topicId, 0, topic) -> new PartitionData(topicId, 1, 0, 100000, Optional.of[Integer](leaderEpoch), Optional.of(leaderEpoch))),
        UNBOUNDED_QUOTA,
        readFromPurgatory = false)
    } finally {
      replicaManager.shutdown(checkpointHW = false)
    }
  }

  // Some threads are closed, but the state didn't reflect in the JVM immediately, so add some wait time for it
  private def assertNoNonDaemonThreadsWithWaiting(threadNamePrefix: String, waitTimeMs: Long = 500L): Unit = {
    var nonDemonThreads: mutable.Set[Thread] = mutable.Set.empty[Thread]
    waitUntilTrue(() => {
      nonDemonThreads = Thread.getAllStackTraces.keySet.asScala.filter { t =>
        !t.isDaemon && t.isAlive && t.getName.startsWith(threadNamePrefix)
      }
      0 == nonDemonThreads.size
    }, s"Found unexpected ${nonDemonThreads.size} NonDaemon threads=${nonDemonThreads.map(t => t.getName).mkString(", ")}", waitTimeMs)
  }

  private def yammerGaugeValue[T](metricName: String): Option[T] = {
    KafkaYammerMetrics.defaultRegistry.allMetrics.asScala
      .filter { case (k, _) => k.getMBeanName.endsWith(metricName) }
      .values
      .headOption
      .map(_.asInstanceOf[Gauge[T]])
      .map(_.value)
  }

  private def clearYammerMetricsExcept(names: Set[String]): Unit = {
    for (metricName <- KafkaYammerMetrics.defaultRegistry.allMetrics.keySet.asScala) {
      if (!names.contains(metricName.getMBeanName)) {
        KafkaYammerMetrics.defaultRegistry.removeMetric(metricName)
      }
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


class MonitorableReplicaSelector extends MockReplicaSelector with Monitorable {
  var pluginMetrics = false

  override def withPluginMetrics(metrics: PluginMetrics): Unit = {
    pluginMetrics = true
  }
}
