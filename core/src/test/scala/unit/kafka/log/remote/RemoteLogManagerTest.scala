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
package kafka.log.remote

import java.io.{ByteArrayInputStream, File, InputStream}
import java.nio.file.Files
import java.util.concurrent.atomic.AtomicBoolean
import java.util.{Collections, Optional, Properties}
import java.{lang, util}
import kafka.log._
import kafka.server.QuotaFactory.UnboundedQuota
import kafka.server._
import kafka.server.checkpoints.LazyOffsetCheckpoints
import kafka.utils.{MockScheduler, MockTime, TestUtils}
import kafka.zk.KafkaZkClient
import org.apache.kafka.common.{Endpoint, TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.record._
import org.apache.kafka.common.requests.FetchRequest.PartitionData
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.server.log.remote.storage.{RemoteLogSegmentMetadata, RemoteStorageManager, _}
import org.easymock.EasyMock.{anyObject, anyString, createMock, expect, replay, reset}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}

class RemoteLogManagerTest {

  val brokerId = 101
  val topicPartition = new TopicPartition("test-topic", 0)
  val time = new MockTime()
  val brokerTopicStats = new BrokerTopicStats
  val metrics = new Metrics
  val rsmConfigPrefix = "rsm.config."
  val rlmmConfigPrefix = "rlmm.config."

  val rsmConfig: Map[String, Any] = Map(
    rsmConfigPrefix + "url" -> "foo.url",
    rsmConfigPrefix + "timeout.ms" -> "1000"
  )
  var logConfig: LogConfig = _
  var tmpDir: File = _
  var replicaManager: ReplicaManager = _
  var logManager: LogManager = _
  var rlmMock: RemoteLogManager = createMock(classOf[RemoteLogManager])

  val rlmConfig: RemoteLogManagerConfig = {
    val props = new Properties
    props.put(RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, true.toString)
    props.put(RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CONFIG_PREFIX_PROP, rsmConfigPrefix)
    props.put(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CONFIG_PREFIX_PROP, rlmmConfigPrefix)
    props.put(RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CLASS_NAME_PROP, "kafka.log.remote.MockRemoteStorageManager")
    props.put(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP, "kafka.log.remote.MockRemoteLogMetadataManager")
    props.put(RemoteLogManagerConfig.REMOTE_LOG_READER_THREADS_PROP, 2.toString)
    props.put(RemoteLogManagerConfig.REMOTE_LOG_READER_MAX_PENDING_TASKS_PROP, 10.toString)
    rsmConfig.foreach(config => props.put(config._1, config._2.toString))
    val config = new AbstractConfig(RemoteLogManagerConfig.CONFIG_DEF, props, false)
    new RemoteLogManagerConfig(config)
  }

  @BeforeEach
  def setup(): Unit = {
    val logProps = createLogProperties(Map.empty)
    logConfig = LogConfig(logProps)

    tmpDir = TestUtils.tempDir()
    val logDir1 = TestUtils.randomPartitionLogDir(tmpDir)
    val logDir2 = TestUtils.randomPartitionLogDir(tmpDir)
    logManager = TestUtils.createLogManager(logDirs = Seq(logDir1, logDir2), defaultConfig = logConfig,
      cleanerConfig = CleanerConfig(enableCleaner = false), time = time, remoteLogManagerConfig = rlmConfig)
    logManager.startup(Set.empty)

    val brokerProps = TestUtils.createBrokerConfig(brokerId, TestUtils.MockZkConnect)
    brokerProps.put(KafkaConfig.LogDirsProp, Seq(logDir1, logDir2).map(_.getAbsolutePath).mkString(","))
    val brokerConfig = KafkaConfig.fromProps(brokerProps)
    val kafkaZkClient: Option[KafkaZkClient] = Some(createMock(classOf[KafkaZkClient]))
    val quotaManagers = QuotaFactory.instantiate(brokerConfig, metrics, time, "")
    val alterIsrManager = TestUtils.createAlterIsrManager()
    replicaManager = new ReplicaManager(
       brokerConfig, metrics, time, kafkaZkClient, new MockScheduler(time),
      logManager, Some(rlmMock), new AtomicBoolean(false), quotaManagers,
      brokerTopicStats, new ZkMetadataCache(brokerId), new LogDirFailureChannel(brokerConfig.logDirs.size),
      alterIsrManager)

    expect(kafkaZkClient.get.getEntityConfigs(anyString(), anyString())).andReturn(
      logProps).anyTimes()
    expect(
      kafkaZkClient.get.conditionalUpdatePath(anyObject(), anyObject(), anyObject(),
        anyObject()))
      .andReturn((true, 0)).anyTimes()
    replay(kafkaZkClient.get)
  }

  @AfterEach
  def tearDown(): Unit = {
    reset(rlmMock)
    brokerTopicStats.close()
    metrics.close()

    logManager.shutdown()
    Utils.delete(tmpDir)
    logManager.liveLogDirs.foreach(Utils.delete)
    replicaManager.shutdown(checkpointHW = false)
  }

  private def createLogProperties(overrides: Map[String, String]): Properties = {
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 1024: java.lang.Integer)
    logProps.put(LogConfig.SegmentIndexBytesProp, 10000: java.lang.Integer)
    logProps.put(LogConfig.RetentionMsProp, 300000: java.lang.Integer)
    overrides.foreach { case (k, v) => logProps.put(k, v) }
    logProps
  }

  @Test
  def testRSMConfigInvocation(): Unit = {

    def logFetcher(tp: TopicPartition): Option[Log] = logManager.getLog(tp)

    // this should initialize RSM
    val logsDirTmp = Files.createTempDirectory("kafka-").toString
    val remoteLogManager = new RemoteLogManager(logFetcher, (_, _) => {}, rlmConfig, time, 1, "", logsDirTmp, new BrokerTopicStats)
    val securityProtocol = SecurityProtocol.PLAINTEXT
    val listenerName = ListenerName.forSecurityProtocol(securityProtocol)
    val endpoint = new Endpoint(listenerName.value(), securityProtocol, "localhost", 9092)
    remoteLogManager.onEndpointCreated(endpoint)

    assertEquals(rsmConfig.size,
      rsmConfig.count { case (k, v) =>
        val keyWithoutPrefix = k.split(rsmConfigPrefix)(1)
        MockRemoteStorageManager.configs.get(keyWithoutPrefix) == v.toString
      })
  }

  @Test
  def testRemoteLogRecordsFetch(): Unit = {
    // return the lastOffset to verify when out of range offsets are requested.
    expect(rlmMock.close()).anyTimes()
    replay(rlmMock)

    val leaderEpoch = 1
    val partition = replicaManager.createPartition(topicPartition)

    val leaderState = new LeaderAndIsrPartitionState()
      .setControllerEpoch(1)
      .setLeader(brokerId)
      .setLeaderEpoch(leaderEpoch)
      .setIsr(Collections.singletonList(brokerId))
      .setZkVersion(1)
      .setReplicas(Collections.singletonList(brokerId))
      .setIsNew(true)
    partition.makeLeader(leaderState, new LazyOffsetCheckpoints(replicaManager.getHighWatermarkCheckpoints), None)

    val recordsArray = Array(
      new SimpleRecord("k1".getBytes, "v1".getBytes),
      new SimpleRecord("k2".getBytes, "v2".getBytes),
      new SimpleRecord("k3".getBytes, "v3".getBytes),
      new SimpleRecord("k4".getBytes, "v4".getBytes)
    )
    val inputRecords: util.List[SimpleRecord] = util.Arrays.asList(recordsArray: _*)

    val inputMemoryRecords = Map(topicPartition -> MemoryRecords.withRecords(0L, CompressionType.NONE, leaderEpoch,
      recordsArray: _*))

    replicaManager.appendRecords(timeout = 30000, requiredAcks = 1, internalTopicsAllowed = true,
      origin = AppendOrigin.Client, entriesPerPartition = inputMemoryRecords, responseCallback = _ => ())

    def logReadResultFor(fetchOffset: Long): LogReadResult = {
      val partitionInfo = Seq((topicPartition, new PartitionData(fetchOffset, 0, 1000,
        Optional.of(leaderEpoch))))
      val readRecords = replicaManager.readFromLocalLog(100, fetchOnlyFromLeader = true, FetchTxnCommitted, 1000,
        hardMaxBytesLimit = false, partitionInfo, UnboundedQuota, None)
      if (readRecords.isEmpty) null else readRecords.last._2
    }

    val logReadResult_0 = logReadResultFor(0L)
    val receivedRecords = logReadResult_0.info.records
    // check the records are same
    val result = new util.ArrayList[SimpleRecord]()
    receivedRecords.records().forEach((t: Record) => result.add(new SimpleRecord(t)))
    assertEquals(inputRecords, result)

    val outOfRangeOffset = logManager.getLog(topicPartition).get.logEndOffset + 1

    // fetching offsets beyond local log would result in fetching from remote log, it is mocked to return lastOffset,
    //nextLocalOffset should be lastOffset +1
    val logReadResult = logReadResultFor(outOfRangeOffset)
    // fetch response should have no records as it is to indicate that the requested fetch messages are in
    // remote tier and the next offset available locally is sent as `nextLocalOffset` so that follower replica can
    // start fetching that for local storage.
    assertTrue(logReadResult.info.records.sizeInBytes() == 0)
  }

  @Test
  def testRLMConfig(): Unit = {
    val key = "hello"
    val props: Properties = new Properties()
    props.put(RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CONFIG_PREFIX_PROP, rsmConfigPrefix)
    props.put(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CONFIG_PREFIX_PROP, rlmmConfigPrefix)
    props.put(rlmmConfigPrefix + key, "world")
    props.put("remote.log.metadata.y", "z")
    val rlmmConfig = new RemoteLogManagerConfig(new AbstractConfig(RemoteLogManagerConfig.CONFIG_DEF, props))
      .remoteLogMetadataManagerProps()

    assertEquals(props.get(rlmmConfigPrefix + key), rlmmConfig.get(key))
    assertFalse(rlmmConfig.containsKey("remote.log.metadata.y"))
  }

  @Test
  def testFindHighestRemoteOffset(): Unit = {

    // FIXME(@kamalcph): Improve this test
    def logFetcher(tp: TopicPartition): Option[Log] = logManager.getLog(tp)

    // this should initialize RSM
    val logsDirTmp = Files.createTempDirectory("kafka-").toString
    val remoteLogManager = new RemoteLogManager(logFetcher, (_, _) => {}, rlmConfig, time, 1, "", logsDirTmp, new BrokerTopicStats)

    val idPartition = new TopicIdPartition(Uuid.randomUuid(), topicPartition)
    assertEquals(-1L, remoteLogManager.findHighestRemoteOffset(idPartition))
  }
}

object MockRemoteStorageManager {
  var configs: util.Map[String, _] = _
}

class MockRemoteStorageManager extends RemoteStorageManager {

  override def configure(configs: util.Map[String, _]): Unit = {
    MockRemoteStorageManager.configs = configs
  }

  override def copyLogSegmentData(remoteLogSegmentMetadata: RemoteLogSegmentMetadata, logSegmentData: LogSegmentData): Unit = {}

  override def fetchLogSegment(remoteLogSegmentMetadata: RemoteLogSegmentMetadata, startPosition: Int): InputStream = {
    new ByteArrayInputStream(Array.emptyByteArray)
  }

  override def fetchLogSegment(remoteLogSegmentMetadata: RemoteLogSegmentMetadata, startPosition: Int, endPosition: Int): InputStream = {
    new ByteArrayInputStream(Array.emptyByteArray)
  }

  override def fetchIndex(remoteLogSegmentMetadata: RemoteLogSegmentMetadata, indexType: RemoteStorageManager.IndexType): InputStream = {
    new ByteArrayInputStream(Array.emptyByteArray)
  }

  override def deleteLogSegmentData(remoteLogSegmentMetadata: RemoteLogSegmentMetadata): Unit = {}

  override def close(): Unit = {}
}

class MockRemoteLogMetadataManager extends RemoteLogMetadataManager {
  override def addRemoteLogSegmentMetadata(remoteLogSegmentMetadata: RemoteLogSegmentMetadata): Unit = {}

  override def updateRemoteLogSegmentMetadata(remoteLogSegmentMetadataUpdate: RemoteLogSegmentMetadataUpdate): Unit = {}

  override def remoteLogSegmentMetadata(topicIdPartition: TopicIdPartition, epochForOffset: Int, offset: Long): Optional[RemoteLogSegmentMetadata] = {
    Optional.empty()
  }

  override def highestOffsetForEpoch(topicIdPartition: TopicIdPartition, leaderEpoch: Int): Optional[lang.Long] = {
    Optional.empty()
  }

  override def putRemotePartitionDeleteMetadata(remotePartitionDeleteMetadata: RemotePartitionDeleteMetadata): Unit = {}

  override def listRemoteLogSegments(topicIdPartition: TopicIdPartition): util.Iterator[RemoteLogSegmentMetadata] = {
    Collections.emptyIterator()
  }
  override def listRemoteLogSegments(topicIdPartition: TopicIdPartition, leaderEpoch: Int): util.Iterator[RemoteLogSegmentMetadata] = {
    Collections.emptyIterator()
  }
  override def onPartitionLeadershipChanges(leaderPartitions: util.Set[TopicIdPartition], followerPartitions: util.Set[TopicIdPartition]): Unit = {}

  override def onStopPartitions(partitions: util.Set[TopicIdPartition]): Unit = {}

  override def configure(configs: util.Map[String, _]): Unit = {}

  override def close(): Unit = {}
}