/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import kafka.server.QuotaFactory.QuotaManagers
import kafka.utils.{Logging, TestUtils}
import org.apache.kafka.common.compress.Compression
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.{TopicIdPartition, Uuid}
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderPartition
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.EpochEndOffset
import org.apache.kafka.common.metadata.{FeatureLevelRecord, PartitionChangeRecord, PartitionRecord, TopicRecord}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.internal.{MemoryRecords, SimpleRecord}
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.image.{MetadataDelta, MetadataImage, MetadataProvenance}
import org.apache.kafka.metadata.KRaftMetadataCache
import org.apache.kafka.server.common.{KRaftVersion, MetadataVersion, OffsetAndEpoch}
import org.apache.kafka.server.network.BrokerEndPoint
import org.apache.kafka.server.LeaderEndPoint
import org.apache.kafka.server.partition.AlterPartitionManager
import org.apache.kafka.server.util.{MockScheduler, MockTime}
import org.apache.kafka.storage.internals.log.{AppendOrigin, LogConfig, LogDirFailureChannel}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.api.Assertions._
import org.mockito.Mockito.mock

import java.io.File
import java.util.{Properties, Map => JMap}
import scala.collection.Map
import scala.jdk.CollectionConverters._

class LocalLeaderEndPointTest extends Logging {

  val time = new MockTime
  val topicId = Uuid.randomUuid()
  val topic = "test"
  val partition = 5
  val topicIdPartition = new TopicIdPartition(topicId, partition, topic)
  val topicPartition = topicIdPartition.topicPartition()
  val sourceBroker: BrokerEndPoint = new BrokerEndPoint(0, "localhost", 9092)
  var replicaManager: ReplicaManager = _
  var endPoint: LeaderEndPoint = _
  var quotaManager: QuotaManagers = _
  var image: MetadataImage = _

  @BeforeEach
  def setUp(): Unit = {
    val props = TestUtils.createBrokerConfig(sourceBroker.id, port = sourceBroker.port)
    val config = KafkaConfig.fromProps(props)

    val logProps = new Properties()
    logProps.put(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true")
    // Keep cleanup.policy=delete (default), not compact, so remote storage is allowed
    val defaultLogConfig = LogConfig.fromProps(Map.empty[String, Object].asJava, logProps)
    val mockLogMgr = TestUtils.createLogManager(
      config.logDirs.asScala.map(new File(_)),
      defaultConfig = defaultLogConfig,
      remoteStorageSystemEnable = true
    )
    val alterPartitionManager = mock(classOf[AlterPartitionManager])
    val metrics = new Metrics
    quotaManager = QuotaFactory.instantiate(config, metrics, time, "", "")
    replicaManager = new ReplicaManager(
      metrics = metrics,
      config = config,
      time = time,
      scheduler = new MockScheduler(time),
      logManager = mockLogMgr,
      quotaManagers = quotaManager,
      metadataCache = new KRaftMetadataCache(config.brokerId, () => KRaftVersion.KRAFT_VERSION_0),
      logDirFailureChannel = new LogDirFailureChannel(config.logDirs.size),
      alterPartitionManager = alterPartitionManager
    )

    val delta = new MetadataDelta.Builder()
      .setImage(MetadataImage.EMPTY)
      .build()
    delta.replay(new FeatureLevelRecord()
      .setName(MetadataVersion.FEATURE_NAME)
      .setFeatureLevel(MetadataVersion.MINIMUM_VERSION.featureLevel())
    )
    delta.replay(new TopicRecord()
      .setName(topic)
      .setTopicId(topicId)
    )
    delta.replay(new PartitionRecord()
      .setPartitionId(partition)
      .setTopicId(topicId)
      .setReplicas(java.util.List.of[Integer](sourceBroker.id))
      .setIsr(java.util.List.of[Integer](sourceBroker.id))
      .setLeader(sourceBroker.id)
      .setLeaderEpoch(0)
      .setPartitionEpoch(0)
    )

    image = delta.apply(MetadataProvenance.EMPTY)
    replicaManager.applyDelta(delta.topicsDelta(), image)

    replicaManager.getPartitionOrException(topicPartition)
      .localLogOrException
    endPoint = new LocalLeaderEndPoint(
      sourceBroker,
      config,
      replicaManager,
      QuotaFactory.UNBOUNDED_QUOTA
    )
  }

  @AfterEach
  def tearDown(): Unit = {
    Utils.swallow(this.logger.underlying, () => replicaManager.shutdown(checkpointHW = false))
    Utils.swallow(this.logger.underlying, () => quotaManager.shutdown())
  }

  @Test
  def testFetchLatestOffset(): Unit = {
    appendRecords(replicaManager, topicIdPartition, records)
      .onFire(response => assertEquals(Errors.NONE, response.error))
    assertEquals(new OffsetAndEpoch(3L, 0), endPoint.fetchLatestOffset(topicPartition, 0))
    bumpLeaderEpoch()
    appendRecords(replicaManager, topicIdPartition, records)
      .onFire(response => assertEquals(Errors.NONE, response.error))
    assertEquals(new OffsetAndEpoch(6L, 1), endPoint.fetchLatestOffset(topicPartition, 7))
  }

  @Test
  def testFetchEarliestOffset(): Unit = {
    appendRecords(replicaManager, topicIdPartition, records)
      .onFire(response => assertEquals(Errors.NONE, response.error))
    assertEquals(new OffsetAndEpoch(0L, 0), endPoint.fetchEarliestOffset(topicPartition, 0))

    bumpLeaderEpoch()
    appendRecords(replicaManager, topicIdPartition, records)
      .onFire(response => assertEquals(Errors.NONE, response.error))
    replicaManager.deleteRecords(timeout = 1000L, Map(topicPartition -> 3), _ => ())
    assertEquals(new OffsetAndEpoch(3L, 1), endPoint.fetchEarliestOffset(topicPartition, 7))
  }

  @Test
  def testFetchEarliestLocalOffset(): Unit = {
    appendRecords(replicaManager, topicIdPartition, records)
      .onFire(response => assertEquals(Errors.NONE, response.error))
    assertEquals(new OffsetAndEpoch(0L, 0), endPoint.fetchEarliestLocalOffset(topicPartition, 0))

    bumpLeaderEpoch()
    appendRecords(replicaManager, topicIdPartition, records)
      .onFire(response => assertEquals(Errors.NONE, response.error))
    replicaManager.logManager.getLog(topicPartition).ifPresent(_.updateLocalLogStartOffset(3))
    assertEquals(new OffsetAndEpoch(0L, 0), endPoint.fetchEarliestOffset(topicPartition, 7))
    assertEquals(new OffsetAndEpoch(3L, 1), endPoint.fetchEarliestLocalOffset(topicPartition, 7))
  }

  @Test
  def testFetchEpochEndOffsets(): Unit = {
    appendRecords(replicaManager, topicIdPartition, records)
      .onFire(response => assertEquals(Errors.NONE, response.error))

    var result = endPoint.fetchEpochEndOffsets(JMap.of(
      topicPartition, new OffsetForLeaderPartition()
        .setPartition(topicPartition.partition)
        .setLeaderEpoch(0))
    ).asScala

    var expected = Map(
      topicPartition -> new EpochEndOffset()
        .setPartition(topicPartition.partition)
        .setErrorCode(Errors.NONE.code)
        .setLeaderEpoch(0)
        .setEndOffset(3L)
    )

    assertEquals(expected, result)

    // Change leader epoch and end offset, and verify the behavior again.
    bumpLeaderEpoch()
    bumpLeaderEpoch()
    assertEquals(2, replicaManager.getPartitionOrException(topicPartition).getLeaderEpoch)

    appendRecords(replicaManager, topicIdPartition, records)
      .onFire(response => assertEquals(Errors.NONE, response.error))

    result = endPoint.fetchEpochEndOffsets(JMap.of(
      topicPartition, new OffsetForLeaderPartition()
        .setPartition(topicPartition.partition)
        .setLeaderEpoch(2)
    )).asScala

    expected = Map(
      topicPartition -> new EpochEndOffset()
        .setPartition(topicPartition.partition)
        .setErrorCode(Errors.NONE.code)
        .setLeaderEpoch(2)
        .setEndOffset(6L)
    )

    assertEquals(expected, result)

    // Check missing epoch: 1, we expect the API to return (leader_epoch=0, end_offset=3).
    result = endPoint.fetchEpochEndOffsets(JMap.of(
      topicPartition, new OffsetForLeaderPartition()
        .setPartition(topicPartition.partition)
        .setLeaderEpoch(1)
    )).asScala

    expected = Map(
      topicPartition -> new EpochEndOffset()
        .setPartition(topicPartition.partition)
        .setErrorCode(Errors.NONE.code)
        .setLeaderEpoch(0)
        .setEndOffset(3L))

    assertEquals(expected, result)

    // Check missing epoch: 5, we expect the API to return (leader_epoch=-1, end_offset=-1)
    result = endPoint.fetchEpochEndOffsets(JMap.of(
      topicPartition, new OffsetForLeaderPartition()
        .setPartition(topicPartition.partition)
        .setLeaderEpoch(5)
    )).asScala

    expected = Map(
      topicPartition -> new EpochEndOffset()
        .setPartition(topicPartition.partition)
        .setErrorCode(Errors.NONE.code)
        .setLeaderEpoch(-1)
        .setEndOffset(-1L)
    )

    assertEquals(expected, result)
  }

  @Test
  def testEarliestPendingUploadOffsetWhenNoSegmentsUploaded(): Unit = {
    // Append some records; no remote upload happened yet
    appendRecords(replicaManager, topicIdPartition, records)
      .onFire(response => assertEquals(Errors.NONE, response.error))

    val expected = endPoint.fetchEarliestOffset(topicPartition, 0)
    val result = endPoint.fetchEarliestPendingUploadOffset(topicPartition, 0)
    assertEquals(expected, result)
  }

  @Test
  def testEarliestPendingUploadOffsetWhenLocalStartGreaterThanStart(): Unit = {
    appendRecords(replicaManager, topicIdPartition, records)
      .onFire(response => assertEquals(Errors.NONE, response.error))

    // Bump epoch and advance local log start offset without changing log start offset
    bumpLeaderEpoch()
    replicaManager.logManager.getLog(topicPartition).ifPresent(_.updateLocalLogStartOffset(3))

    val result = endPoint.fetchEarliestPendingUploadOffset(topicPartition, 1)
    assertEquals(new OffsetAndEpoch(-1L, -1), result)
  }

  @Test
  def testEarliestPendingUploadOffsetWhenHighestRemoteOffsetKnown(): Unit = {
    appendRecords(replicaManager, topicIdPartition, records)
      .onFire(response => assertEquals(Errors.NONE, response.error))

    // Highest remote is 1 => earliest pending should be max(1+1, logStart) = 2,
    // and all records were appended under leader epoch 0
    val log = replicaManager.getPartitionOrException(topicPartition).localLogOrException
    log.updateHighestOffsetInRemoteStorage(1)

    val result = endPoint.fetchEarliestPendingUploadOffset(topicPartition, 0)
    assertEquals(new OffsetAndEpoch(2L, 0), result)
  }

  @Test
  def testEarliestPendingUploadOffsetResolvesEpochOfPendingOffset(): Unit = {
    // Append records under leader epoch 0 (offsets 0-2) and leader epoch 1 (offsets 3-5)
    appendRecords(replicaManager, topicIdPartition, records)
      .onFire(response => assertEquals(Errors.NONE, response.error))
    bumpLeaderEpoch()
    appendRecords(replicaManager, topicIdPartition, records)
      .onFire(response => assertEquals(Errors.NONE, response.error))

    val log = replicaManager.getPartitionOrException(topicPartition).localLogOrException
    log.updateHighestOffsetInRemoteStorage(3)

    // Earliest pending upload offset is max(3 + 1, max(0, 0)) = 4, which was appended under epoch 1
    val result = endPoint.fetchEarliestPendingUploadOffset(topicPartition, 1)
    assertEquals(new OffsetAndEpoch(4L, 1), result)
  }

  @Test
  def testEarliestPendingUploadOffsetWhenEpochCannotBeResolved(): Unit = {
    appendRecords(replicaManager, topicIdPartition, records)
      .onFire(response => assertEquals(Errors.NONE, response.error))

    val log = replicaManager.getPartitionOrException(topicPartition).localLogOrException
    log.updateHighestOffsetInRemoteStorage(1)
    // Wipe the epoch cache so the epoch of the pending upload offset cannot be resolved. The
    // ListOffsets-based path (UnifiedLog#fetchEarliestPendingUploadOffset) leaves the epoch unset
    // in this case, which serializes as -1, so the local path must report -1 as well instead of
    // fabricating epoch 0.
    log.leaderEpochCache().clearAndFlush()

    val result = endPoint.fetchEarliestPendingUploadOffset(topicPartition, 0)
    assertEquals(new OffsetAndEpoch(2L, -1), result)
  }

  @Test
  def testFetchOffsetsReturnUndefinedEpochWhenEpochCannotBeResolved(): Unit = {
    appendRecords(replicaManager, topicIdPartition, records)
      .onFire(response => assertEquals(Errors.NONE, response.error))

    val log = replicaManager.getPartitionOrException(topicPartition).localLogOrException
    log.leaderEpochCache().clearAndFlush()

    assertEquals(new OffsetAndEpoch(0L, -1), endPoint.fetchEarliestOffset(topicPartition, 0))
    assertEquals(new OffsetAndEpoch(3L, -1), endPoint.fetchLatestOffset(topicPartition, 0))
    assertEquals(new OffsetAndEpoch(0L, -1), endPoint.fetchEarliestLocalOffset(topicPartition, 0))
  }

  @Test
  def testEarliestPendingUploadOffsetWhenLocalStartGreaterThanStartWithKnownRemoteOffset(): Unit = {
    // Append records to create initial log state
    appendRecords(replicaManager, topicIdPartition, records)
      .onFire(response => assertEquals(Errors.NONE, response.error))

    val log = replicaManager.getPartitionOrException(topicPartition).localLogOrException

    // Simulate remote upload up to offset 5
    log.updateHighestOffsetInRemoteStorage(5)

    // Simulate local log start advancing to offset 8
    log.updateLocalLogStartOffset(8)

    // Expected: max(highestRemoteOffset + 1, max(logStartOffset, localLogStartOffset))
    //         = max(5 + 1, max(0, 8)) = max(6, 8) = 8, appended under leader epoch 0
    val result = endPoint.fetchEarliestPendingUploadOffset(topicPartition, 0)
    assertEquals(new OffsetAndEpoch(8L, 0), result)
  }

  @Test
  def testEarliestPendingUploadOffsetWhenLogStartGreaterThanLocalStartWithLowRemoteOffset(): Unit = {
    // Append 12 records (offsets 0-11)
    for (_ <- 1 to 4) {
      appendRecords(replicaManager, topicIdPartition, records)
        .onFire(response => assertEquals(Errors.NONE, response.error))
    }

    // Delete records to advance logStartOffset to 10
    replicaManager.deleteRecords(timeout = 1000L, Map(topicPartition -> 10), _ => ())

    val log = replicaManager.getPartitionOrException(topicPartition).localLogOrException

    // Set localLogStartOffset to 3 (less than logStartOffset)
    log.updateLocalLogStartOffset(3)

    // Set highestRemoteOffset to 5 (less than logStartOffset)
    log.updateHighestOffsetInRemoteStorage(5)

    // Expected: max(5+1, max(10, 3)) = max(6, 10) = 10, appended under leader epoch 0
    val result = endPoint.fetchEarliestPendingUploadOffset(topicPartition, 0)
    assertEquals(new OffsetAndEpoch(10L, 0), result)
  }

  @Test
  def testEarliestPendingUploadOffsetWhenRemoteOffsetDominatesLogStart(): Unit = {
    // Append 18 records (offsets 0-17)
    for (_ <- 1 to 6) {
      appendRecords(replicaManager, topicIdPartition, records)
        .onFire(response => assertEquals(Errors.NONE, response.error))
    }

    // Delete records to advance logStartOffset to 10
    replicaManager.deleteRecords(timeout = 1000L, Map(topicPartition -> 10), _ => ())

    val log = replicaManager.getPartitionOrException(topicPartition).localLogOrException

    // Set localLogStartOffset to 3 (less than logStartOffset)
    log.updateLocalLogStartOffset(3)

    // Set highestRemoteOffset to 15 (greater than logStartOffset)
    log.updateHighestOffsetInRemoteStorage(15)

    // Expected: max(15+1, max(10, 3)) = max(16, 10) = 16, appended under leader epoch 0
    val result = endPoint.fetchEarliestPendingUploadOffset(topicPartition, 0)
    assertEquals(new OffsetAndEpoch(16L, 0), result)
  }

  @Test
  def testEarliestPendingUploadOffsetWhenBothStartOffsetsEqualAndDominateRemote(): Unit = {
    // Append 12 records (offsets 0-11)
    for (_ <- 1 to 4) {
      appendRecords(replicaManager, topicIdPartition, records)
        .onFire(response => assertEquals(Errors.NONE, response.error))
    }

    // Delete records to advance both logStartOffset and localLogStartOffset to 10
    replicaManager.deleteRecords(timeout = 1000L, Map(topicPartition -> 10), _ => ())

    val log = replicaManager.getPartitionOrException(topicPartition).localLogOrException

    // Verify both offsets are equal (deleteRecords updates both)
    assertEquals(10L, log.logStartOffset())
    assertEquals(10L, log.localLogStartOffset())

    // Set highestRemoteOffset to 5 (less than start offsets)
    log.updateHighestOffsetInRemoteStorage(5)

    // Expected: max(5+1, max(10, 10)) = max(6, 10) = 10, appended under leader epoch 0
    val result = endPoint.fetchEarliestPendingUploadOffset(topicPartition, 0)
    assertEquals(new OffsetAndEpoch(10L, 0), result)
  }

  private class CallbackResult[T] {
    private var value: Option[T] = None
    private var fun: Option[T => Unit] = None

    private def hasFired: Boolean = {
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

  private def bumpLeaderEpoch(): Unit = {
    val delta = new MetadataDelta.Builder()
      .setImage(image)
      .build()
    delta.replay(new PartitionChangeRecord()
      .setTopicId(topicId)
      .setPartitionId(partition)
      .setLeader(sourceBroker.id)
    )

    image = delta.apply(MetadataProvenance.EMPTY)
    replicaManager.applyDelta(delta.topicsDelta, image)
  }

  private def appendRecords(replicaManager: ReplicaManager,
                            partition: TopicIdPartition,
                            records: MemoryRecords,
                            origin: AppendOrigin = AppendOrigin.CLIENT,
                            requiredAcks: Short = -1): CallbackResult[PartitionResponse] = {
    val result = new CallbackResult[PartitionResponse]()
    def appendCallback(responses: JMap[TopicIdPartition, PartitionResponse]): Unit = {
      val response = responses.get(partition)
      assertNotNull(response)
      result.fire(response)
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

  private def records: MemoryRecords = {
    MemoryRecords.withRecords(Compression.NONE,
      new SimpleRecord("first message".getBytes()),
      new SimpleRecord("second message".getBytes()),
      new SimpleRecord("third message".getBytes()),
    )
  }
}
