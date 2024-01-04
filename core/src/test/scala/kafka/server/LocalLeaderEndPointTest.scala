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

import kafka.cluster.BrokerEndPoint
import kafka.server.QuotaFactory.QuotaManagers
import kafka.server.checkpoints.LazyOffsetCheckpoints
import kafka.utils.{CoreUtils, Logging, TestUtils}
import org.apache.kafka.common.{Node, TopicPartition, Uuid}
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderPartition
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.EpochEndOffset
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record.{CompressionType, MemoryRecords, SimpleRecord}
import org.apache.kafka.common.requests.LeaderAndIsrRequest
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.server.common.OffsetAndEpoch
import org.apache.kafka.server.util.{MockScheduler, MockTime}
import org.apache.kafka.storage.internals.log.{AppendOrigin, LogDirFailureChannel}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.api.Assertions._
import org.mockito.Mockito.mock

import java.io.File
import java.util.Collections
import scala.collection.{Map, Seq}
import scala.jdk.CollectionConverters._

class LocalLeaderEndPointTest extends Logging {

  val time = new MockTime
  val topicId: Uuid = Uuid.randomUuid()
  val topic = "test"
  val topicPartition = new TopicPartition(topic, 5)
  val sourceBroker: BrokerEndPoint = BrokerEndPoint(0, "localhost", 9092)
  var replicaManager: ReplicaManager = _
  var endPoint: LeaderEndPoint = _
  var quotaManager: QuotaManagers = _

  @BeforeEach
  def setUp(): Unit = {
    val props = TestUtils.createBrokerConfig(sourceBroker.id, TestUtils.MockZkConnect, port = sourceBroker.port)
    val config = KafkaConfig.fromProps(props)
    val mockLogMgr = TestUtils.createLogManager(config.logDirs.map(new File(_)))
    val alterPartitionManager = mock(classOf[AlterPartitionManager])
    val metrics = new Metrics
    quotaManager = QuotaFactory.instantiate(config, metrics, time, "")
    replicaManager = new ReplicaManager(
      metrics = metrics,
      config = config,
      time = time,
      scheduler = new MockScheduler(time),
      logManager = mockLogMgr,
      quotaManagers = quotaManager,
      metadataCache = MetadataCache.zkMetadataCache(config.brokerId, config.interBrokerProtocolVersion),
      logDirFailureChannel = new LogDirFailureChannel(config.logDirs.size),
      alterPartitionManager = alterPartitionManager)
    val partition = replicaManager.createPartition(topicPartition)
    partition.createLogIfNotExists(isNew = false, isFutureReplica = false,
      new LazyOffsetCheckpoints(replicaManager.highWatermarkCheckpoints), None)
    // Make this replica the leader.
    val leaderAndIsrRequest = buildLeaderAndIsrRequest(leaderEpoch = 0)
    replicaManager.becomeLeaderOrFollower(0, leaderAndIsrRequest, (_, _) => ())
    replicaManager.getPartitionOrException(topicPartition)
      .localLogOrException
    endPoint = new LocalLeaderEndPoint(sourceBroker, config, replicaManager, QuotaFactory.UnboundedQuota)
  }

  @AfterEach
  def tearDown(): Unit = {
    CoreUtils.swallow(replicaManager.shutdown(checkpointHW = false), this)
    CoreUtils.swallow(quotaManager.shutdown(), this)
  }

  @Test
  def testFetchLatestOffset(): Unit = {
    appendRecords(replicaManager, topicPartition, records)
      .onFire(response => assertEquals(Errors.NONE, response.error))
    assertEquals(new OffsetAndEpoch(3L, 0), endPoint.fetchLatestOffset(topicPartition, currentLeaderEpoch = 0))
    val leaderAndIsrRequest =  buildLeaderAndIsrRequest(leaderEpoch = 4)
    replicaManager.becomeLeaderOrFollower(0, leaderAndIsrRequest, (_, _) => ())
    appendRecords(replicaManager, topicPartition, records)
      .onFire(response => assertEquals(Errors.NONE, response.error))
    assertEquals(new OffsetAndEpoch(6L, 4), endPoint.fetchLatestOffset(topicPartition, currentLeaderEpoch = 7))
  }

  @Test
  def testFetchEarliestOffset(): Unit = {
    appendRecords(replicaManager, topicPartition, records)
      .onFire(response => assertEquals(Errors.NONE, response.error))
    assertEquals(new OffsetAndEpoch(0L, 0), endPoint.fetchEarliestOffset(topicPartition, currentLeaderEpoch = 0))

    val leaderAndIsrRequest = buildLeaderAndIsrRequest(leaderEpoch = 4)
    replicaManager.becomeLeaderOrFollower(0, leaderAndIsrRequest, (_, _) => ())
    appendRecords(replicaManager, topicPartition, records)
      .onFire(response => assertEquals(Errors.NONE, response.error))
    replicaManager.deleteRecords(timeout = 1000L, Map(topicPartition -> 3), _ => ())
    assertEquals(new OffsetAndEpoch(3L, 4), endPoint.fetchEarliestOffset(topicPartition, currentLeaderEpoch = 7))
  }

  @Test
  def testFetchEarliestLocalOffset(): Unit = {
    appendRecords(replicaManager, topicPartition, records)
      .onFire(response => assertEquals(Errors.NONE, response.error))
    assertEquals(new OffsetAndEpoch(0L, 0), endPoint.fetchEarliestLocalOffset(topicPartition, currentLeaderEpoch = 0))

    val leaderAndIsrRequest = buildLeaderAndIsrRequest(leaderEpoch = 4)
    replicaManager.becomeLeaderOrFollower(0, leaderAndIsrRequest, (_, _) => ())
    appendRecords(replicaManager, topicPartition, records)
      .onFire(response => assertEquals(Errors.NONE, response.error))
    replicaManager.logManager.getLog(topicPartition).foreach(log => log._localLogStartOffset = 3)
    assertEquals(new OffsetAndEpoch(0L, 0), endPoint.fetchEarliestOffset(topicPartition, currentLeaderEpoch = 7))
    assertEquals(new OffsetAndEpoch(3L, 4), endPoint.fetchEarliestLocalOffset(topicPartition, currentLeaderEpoch = 7))
  }

  @Test
  def testFetchEpochEndOffsets(): Unit = {
    appendRecords(replicaManager, topicPartition, records)
      .onFire(response => assertEquals(Errors.NONE, response.error))

    var result = endPoint.fetchEpochEndOffsets(Map(
      topicPartition -> new OffsetForLeaderPartition()
        .setPartition(topicPartition.partition)
        .setLeaderEpoch(0)))

    var expected = Map(
      topicPartition -> new EpochEndOffset()
        .setPartition(topicPartition.partition)
        .setErrorCode(Errors.NONE.code)
        .setLeaderEpoch(0)
        .setEndOffset(3L))

    assertEquals(expected, result)

    // Change leader epoch and end offset, and verify the behavior again.
    val leaderAndIsrRequest = buildLeaderAndIsrRequest(leaderEpoch = 4)
    replicaManager.becomeLeaderOrFollower(0, leaderAndIsrRequest, (_, _) => ())
    appendRecords(replicaManager, topicPartition, records)
      .onFire(response => assertEquals(Errors.NONE, response.error))

    result = endPoint.fetchEpochEndOffsets(Map(
      topicPartition -> new OffsetForLeaderPartition()
        .setPartition(topicPartition.partition)
        .setLeaderEpoch(4)))

    expected = Map(
      topicPartition -> new EpochEndOffset()
        .setPartition(topicPartition.partition)
        .setErrorCode(Errors.NONE.code)
        .setLeaderEpoch(4)
        .setEndOffset(6L))

    assertEquals(expected, result)

    // Check missing epoch: 3, we expect the API to return (leader_epoch=0, end_offset=3).
    result = endPoint.fetchEpochEndOffsets(Map(
      topicPartition -> new OffsetForLeaderPartition()
        .setPartition(topicPartition.partition)
        .setLeaderEpoch(3)))

    expected = Map(
      topicPartition -> new EpochEndOffset()
        .setPartition(topicPartition.partition)
        .setErrorCode(Errors.NONE.code)
        .setLeaderEpoch(0)
        .setEndOffset(3L))

    assertEquals(expected, result)

    // Check missing epoch: 5, we expect the API to return (leader_epoch=-1, end_offset=-1)
    result = endPoint.fetchEpochEndOffsets(Map(
      topicPartition -> new OffsetForLeaderPartition()
        .setPartition(topicPartition.partition)
        .setLeaderEpoch(5)))

    expected = Map(
      topicPartition -> new EpochEndOffset()
        .setPartition(topicPartition.partition)
        .setErrorCode(Errors.NONE.code)
        .setLeaderEpoch(-1)
        .setEndOffset(-1L))

    assertEquals(expected, result)
  }

  private class CallbackResult[T] {
    private var value: Option[T] = None
    private var fun: Option[T => Unit] = None

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

  private def buildLeaderAndIsrRequest(leaderEpoch: Int): LeaderAndIsrRequest = {
    val brokerList = Seq[Integer](sourceBroker.id).asJava
    val topicIds = Collections.singletonMap(topic, topicId)
    new LeaderAndIsrRequest.Builder(ApiKeys.LEADER_AND_ISR.latestVersion, 0, 0, 0,
      Seq(new LeaderAndIsrPartitionState()
        .setTopicName(topic)
        .setPartitionIndex(topicPartition.partition())
        .setControllerEpoch(0)
        .setLeader(sourceBroker.id)
        .setLeaderEpoch(leaderEpoch)
        .setIsr(brokerList)
        .setPartitionEpoch(0)
        .setReplicas(brokerList)
        .setIsNew(false)).asJava,
      topicIds,
      Set(node(sourceBroker)).asJava).build()
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

  private def node(endPoint: BrokerEndPoint): Node = {
    new Node(endPoint.id, endPoint.host, endPoint.port)
  }

  private def records: MemoryRecords = {
    MemoryRecords.withRecords(CompressionType.NONE,
      new SimpleRecord("first message".getBytes()),
      new SimpleRecord("second message".getBytes()),
      new SimpleRecord("third message".getBytes()),
    )
  }
}
