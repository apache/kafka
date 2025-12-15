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
import kafka.utils.{CoreUtils, Logging, TestUtils}
import org.apache.kafka.common.compress.Compression
import org.apache.kafka.common.{TopicIdPartition, Uuid}
import org.apache.kafka.common.metadata.{FeatureLevelRecord, PartitionChangeRecord, PartitionRecord, TopicRecord}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.record.{MemoryRecords, SimpleRecord}
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.image.{MetadataDelta, MetadataImage, MetadataProvenance}
import org.apache.kafka.metadata.KRaftMetadataCache
import org.apache.kafka.server.common.{KRaftVersion, MetadataVersion}
import org.apache.kafka.server.network.BrokerEndPoint
import org.apache.kafka.server.LeaderEndPoint
import org.apache.kafka.server.util.{MockScheduler, MockTime}
import org.apache.kafka.storage.internals.log.{AppendOrigin, LogDirFailureChannel}
import org.junit.jupiter.api.{AfterEach, BeforeEach}
import org.junit.jupiter.api.Assertions._
import org.mockito.Mockito.mock

import java.io.File
import scala.collection.Map
import scala.jdk.CollectionConverters._

/**
 * Shared test base providing setup/teardown and helpers for LocalLeaderEndPoint tests.
 * Subclasses can customize the log manager by overriding createLogManager(config).
 */
abstract class LocalLeaderEndPointTestBase extends Logging {

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
    val mockLogMgr = createLogManager(config)
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

    val delta = new MetadataDelta(MetadataImage.EMPTY)
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

  /**
   * Subclasses can override to provide a custom LogManager (e.g., with remote enabled).
   */
  protected def createLogManager(config: KafkaConfig) = {
    TestUtils.createLogManager(config.logDirs.asScala.map(new File(_)))
  }

  @AfterEach
  def tearDown(): Unit = {
    CoreUtils.swallow(replicaManager.shutdown(checkpointHW = false), this)
    CoreUtils.swallow(quotaManager.shutdown(), this)
  }

  protected class CallbackResult[T] {
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

  protected def bumpLeaderEpoch(): Unit = {
    val delta = new MetadataDelta(image)
    delta.replay(new PartitionChangeRecord()
      .setTopicId(topicId)
      .setPartitionId(partition)
      .setLeader(sourceBroker.id)
    )

    image = delta.apply(MetadataProvenance.EMPTY)
    replicaManager.applyDelta(delta.topicsDelta, image)
  }

  protected def appendRecords(replicaManager: ReplicaManager,
                              partition: TopicIdPartition,
                              records: MemoryRecords,
                              origin: AppendOrigin = AppendOrigin.CLIENT,
                              requiredAcks: Short = -1): CallbackResult[PartitionResponse] = {
    val result = new CallbackResult[PartitionResponse]()
    def appendCallback(responses: scala.collection.Map[TopicIdPartition, PartitionResponse]): Unit = {
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

  protected def records: MemoryRecords = {
    MemoryRecords.withRecords(Compression.NONE,
      new SimpleRecord("first message".getBytes()),
      new SimpleRecord("second message".getBytes()),
      new SimpleRecord("third message".getBytes()),
    )
  }
}
