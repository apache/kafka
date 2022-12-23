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

import java.net.InetAddress
import java.util
import java.util.concurrent.{CompletableFuture, Executors, LinkedBlockingQueue, TimeUnit}
import java.util.{Optional, Properties}
import kafka.api.LeaderAndIsr
import kafka.log.LogConfig
import kafka.server.metadata.KRaftMetadataCache
import kafka.server.metadata.MockConfigRepository
import kafka.utils.TestUtils.waitUntilTrue
import kafka.utils.{MockTime, ShutdownableThread, TestUtils}
import org.apache.kafka.common.metadata.RegisterBrokerRecord
import org.apache.kafka.common.metadata.{PartitionChangeRecord, PartitionRecord, TopicRecord}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record.SimpleRecord
import org.apache.kafka.common.replica.ClientMetadata.DefaultClientMetadata
import org.apache.kafka.common.requests.{FetchRequest, ProduceResponse}
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.{IsolationLevel, TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.image.{MetadataDelta, MetadataImage}
import org.apache.kafka.metadata.LeaderRecoveryState
import org.apache.kafka.metadata.PartitionRegistration
import org.apache.kafka.server.log.internals.{AppendOrigin, LogDirFailureChannel}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, Test}
import org.mockito.Mockito

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.Random

class ReplicaManagerConcurrencyTest {

  private val time = new MockTime()
  private val metrics = new Metrics()
  private val executor = Executors.newScheduledThreadPool(8)
  private val tasks = mutable.Buffer.empty[ShutdownableThread]

  private def submit(task: ShutdownableThread): Unit = {
    tasks += task
    executor.submit(task)
  }

  @AfterEach
  def cleanup(): Unit = {
    tasks.foreach(_.shutdown())
    executor.shutdownNow()
    executor.awaitTermination(5, TimeUnit.SECONDS)
    metrics.close()
  }

  @Test
  def testIsrExpandAndShrinkWithConcurrentProduce(): Unit = {
    val localId = 0
    val remoteId = 1
    val metadataCache = MetadataCache.kRaftMetadataCache(localId)
    val channel = new ControllerChannel
    val replicaManager = buildReplicaManager(localId, channel, metadataCache)

    // Start with the remote replica out of the ISR
    val initialPartitionRegistration = registration(
      replicaIds = Seq(localId, remoteId),
      isr = Seq(localId),
      leader = localId,
      LeaderRecoveryState.RECOVERED
    )

    val topicModel = new TopicModel(Uuid.randomUuid(), "foo", Map(0 -> initialPartitionRegistration))
    val topicPartition = new TopicPartition(topicModel.name, 0)
    val topicIdPartition = new TopicIdPartition(topicModel.topicId, topicPartition)
    val controller = new ControllerModel(Seq(localId, remoteId), topicModel, channel, replicaManager, metadataCache)

    submit(new Clock(time))
    replicaManager.startup()

    submit(controller)
    controller.initialize()

    waitUntilTrue(() => {
      replicaManager.getPartition(topicPartition) match {
        case HostedPartition.Online(partition) => partition.isLeader
        case _ => false
      }
    }, "Timed out waiting for partition to initialize")

    val partition = replicaManager.getPartitionOrException(topicPartition)

    // Start several producers which are actively writing to the partition
    (0 to 2).foreach { i =>
      submit(new ProducerModel(
        clientId = s"producer-$i",
        topicPartition,
        replicaManager
      ))
    }

    // Start the remote replica fetcher and wait for it to join the ISR
    val fetcher = new FetcherModel(
      clientId = s"replica-$remoteId",
      replicaId = remoteId,
      topicIdPartition,
      replicaManager
    )

    submit(fetcher)
    waitUntilTrue(() => {
      partition.inSyncReplicaIds == Set(localId, remoteId)
    }, "Test timed out before ISR was expanded")

    // Stop the fetcher so that the replica is removed from the ISR
    fetcher.shutdown()
    waitUntilTrue(() => {
      partition.inSyncReplicaIds == Set(localId)
    }, "Test timed out before ISR was shrunk")
  }

  private class Clock(
    time: MockTime
  ) extends ShutdownableThread(name = "clock", isInterruptible = false) {
    override def doWork(): Unit = {
      time.sleep(1)
    }
  }

  private def buildReplicaManager(
    localId: Int,
    channel: ControllerChannel,
    metadataCache: MetadataCache,
  ): ReplicaManager = {
    val logDir = TestUtils.tempDir()

    val props = new Properties
    props.put(KafkaConfig.QuorumVotersProp, "100@localhost:12345")
    props.put(KafkaConfig.ProcessRolesProp, "broker")
    props.put(KafkaConfig.NodeIdProp, localId.toString)
    props.put(KafkaConfig.ControllerListenerNamesProp, "SSL")
    props.put(KafkaConfig.LogDirProp, logDir.getAbsolutePath)
    props.put(KafkaConfig.ReplicaLagTimeMaxMsProp, 5000.toString)

    val config = new KafkaConfig(props, doLog = false)

    val logManager = TestUtils.createLogManager(
      defaultConfig = new LogConfig(new Properties),
      configRepository = new MockConfigRepository,
      logDirs = Seq(logDir),
      time = time
    )

    new ReplicaManager(
      metrics = metrics,
      config = config,
      time = time,
      scheduler = time.scheduler,
      logManager = logManager,
      quotaManagers = QuotaFactory.instantiate(config, metrics, time, ""),
      metadataCache = metadataCache,
      logDirFailureChannel = new LogDirFailureChannel(config.logDirs.size),
      alterPartitionManager = new MockAlterPartitionManager(channel)
    ) {
      override def createReplicaFetcherManager(
        metrics: Metrics,
        time: Time,
        threadNamePrefix: Option[String],
        quotaManager: ReplicationQuotaManager
      ): ReplicaFetcherManager = {
        Mockito.mock(classOf[ReplicaFetcherManager])
      }
    }
  }

  private class FetcherModel(
    clientId: String,
    replicaId: Int,
    topicIdPartition: TopicIdPartition,
    replicaManager: ReplicaManager
  ) extends ShutdownableThread(name = clientId, isInterruptible = false) {
    private val random = new Random()

    private val clientMetadata = new DefaultClientMetadata(
      "",
      clientId,
      InetAddress.getLocalHost,
      KafkaPrincipal.ANONYMOUS,
      "PLAINTEXT"
    )

    private var fetchOffset = 0L

    override def doWork(): Unit = {
      val partitionData = new FetchRequest.PartitionData(
        topicIdPartition.topicId,
        fetchOffset,
        -1,
        65536,
        Optional.empty(),
        Optional.empty()
      )

      val future = new CompletableFuture[FetchPartitionData]()
      def fetchCallback(results: collection.Seq[(TopicIdPartition, FetchPartitionData)]): Unit = {
        try {
          assertEquals(1, results.size)
          val (topicIdPartition, result) = results.head
          assertEquals(this.topicIdPartition, topicIdPartition)
          assertEquals(Errors.NONE, result.error)
          future.complete(result)
        } catch {
          case e: Throwable => future.completeExceptionally(e)
        }
      }

      val fetchParams = FetchParams(
        requestVersion = ApiKeys.FETCH.latestVersion,
        replicaId = replicaId,
        maxWaitMs = random.nextInt(100),
        minBytes = 1,
        maxBytes = 1024 * 1024,
        isolation = FetchIsolation(replicaId, IsolationLevel.READ_UNCOMMITTED),
        clientMetadata = Some(clientMetadata)
      )

      replicaManager.fetchMessages(
        params = fetchParams,
        fetchInfos = Seq(topicIdPartition -> partitionData),
        quota = QuotaFactory.UnboundedQuota,
        responseCallback = fetchCallback,
      )

      val fetchResult = future.get()
      fetchResult.records.batches.forEach { batch =>
        fetchOffset = batch.lastOffset + 1
      }
    }
  }

  private class ProducerModel(
    clientId: String,
    topicPartition: TopicPartition,
    replicaManager: ReplicaManager
  ) extends ShutdownableThread(name = clientId, isInterruptible = false) {
    private val random = new Random()
    private var sequence = 0

    override def doWork(): Unit = {
      val numRecords = (random.nextInt() % 10) + 1

      val records = (0 until numRecords).map { i =>
        new SimpleRecord(s"$clientId-${sequence + i}".getBytes)
      }

      val future = new CompletableFuture[ProduceResponse.PartitionResponse]()
      def produceCallback(results: collection.Map[TopicPartition, ProduceResponse.PartitionResponse]): Unit = {
        try {
          assertEquals(1, results.size)
          val (topicPartition, result) = results.head
          assertEquals(this.topicPartition, topicPartition)
          assertEquals(Errors.NONE, result.error)
          future.complete(result)
        } catch {
          case e: Throwable => future.completeExceptionally(e)
        }
      }

      replicaManager.appendRecords(
        timeout = 30000,
        requiredAcks = (-1).toShort,
        internalTopicsAllowed = false,
        origin = AppendOrigin.CLIENT,
        entriesPerPartition = collection.Map(topicPartition -> TestUtils.records(records)),
        responseCallback = produceCallback
      )

      future.get()
      sequence += numRecords
    }
  }

  sealed trait ControllerEvent
  case object InitializeEvent extends ControllerEvent
  case object ShutdownEvent extends ControllerEvent
  case class AlterIsrEvent(
    future: CompletableFuture[LeaderAndIsr],
    topicPartition: TopicIdPartition,
    leaderAndIsr: LeaderAndIsr
  ) extends ControllerEvent

  private class ControllerChannel {
    private val eventQueue = new LinkedBlockingQueue[ControllerEvent]()

    def poll(): ControllerEvent = {
      eventQueue.take()
    }

    def alterIsr(
      topicPartition: TopicIdPartition,
      leaderAndIsr: LeaderAndIsr
    ): CompletableFuture[LeaderAndIsr] = {
      val future = new CompletableFuture[LeaderAndIsr]()
      eventQueue.offer(AlterIsrEvent(future, topicPartition, leaderAndIsr))
      future
    }

    def initialize(): Unit = {
      eventQueue.offer(InitializeEvent)
    }

    def shutdown(): Unit = {
      eventQueue.offer(ShutdownEvent)
    }
  }

  private class ControllerModel(
    brokerIds: Seq[Int],
    topic: TopicModel,
    channel: ControllerChannel,
    replicaManager: ReplicaManager,
    metadataCache: KRaftMetadataCache
  ) extends ShutdownableThread(name = "controller", isInterruptible = false) {
    private var latestImage = MetadataImage.EMPTY

    def initialize(): Unit = {
      channel.initialize()
    }

    override def shutdown(): Unit = {
      super.initiateShutdown()
      channel.shutdown()
      super.awaitShutdown()
    }

    override def doWork(): Unit = {
      channel.poll() match {
        case InitializeEvent =>
          val delta = new MetadataDelta.Builder().setImage(latestImage).build()
          brokerIds.foreach { brokerId =>
            delta.replay(new RegisterBrokerRecord()
              .setBrokerId(brokerId)
              .setFenced(false)
            )
          }
          topic.initialize(delta)
          latestImage = delta.apply(latestImage.provenance())
          metadataCache.setImage(latestImage)
          replicaManager.applyDelta(delta.topicsDelta, latestImage)

        case AlterIsrEvent(future, topicPartition, leaderAndIsr) =>
          val delta = new MetadataDelta.Builder().setImage(latestImage).build()
          val updatedLeaderAndIsr = topic.alterIsr(topicPartition, leaderAndIsr, delta)
          latestImage = delta.apply(latestImage.provenance())
          future.complete(updatedLeaderAndIsr)
          replicaManager.applyDelta(delta.topicsDelta, latestImage)

        case ShutdownEvent =>
      }
    }
  }

  private class TopicModel(
    val topicId: Uuid,
    val name: String,
    initialRegistrations: Map[Int, PartitionRegistration]
  ) {
    private val partitions: Map[Int, PartitionModel] = initialRegistrations.map {
      case (partitionId, registration) =>
        partitionId -> new PartitionModel(this, partitionId, registration)
    }

    def initialize(delta: MetadataDelta): Unit = {
      delta.replay(new TopicRecord()
        .setName(name)
        .setTopicId(topicId)
      )
      partitions.values.foreach(_.initialize(delta))
    }

    def alterIsr(
      topicPartition: TopicIdPartition,
      leaderAndIsr: LeaderAndIsr,
      delta: MetadataDelta
    ): LeaderAndIsr = {
      val partitionModel = partitions.getOrElse(topicPartition.partition,
        throw new IllegalStateException(s"Unexpected partition $topicPartition")
      )
      partitionModel.alterIsr(leaderAndIsr, delta)
    }
  }

  private class PartitionModel(
    val topic: TopicModel,
    val partitionId: Int,
    var registration: PartitionRegistration
  ) {
    def alterIsr(
      leaderAndIsr: LeaderAndIsr,
      delta: MetadataDelta
    ): LeaderAndIsr = {
      delta.replay(new PartitionChangeRecord()
        .setTopicId(topic.topicId)
        .setPartitionId(partitionId)
        .setIsr(leaderAndIsr.isr.map(Int.box).asJava)
        .setLeader(leaderAndIsr.leader)
      )
      this.registration = delta.topicsDelta
        .changedTopic(topic.topicId)
        .partitionChanges
        .get(partitionId)

      leaderAndIsr.withPartitionEpoch(registration.partitionEpoch)
    }

    private def toList(ints: Array[Int]): util.List[Integer] = {
      ints.map(Int.box).toList.asJava
    }

    def initialize(delta: MetadataDelta): Unit = {
      delta.replay(new PartitionRecord()
        .setTopicId(topic.topicId)
        .setPartitionId(partitionId)
        .setReplicas(toList(registration.replicas))
        .setIsr(toList(registration.isr))
        .setLeader(registration.leader)
        .setLeaderEpoch(registration.leaderEpoch)
        .setPartitionEpoch(registration.partitionEpoch)
      )
    }
  }

  private class MockAlterPartitionManager(channel: ControllerChannel) extends AlterPartitionManager {
    override def submit(
      topicPartition: TopicIdPartition,
      leaderAndIsr: LeaderAndIsr,
      controllerEpoch: Int
    ): CompletableFuture[LeaderAndIsr] = {
      channel.alterIsr(topicPartition, leaderAndIsr)
    }
  }

  private def registration(
    replicaIds: Seq[Int],
    isr: Seq[Int],
    leader: Int,
    leaderRecoveryState: LeaderRecoveryState,
    leaderEpoch: Int = 0,
    partitionEpoch: Int = 0
  ): PartitionRegistration = {
    new PartitionRegistration(
      replicaIds.toArray,
      isr.toArray,
      Array.empty[Int],
      Array.empty[Int],
      leader,
      leaderRecoveryState,
      leaderEpoch,
      partitionEpoch
    )
  }

}
