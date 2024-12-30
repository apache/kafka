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

package kafka.coordinator

import java.util.concurrent.{ConcurrentHashMap, ExecutorService, Executors}
import java.util.{Collections, Random}
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.Lock
import kafka.coordinator.AbstractCoordinatorConcurrencyTest._
import kafka.cluster.Partition
import kafka.log.{LogManager, UnifiedLog}
import kafka.server.QuotaFactory.QuotaManagers
import kafka.server.{KafkaConfig, _}
import kafka.utils._
import kafka.zk.KafkaZkClient
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.{MemoryRecords, RecordBatch, RecordValidationStats}
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.utils.{Time, Utils}
import org.apache.kafka.server.ActionQueue
import org.apache.kafka.server.common.RequestLocal
import org.apache.kafka.server.purgatory.{DelayedOperationPurgatory, TopicPartitionOperationKey}
import org.apache.kafka.server.util.timer.{MockTimer, Timer}
import org.apache.kafka.server.util.{MockScheduler, MockTime, Scheduler}
import org.apache.kafka.storage.internals.log.{AppendOrigin, LogConfig, VerificationGuard}
import org.junit.jupiter.api.{AfterEach, BeforeEach}
import org.mockito.Mockito.{mock, when, withSettings}

import scala.collection._
import scala.jdk.CollectionConverters._

abstract class AbstractCoordinatorConcurrencyTest[M <: CoordinatorMember] extends Logging {
  val nThreads = 5
  val serverProps = TestUtils.createBrokerConfig(nodeId = 0, zkConnect = "")
  val random = new Random
  var replicaManager: TestReplicaManager = _
  var zkClient: KafkaZkClient = _
  var time: MockTime = _
  var timer: MockTimer = _
  var executor: ExecutorService = _
  var scheduler: MockScheduler = _

  @BeforeEach
  def setUp(): Unit = {
    time = new MockTime
    timer = new MockTimer(time)
    scheduler = new MockScheduler(time)
    executor = Executors.newFixedThreadPool(nThreads)
    val mockLogMger = mock(classOf[LogManager])
    when(mockLogMger.liveLogDirs).thenReturn(Seq.empty)
    val producePurgatory = new DelayedOperationPurgatory[DelayedProduce]("Produce", timer, 1, 1000, false, true)
    val watchKeys = Collections.newSetFromMap(new ConcurrentHashMap[TopicPartitionOperationKey, java.lang.Boolean]()).asScala
    replicaManager = TestReplicaManager(KafkaConfig.fromProps(serverProps), time, scheduler, timer, mockLogMger, mock(classOf[QuotaManagers], withSettings().stubOnly()), producePurgatory, watchKeys)
    zkClient = mock(classOf[KafkaZkClient], withSettings().stubOnly())
  }

  @AfterEach
  def tearDown(): Unit = {
    CoreUtils.swallow(replicaManager.shutdown(false), this)
    CoreUtils.swallow(executor.shutdownNow(), this)
    Utils.closeQuietly(timer, "mock timer")
    CoreUtils.swallow(scheduler.shutdown(), this)
    CoreUtils.swallow(time.scheduler.shutdown(), this)
  }

  /**
    * Verify that concurrent operations run in the normal sequence produce the expected results.
    */
  def verifyConcurrentOperations(createMembers: String => Set[M], operations: Seq[Operation]): Unit = {
    OrderedOperationSequence(createMembers("verifyConcurrentOperations"), operations).run()
  }

  /**
    * Verify that arbitrary operations run in some random sequence don't leave the coordinator
    * in a bad state. Operations in the normal sequence should continue to work as expected.
    */
  def verifyConcurrentRandomSequences(createMembers: String => Set[M], operations: Seq[Operation]): Unit = {
    for (i <- 0 to 10) {
      // Run some random operations
      RandomOperationSequence(createMembers(s"random$i"), operations).run()

      // Check that proper sequences still work correctly
      OrderedOperationSequence(createMembers(s"ordered$i"), operations).run()
    }
  }

  def verifyConcurrentActions(actions: Set[Action]): Unit = {
    val futures = actions.map(executor.submit)
    futures.map(_.get)
    enableCompletion()
    actions.foreach(_.await())
  }

  def enableCompletion(): Unit = {
    replicaManager.tryCompleteActions()
    scheduler.tick()
  }

  abstract class OperationSequence(members: Set[M], operations: Seq[Operation]) {
    def actionSequence: Seq[Set[Action]]
    def run(): Unit = {
      actionSequence.foreach(verifyConcurrentActions)
    }
  }

  case class OrderedOperationSequence(members: Set[M], operations: Seq[Operation])
    extends OperationSequence(members, operations) {
    override def actionSequence: Seq[Set[Action]] = {
      operations.map { op =>
        members.map(op.actionWithVerify)
      }
    }
  }

  case class RandomOperationSequence(members: Set[M], operations: Seq[Operation])
    extends OperationSequence(members, operations) {
    val opCount = operations.length
    def actionSequence: Seq[Set[Action]] = {
      (0 to opCount).map { _ =>
        members.map { member =>
          val op = operations(random.nextInt(opCount))
          op.actionNoVerify(member) // Don't wait or verify since these operations may block
        }
      }
    }
  }

  abstract class Operation {
    def run(member: M): Unit
    def awaitAndVerify(member: M): Unit
    def actionWithVerify(member: M): Action = {
      new Action() {
        def run(): Unit = Operation.this.run(member)
        def await(): Unit = awaitAndVerify(member)
      }
    }
    def actionNoVerify(member: M): Action = {
      new Action() {
        def run(): Unit = Operation.this.run(member)
        def await(): Unit = timer.advanceClock(100) // Don't wait since operation may block
      }
    }
  }
}

object AbstractCoordinatorConcurrencyTest {
  trait Action extends Runnable {
    def await(): Unit
  }

  trait CoordinatorMember {
  }

  class TestReplicaManager(config: KafkaConfig,
                           mtime: Time,
                           scheduler: Scheduler,
                           logManager: LogManager,
                           quotaManagers: QuotaManagers,
                           val watchKeys: mutable.Set[TopicPartitionOperationKey],
                           val producePurgatory: DelayedOperationPurgatory[DelayedProduce],
                           val delayedFetchPurgatoryParam: DelayedOperationPurgatory[DelayedFetch],
                           val delayedDeleteRecordsPurgatoryParam: DelayedOperationPurgatory[DelayedDeleteRecords],
                           val delayedElectLeaderPurgatoryParam: DelayedOperationPurgatory[DelayedElectLeader],
                           val delayedRemoteFetchPurgatoryParam: DelayedOperationPurgatory[DelayedRemoteFetch],
                           val delayedRemoteListOffsetsPurgatoryParam: DelayedOperationPurgatory[DelayedRemoteListOffsets])
    extends ReplicaManager(
      config,
      metrics = null,
      mtime,
      scheduler,
      logManager,
      None,
      quotaManagers,
      null,
      null,
      null,
      delayedProducePurgatoryParam = Some(producePurgatory),
      delayedFetchPurgatoryParam = Some(delayedFetchPurgatoryParam),
      delayedDeleteRecordsPurgatoryParam = Some(delayedDeleteRecordsPurgatoryParam),
      delayedElectLeaderPurgatoryParam = Some(delayedElectLeaderPurgatoryParam),
      delayedRemoteFetchPurgatoryParam = Some(delayedRemoteFetchPurgatoryParam),
      delayedRemoteListOffsetsPurgatoryParam = Some(delayedRemoteListOffsetsPurgatoryParam),
      threadNamePrefix = Option(this.getClass.getName)) {

    @volatile var logs: mutable.Map[TopicPartition, (UnifiedLog, Long)] = _

    override def maybeStartTransactionVerificationForPartition(
      topicPartition: TopicPartition,
      transactionalId: String,
      producerId: Long,
      producerEpoch: Short,
      baseSequence: Int,
      callback: ((Errors, VerificationGuard)) => Unit,
      transactionSupportedOperation: TransactionSupportedOperation
    ): Unit = {
      // Skip verification
      callback((Errors.NONE, VerificationGuard.SENTINEL))
    }

    override def tryCompleteActions(): Unit = watchKeys.map(producePurgatory.checkAndComplete)

    override def appendRecords(timeout: Long,
                               requiredAcks: Short,
                               internalTopicsAllowed: Boolean,
                               origin: AppendOrigin,
                               entriesPerPartition: Map[TopicPartition, MemoryRecords],
                               responseCallback: Map[TopicPartition, PartitionResponse] => Unit,
                               delayedProduceLock: Option[Lock] = None,
                               processingStatsCallback: Map[TopicPartition, RecordValidationStats] => Unit = _ => (),
                               requestLocal: RequestLocal = RequestLocal.noCaching,
                               actionQueue: ActionQueue = null,
                               verificationGuards: Map[TopicPartition, VerificationGuard] = Map.empty): Unit = {

      if (entriesPerPartition.isEmpty)
        return
      val produceMetadata = ProduceMetadata(1, entriesPerPartition.map {
        case (tp, _) =>
          (tp, ProducePartitionStatus(0L, new PartitionResponse(Errors.NONE, 0L, RecordBatch.NO_TIMESTAMP, 0L)))
      })
      val delayedProduce = new DelayedProduce(5, produceMetadata, this, responseCallback, delayedProduceLock) {
        // Complete produce requests after a few attempts to trigger delayed produce from different threads
        val completeAttempts = new AtomicInteger
        override def tryComplete(): Boolean = {
          if (completeAttempts.incrementAndGet() >= 3)
            forceComplete()
          else
            false
        }
        override def onComplete(): Unit = {
          responseCallback(entriesPerPartition.map {
            case (tp, _) =>
              (tp, new PartitionResponse(Errors.NONE, 0L, RecordBatch.NO_TIMESTAMP, 0L))
          })
        }
      }
      val producerRequestKeys = entriesPerPartition.keys.map(new TopicPartitionOperationKey(_))
      watchKeys ++= producerRequestKeys
      producePurgatory.tryCompleteElseWatch(delayedProduce, producerRequestKeys.toList.asJava)
    }

    override def onlinePartition(topicPartition: TopicPartition): Option[Partition] = {
      Some(mock(classOf[Partition]))
    }

    def getOrCreateLogs(): mutable.Map[TopicPartition, (UnifiedLog, Long)] = {
      if (logs == null)
        logs = mutable.Map[TopicPartition, (UnifiedLog, Long)]()
      logs
    }

    def updateLog(topicPartition: TopicPartition, log: UnifiedLog, endOffset: Long): Unit = {
      getOrCreateLogs().put(topicPartition, (log, endOffset))
    }

    override def getLogConfig(topicPartition: TopicPartition): Option[LogConfig] = {
      getOrCreateLogs().get(topicPartition).map(_._1.config)
    }

    override def getLog(topicPartition: TopicPartition): Option[UnifiedLog] =
      getOrCreateLogs().get(topicPartition).map(l => l._1)

    override def getLogEndOffset(topicPartition: TopicPartition): Option[Long] =
      getOrCreateLogs().get(topicPartition).map(l => l._2)
  }

  object TestReplicaManager {
    def apply(config: KafkaConfig,
              time: Time,
              scheduler: Scheduler,
              timer: Timer,
              logManager: LogManager,
              quotaManagers: QuotaManagers,
              producePurgatory: DelayedOperationPurgatory[DelayedProduce],
              watchKeys: mutable.Set[TopicPartitionOperationKey]): TestReplicaManager = {
      val mockRemoteFetchPurgatory = new DelayedOperationPurgatory[DelayedRemoteFetch](
        "RemoteFetch", timer, 0, 1000, false, true)
      val mockRemoteListOffsetsPurgatory = new DelayedOperationPurgatory[DelayedRemoteListOffsets](
        "RemoteListOffsets", timer, 0, 1000, false, true)
      val mockFetchPurgatory = new DelayedOperationPurgatory[DelayedFetch](
        "Fetch", timer, 0, 1000, false, true)
      val mockDeleteRecordsPurgatory = new DelayedOperationPurgatory[DelayedDeleteRecords](
        "DeleteRecords", timer, 0, 1000, false, true)
      val mockElectLeaderPurgatory = new DelayedOperationPurgatory[DelayedElectLeader](
        "ElectLeader", timer, 0, 1000, false, true)
      new TestReplicaManager(config, time, scheduler, logManager, quotaManagers, watchKeys, producePurgatory,
        mockFetchPurgatory, mockDeleteRecordsPurgatory, mockElectLeaderPurgatory, mockRemoteFetchPurgatory,
        mockRemoteListOffsetsPurgatory)
    }
  }
}
