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

import java.util.concurrent.{ConcurrentHashMap, Executors}
import java.util.{Collections, Random}
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.Lock

import kafka.coordinator.AbstractCoordinatorConcurrencyTest._
import kafka.log.{AppendOrigin, Log}
import kafka.server._
import kafka.utils._
import kafka.utils.timer.MockTimer
import kafka.zk.KafkaZkClient
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.{MemoryRecords, RecordBatch, RecordConversionStats}
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.easymock.EasyMock
import org.junit.jupiter.api.{AfterEach, BeforeEach}

import scala.collection._
import scala.jdk.CollectionConverters._

abstract class AbstractCoordinatorConcurrencyTest[M <: CoordinatorMember] {

  val nThreads = 5

  val time = new MockTime
  val timer = new MockTimer
  val executor = Executors.newFixedThreadPool(nThreads)
  val scheduler = new MockScheduler(time)
  var replicaManager: TestReplicaManager = _
  var zkClient: KafkaZkClient = _
  val serverProps = TestUtils.createBrokerConfig(nodeId = 0, zkConnect = "")
  val random = new Random

  @BeforeEach
  def setUp(): Unit = {

    replicaManager = EasyMock.partialMockBuilder(classOf[TestReplicaManager]).createMock()
    replicaManager.createDelayedProducePurgatory(timer)

    zkClient = EasyMock.createNiceMock(classOf[KafkaZkClient])
  }

  @AfterEach
  def tearDown(): Unit = {
    EasyMock.reset(replicaManager)
    if (executor != null)
      executor.shutdownNow()
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
    EasyMock.reset(replicaManager)
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

  class TestReplicaManager extends ReplicaManager(
    null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, None, null, null) {

    var producePurgatory: DelayedOperationPurgatory[DelayedProduce] = _
    var watchKeys: mutable.Set[TopicPartitionOperationKey] = _
    def createDelayedProducePurgatory(timer: MockTimer): Unit = {
      producePurgatory = new DelayedOperationPurgatory[DelayedProduce]("Produce", timer, 1, reaperEnabled = false)
      watchKeys = Collections.newSetFromMap(new ConcurrentHashMap[TopicPartitionOperationKey, java.lang.Boolean]()).asScala
    }

    override def tryCompleteActions(): Unit = watchKeys.map(producePurgatory.checkAndComplete)

    override def appendRecords(timeout: Long,
                               requiredAcks: Short,
                               internalTopicsAllowed: Boolean,
                               origin: AppendOrigin,
                               entriesPerPartition: Map[TopicPartition, MemoryRecords],
                               responseCallback: Map[TopicPartition, PartitionResponse] => Unit,
                               delayedProduceLock: Option[Lock] = None,
                               processingStatsCallback: Map[TopicPartition, RecordConversionStats] => Unit = _ => ()): Unit = {

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
      val producerRequestKeys = entriesPerPartition.keys.map(TopicPartitionOperationKey(_)).toSeq
      watchKeys ++= producerRequestKeys
      producePurgatory.tryCompleteElseWatch(delayedProduce, producerRequestKeys)
    }
    override def getMagic(topicPartition: TopicPartition): Option[Byte] = {
      Some(RecordBatch.MAGIC_VALUE_V2)
    }
    @volatile var logs: mutable.Map[TopicPartition, (Log, Long)] = _
    def getOrCreateLogs(): mutable.Map[TopicPartition, (Log, Long)] = {
      if (logs == null)
        logs = mutable.Map[TopicPartition, (Log, Long)]()
      logs
    }
    def updateLog(topicPartition: TopicPartition, log: Log, endOffset: Long): Unit = {
      getOrCreateLogs().put(topicPartition, (log, endOffset))
    }
    override def getLog(topicPartition: TopicPartition): Option[Log] =
      getOrCreateLogs().get(topicPartition).map(l => l._1)
    override def getLogEndOffset(topicPartition: TopicPartition): Option[Long] =
      getOrCreateLogs().get(topicPartition).map(l => l._2)
  }
}
