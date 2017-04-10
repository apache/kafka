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
package kafka.coordinator.transaction

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{EndTxnRequest, TransactionResult}
import org.apache.kafka.common.utils.MockTime
import org.easymock.{Capture, EasyMock, IAnswer}
import org.junit.Assert._
import org.junit.{After, Before, Test}

class TransactionCoordinatorTest {

  val time = new MockTime()

  var nextPid: Long = 0L
  val pidManager: ProducerIdManager = EasyMock.createNiceMock(classOf[ProducerIdManager])
  val transactionManager: TransactionStateManager = EasyMock.createNiceMock(classOf[TransactionStateManager])
  val transactionMarkerChannelManager: TransactionMarkerChannelManager = EasyMock.createNiceMock(classOf[TransactionMarkerChannelManager])
  val capturedTxn: Capture[TransactionMetadata] = EasyMock.newCapture()
  val capturedArgument: Capture[Errors => Unit] = EasyMock.newCapture()


  private def setupMocks() {
    EasyMock.expect(pidManager.nextPid())
      .andAnswer(new IAnswer[Long] {
        override def answer(): Long = {
          nextPid += 1
          nextPid - 1
        }
      })
      .anyTimes()


    EasyMock.expect(transactionManager.isCoordinatorFor(EasyMock.eq("a")))
      .andReturn(true)
      .anyTimes()
    EasyMock.expect(transactionManager.isCoordinatorFor(EasyMock.eq("b")))
      .andReturn(false)
      .anyTimes()
    EasyMock.expect(transactionManager.isCoordinatorFor(EasyMock.eq("c")))
      .andReturn(true)
      .anyTimes()
    EasyMock.expect(transactionManager.isCoordinatorLoadingInProgress(EasyMock.anyString()))
      .andReturn(false)
      .anyTimes()
    EasyMock.expect(transactionManager.validateTransactionTimeoutMs(EasyMock.anyInt()))
      .andReturn(true)
      .anyTimes()


    EasyMock.expect(transactionManager.addTransaction(EasyMock.eq("a"), EasyMock.capture(capturedTxn)))
      .andAnswer(new IAnswer[TransactionMetadata] {
        override def answer(): TransactionMetadata = {
          capturedTxn.getValue
        }
      })
      .once()
    EasyMock.expect(transactionManager.getTransaction(EasyMock.eq("a")))
      .andAnswer(new IAnswer[Option[TransactionMetadata]] {
        override def answer(): Option[TransactionMetadata] = {
          if (capturedTxn.hasCaptured) {
            Some(capturedTxn.getValue)
          } else {
            None
          }
        }
      })
      .anyTimes()
    EasyMock.expect(transactionManager.getTransaction(EasyMock.eq("c")))
      .andAnswer(new IAnswer[Option[TransactionMetadata]] {
        override def answer(): Option[TransactionMetadata] = {
          None
        }
      })
      .anyTimes()
    EasyMock.expect(transactionManager.appendTransactionToLog(
      EasyMock.eq("a"),
      EasyMock.capture(capturedTxn),
      EasyMock.capture(capturedArgument),
      EasyMock.anyObject()))
      .andAnswer(new IAnswer[Unit] {
        override def answer(): Unit = {
          // do nothing
        }
      })
      .anyTimes()
    EasyMock.replay(pidManager, transactionManager)
  }


  val brokerId = 0

  val coordinator: TransactionCoordinator = new TransactionCoordinator(brokerId,
    pidManager,
    transactionManager,
    transactionMarkerChannelManager,
    time)

  var result: InitPidResult = _
  var error: Errors = Errors.NONE


  @Test
  def testHandleInitPid() = {
    setupMocks()

    val transactionTimeoutMs = 1000

    coordinator.handleInitPid("", transactionTimeoutMs, initPidMockCallback)
    assertEquals(InitPidResult(0L, 0, Errors.NONE), result)

    coordinator.handleInitPid(null, transactionTimeoutMs, initPidMockCallback)
    assertEquals(InitPidResult(1L, 0, Errors.NONE), result)

    coordinator.handleInitPid("a", transactionTimeoutMs, initPidMockCallback)
    assertEquals(InitPidResult(2L, 0, Errors.NONE), result)

    coordinator.handleInitPid("a", transactionTimeoutMs, initPidMockCallback)
    assertEquals(InitPidResult(2L, 1, Errors.NONE), result)

    coordinator.handleInitPid("b", transactionTimeoutMs, initPidMockCallback)
    assertEquals(InitPidResult(-1L, -1, Errors.NOT_COORDINATOR), result)
  }

  @Test
  def testHandleAddPartitionsToTxn() = {
    setupMocks()
    val transactionTimeoutMs = 1000

    coordinator.handleInitPid("a", transactionTimeoutMs, initPidMockCallback)
    coordinator.handleInitPid("a", transactionTimeoutMs, initPidMockCallback)
    assertEquals(InitPidResult(0L, 1, Errors.NONE), result)

    coordinator.handleAddPartitionsToTransaction("a", 0L, 1, Set[TopicPartition](new TopicPartition("topic1", 0)), errorsCallback)
    assertEquals(0L, capturedTxn.getValue.pid)
    assertEquals(1, capturedTxn.getValue.epoch)
    assertEquals(Ongoing, capturedTxn.getValue.state)
    assertEquals(None, capturedTxn.getValue.pendingState)
    assertEquals(transactionTimeoutMs, capturedTxn.getValue.txnTimeoutMs)
    assertEquals(Set[TopicPartition](new TopicPartition("topic1", 0)), capturedTxn.getValue.topicPartitions)

    assertEquals(Errors.NONE, error)

    // testing error cases
    coordinator.handleAddPartitionsToTransaction("", 0L, 1, Set[TopicPartition](new TopicPartition("topic1", 0)), errorsCallback)
    assertEquals(Errors.INVALID_REQUEST, error)

    coordinator.handleAddPartitionsToTransaction("b", 0L, 1, Set[TopicPartition](new TopicPartition("topic1", 0)), errorsCallback)
    assertEquals(Errors.NOT_COORDINATOR, error)

    coordinator.handleAddPartitionsToTransaction("c", 0L, 1, Set[TopicPartition](new TopicPartition("topic1", 0)), errorsCallback)
    assertEquals(Errors.INVALID_PID_MAPPING, error)

    coordinator.handleAddPartitionsToTransaction("a", 1L, 1, Set[TopicPartition](new TopicPartition("topic1", 0)), errorsCallback)
    assertEquals(Errors.INVALID_PID_MAPPING, error)

    coordinator.handleAddPartitionsToTransaction("a", 0L, 0, Set[TopicPartition](new TopicPartition("topic1", 0)), errorsCallback)
    assertEquals(Errors.PRODUCER_FENCED, error)

    capturedTxn.getValue.state = PrepareCommit
    coordinator.handleAddPartitionsToTransaction("a", 0L, 1, Set[TopicPartition](new TopicPartition("topic1", 0)), errorsCallback)
    assertEquals(Errors.INVALID_TXN_STATE, error)

    capturedTxn.getValue.state = Ongoing
    capturedTxn.getValue.pendingState = Some(PrepareCommit)
    coordinator.handleAddPartitionsToTransaction("a", 0L, 1, Set[TopicPartition](new TopicPartition("topic1", 0)), errorsCallback)
    assertEquals(Errors.COORDINATOR_LOAD_IN_PROGRESS, error)
  }

  @Test
  def shouldReplyWithInvalidPidMappingOnEndTxnWhenTxnIdDoesntExist(): Unit = {
    val transactionId = "unknown"
    EasyMock.expect(transactionManager.isCoordinatorFor(transactionId))
      .andReturn(true)
    EasyMock.expect(transactionManager.getTransaction(transactionId)).andReturn(None)
    EasyMock.replay(transactionManager)

    coordinator.handleEndTransaction(transactionId, 0, 0, TransactionResult.COMMIT, errorsCallback)
    assertEquals(Errors.INVALID_PID_MAPPING, error)
    EasyMock.verify(transactionManager)
  }

  @Test
  def shouldReplyWithInvalidPidMappingOnEndTxnWhenPidDosentMatchMapped(): Unit = {
    val transactionId = "known"
    EasyMock.expect(transactionManager.isCoordinatorFor(transactionId))
      .andReturn(true)
    EasyMock.expect(transactionManager.getTransaction(transactionId))
      .andReturn(Some(new TransactionMetadata(10, 0, 0, Ongoing, collection.mutable.Set.empty[TopicPartition], time.milliseconds())))
    EasyMock.replay(transactionManager)

    coordinator.handleEndTransaction(transactionId, 0, 0, TransactionResult.COMMIT, errorsCallback)
    assertEquals(Errors.INVALID_PID_MAPPING, error)
    EasyMock.verify(transactionManager)
  }

  @Test
  def shouldReplyWithProducerFencedOnEndTxnWhenEpochIsNotSameAsTransaction(): Unit = {
    val transactionId = "known"
    val pid = 10
    EasyMock.expect(transactionManager.isCoordinatorFor(transactionId))
      .andReturn(true)
    EasyMock.expect(transactionManager.getTransaction(transactionId))
      .andReturn(Some(new TransactionMetadata(pid, 1, 1, Ongoing, collection.mutable.Set.empty[TopicPartition], time.milliseconds())))
    EasyMock.replay(transactionManager)

    coordinator.handleEndTransaction(transactionId, pid, 0, TransactionResult.COMMIT, errorsCallback)
    assertEquals(Errors.PRODUCER_FENCED, error)
    EasyMock.verify(transactionManager)
  }

  @Test
  def shouldReturnOkOnEndTxnWhenStatusIsCompleteCommitAndResultIsCommit(): Unit ={
    val transactionId = "known"
    val pid = 10
    EasyMock.expect(transactionManager.isCoordinatorFor(transactionId))
      .andReturn(true)
    EasyMock.expect(transactionManager.getTransaction(transactionId))
      .andReturn(Some(new TransactionMetadata(pid, 1, 1, CompleteCommit, collection.mutable.Set.empty[TopicPartition], time.milliseconds())))
    EasyMock.replay(transactionManager)

    coordinator.handleEndTransaction(transactionId, pid, 1, TransactionResult.COMMIT, errorsCallback)
    assertEquals(Errors.NONE, error)
    EasyMock.verify(transactionManager)
  }

  @Test
  def shouldReturnOkOnEndTxnWhenStatusIsCompleteAbortAndResultIsAbort(): Unit ={
    val transactionId = "known"
    val pid = 10
    EasyMock.expect(transactionManager.isCoordinatorFor(transactionId))
      .andReturn(true)
    EasyMock.expect(transactionManager.getTransaction(transactionId))
      .andReturn(Some(new TransactionMetadata(pid, 1, 1, CompleteAbort, collection.mutable.Set.empty[TopicPartition], time.milliseconds())))
    EasyMock.replay(transactionManager)

    coordinator.handleEndTransaction(transactionId, pid, 1, TransactionResult.ABORT, errorsCallback)
    assertEquals(Errors.NONE, error)
    EasyMock.verify(transactionManager)
  }

  @Test
  def shouldReturnInvalidTxnRequestOnEndTxnRequestWhenStatusIsCompleteAbortAndResultIsNotAbort(): Unit = {
    val transactionId = "known"
    val pid = 10
    EasyMock.expect(transactionManager.isCoordinatorFor(transactionId))
      .andReturn(true)
    EasyMock.expect(transactionManager.getTransaction(transactionId))
      .andReturn(Some(new TransactionMetadata(pid, 1, 1, CompleteAbort, collection.mutable.Set.empty[TopicPartition], time.milliseconds())))
    EasyMock.replay(transactionManager)

    coordinator.handleEndTransaction(transactionId, pid, 1, TransactionResult.COMMIT, errorsCallback)
    assertEquals(Errors.INVALID_TXN_STATE, error)
    EasyMock.verify(transactionManager)
  }

  @Test
  def shouldReturnInvalidTxnRequestOnEndTxnRequestWhenStatusIsCompleteCommitAndResultIsNotCommit(): Unit = {
    val transactionId = "known"
    val pid = 10
    EasyMock.expect(transactionManager.isCoordinatorFor(transactionId))
      .andReturn(true)
    EasyMock.expect(transactionManager.getTransaction(transactionId))
      .andReturn(Some(new TransactionMetadata(pid, 1, 1, CompleteCommit, collection.mutable.Set.empty[TopicPartition], time.milliseconds())))
    EasyMock.replay(transactionManager)

    coordinator.handleEndTransaction(transactionId, pid, 1, TransactionResult.ABORT, errorsCallback)
    assertEquals(Errors.INVALID_TXN_STATE, error)
    EasyMock.verify(transactionManager)
  }

  @Test
  def shouldReturnInvalidTxnRequestOnEndTxnRequestWhenStatusIsPrepareCommit(): Unit = {
    val transactionId = "known"
    val pid = 10
    EasyMock.expect(transactionManager.isCoordinatorFor(transactionId))
      .andReturn(true)
    EasyMock.expect(transactionManager.getTransaction(transactionId))
      .andReturn(Some(new TransactionMetadata(pid, 1, 1, PrepareCommit, collection.mutable.Set.empty[TopicPartition], time.milliseconds())))
    EasyMock.replay(transactionManager)

    coordinator.handleEndTransaction(transactionId, pid, 1, TransactionResult.COMMIT, errorsCallback)
    assertEquals(Errors.INVALID_TXN_STATE, error)
    EasyMock.verify(transactionManager)
  }

  @Test
  def shouldReturnInvalidTxnRequestOnEndTxnRequestWhenStatusIsPrepareAbort(): Unit = {
    val transactionId = "known"
    val pid = 10
    EasyMock.expect(transactionManager.isCoordinatorFor(transactionId))
      .andReturn(true)
    EasyMock.expect(transactionManager.getTransaction(transactionId))
      .andReturn(Some(new TransactionMetadata(pid, 1, 1, PrepareAbort, collection.mutable.Set.empty[TopicPartition], time.milliseconds())))
    EasyMock.replay(transactionManager)

    coordinator.handleEndTransaction(transactionId, pid, 1, TransactionResult.COMMIT, errorsCallback)
    assertEquals(Errors.INVALID_TXN_STATE, error)
    EasyMock.verify(transactionManager)
  }


  @Test
  def shouldAppendPrepareCommitToLogOnEndTxnWhenStatusIsOngoingAndResultIsCommit(): Unit = {
    val transactionId = "known"
    val pid = 10
    val epoch:Short = 1
    val txnTimeoutMs = 1

    mockPrepare(transactionId, pid, epoch, txnTimeoutMs, PrepareCommit)

    EasyMock.replay(transactionManager)

    coordinator.handleEndTransaction(transactionId, pid, epoch, TransactionResult.COMMIT, errorsCallback)

    EasyMock.verify(transactionManager)
  }

  @Test
  def shouldAppendPrepareAbortToLogOnEndTxnWhenStatusIsOngoingAndResultIsAbort(): Unit = {
    val transactionId = "known"
    val pid = 10
    val epoch:Short = 1
    val txnTimeoutMs = 1

    mockPrepare(transactionId, pid, epoch, txnTimeoutMs, PrepareAbort)

    EasyMock.replay(transactionManager)

    coordinator.handleEndTransaction(transactionId, pid, epoch, TransactionResult.ABORT, errorsCallback)
    EasyMock.verify(transactionManager)
  }


  @Test
  def shouldAppendCompleteAbortToLogOnEndTxnWhenStatusIsOngoingAndResultIsAbort(): Unit = {
    val transactionId = "known"
    val pid = 10
    val epoch:Short = 1
    val txnTimeoutMs = 1

    mockComplete(transactionId, pid, epoch, txnTimeoutMs, PrepareAbort)

    EasyMock.replay(transactionManager, transactionMarkerChannelManager)

    coordinator.handleEndTransaction(transactionId, pid, epoch, TransactionResult.ABORT, errorsCallback)

    EasyMock.verify(transactionManager)
  }

  @Test
  def shouldAppendCompleteCommitToLogOnEndTxnWhenStatusIsOngoingAndResultIsCommit(): Unit = {
    val transactionId = "known"
    val pid = 10
    val epoch:Short = 1
    val txnTimeoutMs = 1

    mockComplete(transactionId, pid, epoch, txnTimeoutMs, PrepareCommit)

    EasyMock.replay(transactionManager, transactionMarkerChannelManager)

    coordinator.handleEndTransaction(transactionId, pid, epoch, TransactionResult.COMMIT, errorsCallback)

    EasyMock.verify(transactionManager)
  }

  @Test
  def shouldRespondWithInvalidRequestOnEndTxnWhenTransactionalIdIsNull(): Unit = {
    coordinator.handleEndTransaction(null, 0, 0, TransactionResult.COMMIT, errorsCallback)
    assertEquals(Errors.INVALID_REQUEST, error)
  }

  @Test
  def shouldRespondWithInvalidRequestOnEndTxnWhenTransactionalIdIsEmpty(): Unit = {
    coordinator.handleEndTransaction("", 0, 0, TransactionResult.COMMIT, errorsCallback)
    assertEquals(Errors.INVALID_REQUEST, error)
  }

  @Test
  def shouldRespondWithNotCoordinatorOnEndTxnWhenIsNotCoordinatorForId(): Unit = {
    coordinator.handleEndTransaction("id", 0, 0, TransactionResult.COMMIT, errorsCallback)
    assertEquals(Errors.NOT_COORDINATOR, error)
  }

  @Test
  def shouldRespondWithCoordinatorLoadInProgressOnEndTxnWhenCoordinatorIsLoading(): Unit = {
    EasyMock.expect(transactionManager.isCoordinatorFor(EasyMock.anyString()))
      .andReturn(true)
    EasyMock.expect(transactionManager.isCoordinatorLoadingInProgress(EasyMock.anyString()))
      .andReturn(true)

    EasyMock.replay(transactionManager)

    coordinator.handleEndTransaction("id", 0, 0, TransactionResult.COMMIT, errorsCallback)
    assertEquals(Errors.COORDINATOR_LOAD_IN_PROGRESS, error)
  }

  private def mockPrepare(transactionId: String,
                          pid: Int,
                          epoch: Short,
                          txnTimeoutMs: Int,
                          transactionState: TransactionState,
                          runCallback: Boolean = false) = {
    val originalMetadata = new TransactionMetadata(pid,
      epoch,
      txnTimeoutMs,
      Ongoing,
      collection.mutable.Set.empty[TopicPartition],
      time.milliseconds())

    val prepareCommitMetadata = new TransactionMetadata(pid,
      epoch,
      txnTimeoutMs,
      transactionState,
      collection.mutable.Set.empty[TopicPartition], time.milliseconds())

    EasyMock.expect(transactionManager.isCoordinatorFor(transactionId))
      .andReturn(true)
    EasyMock.expect(transactionManager.getTransaction(transactionId))
      .andReturn(Some(originalMetadata))
      .once()

    EasyMock.expect(transactionManager.appendTransactionToLog(EasyMock.eq(transactionId),
      EasyMock.eq(prepareCommitMetadata),
      EasyMock.capture(capturedArgument),
      EasyMock.anyObject()))
      .andAnswer(new IAnswer[Unit] {
        override def answer(): Unit = {
          if (runCallback) capturedArgument.getValue.apply(Errors.NONE)
        }
      }).once()
    prepareCommitMetadata
  }

  private def mockComplete(transactionId: String,
                           pid: Int,
                           epoch: Short,
                           txnTimeoutMs: Int,
                           transactionState: TransactionState) = {


    val prepareMetadata: TransactionMetadata = mockPrepare(transactionId, pid, epoch, txnTimeoutMs, transactionState, true)
    val finalState = if (transactionState == PrepareAbort) CompleteAbort else CompleteCommit

    EasyMock.expect(transactionManager.coordinatorEpochFor(transactionId))
      .andReturn(Some(0))

    EasyMock.expect(transactionManager.getTransaction(transactionId))
      .andReturn(Some(prepareMetadata))
      .once()

    val completedMetadata = new TransactionMetadata(pid,
      epoch,
      txnTimeoutMs,
      finalState,
      prepareMetadata.topicPartitions,
      prepareMetadata.timestamp)

    EasyMock.expect(transactionManager.appendTransactionToLog(EasyMock.eq(transactionId),
      EasyMock.eq(completedMetadata),
      EasyMock.capture(capturedArgument),
      EasyMock.anyObject()))
      .andAnswer(new IAnswer[Unit] {
        override def answer(): Unit = {

        }
      }).once()

    EasyMock.expect(transactionMarkerChannelManager.addTxnMarkerRequest(
      EasyMock.anyObject(),
      EasyMock.anyInt(),
      EasyMock.capture(capturedArgument)
    )).andAnswer(new IAnswer[Unit] {
      override def answer(): Unit = {
        capturedArgument.getValue.apply(Errors.NONE)
      }
    })
  }


  def initPidMockCallback(ret: InitPidResult): Unit = {
    result = ret
  }

  def errorsCallback(ret: Errors): Unit = {
    error = ret
  }
}
