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

import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.utils.MockTime
import org.easymock.{Capture, EasyMock, IAnswer}
import org.junit.Assert._
import org.junit.{After, Before, Test}

class TransactionCoordinatorTest {

  val time = new MockTime()

  var nextPid: Long = 0L
  val pidManager = EasyMock.createNiceMock(classOf[ProducerIdManager])

  EasyMock.expect(pidManager.nextPid())
    .andAnswer(new IAnswer[Long] {
      override def answer(): Long = {
        nextPid += 1
        nextPid - 1
      }
    })
    .anyTimes()

  val transactionManager = EasyMock.createNiceMock(classOf[TransactionStateManager])

  EasyMock.expect(transactionManager.isCoordinatorFor(EasyMock.eq("a")))
    .andReturn(true)
    .anyTimes()
  EasyMock.expect(transactionManager.isCoordinatorFor(EasyMock.eq("b")))
    .andReturn(false)
    .anyTimes()
  EasyMock.expect(transactionManager.isCoordinatorLoadingInProgress(EasyMock.anyString()))
    .andReturn(false)
    .anyTimes()
  EasyMock.expect(transactionManager.validateTransactionTimeoutMs(EasyMock.anyInt()))
    .andReturn(true)
    .anyTimes()

  val capturedTxn: Capture[TransactionMetadata] = EasyMock.newCapture()
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

  val coordinator: TransactionCoordinator = new TransactionCoordinator(0, pidManager, transactionManager)

  var result: InitPidResult = _

  @Before
  def setUp(): Unit = {
    EasyMock.replay(pidManager, transactionManager)

    coordinator.startup()
    // only give one of the two partitions of the transaction topic
    coordinator.handleTxnImmigration(1)
  }

  @After
  def tearDown(): Unit = {
    EasyMock.reset(pidManager, transactionManager)
    coordinator.shutdown()
  }

  @Test
  def testHandleInitPid() = {
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

  def initPidMockCallback(ret: InitPidResult): Unit = {
    result = ret
  }
}
