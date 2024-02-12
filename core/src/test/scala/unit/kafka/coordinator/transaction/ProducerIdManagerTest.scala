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

import kafka.coordinator.transaction.ProducerIdManager.RetryBackoffMs
import kafka.utils.TestUtils
import kafka.zk.{KafkaZkClient, ProducerIdBlockZNode}
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.errors.CoordinatorLoadInProgressException
import org.apache.kafka.common.message.AllocateProducerIdsResponseData
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.AllocateProducerIdsResponse
import org.apache.kafka.common.utils.{MockTime, Time}
import org.apache.kafka.server.NodeToControllerChannelManager
import org.apache.kafka.server.common.ProducerIdsBlock
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{EnumSource, ValueSource}
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.{any, anyString}
import org.mockito.Mockito.{mock, when}

import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.mutable
import scala.util.{Failure, Success}

class ProducerIdManagerTest {

  var brokerToController: NodeToControllerChannelManager = mock(classOf[NodeToControllerChannelManager])
  val zkClient: KafkaZkClient = mock(classOf[KafkaZkClient])

  // Mutable test implementation that lets us easily set the idStart and error
  class MockProducerIdManager(
    val brokerId: Int,
    var idStart: Long,
    val idLen: Int,
    var error: Errors = Errors.NONE,
    val isErroneousBlock: Boolean = false,
    val time: Time = Time.SYSTEM,
    var remainingRetries: Int = 1
  ) extends RPCProducerIdManager(brokerId, time, () => 1, brokerToController) {

    private val brokerToControllerRequestExecutor = Executors.newSingleThreadExecutor()
    val capturedFailure: AtomicBoolean = new AtomicBoolean(false)

    override private[transaction] def sendRequest(): Unit = {

      brokerToControllerRequestExecutor.submit(() => {
        if (error == Errors.NONE) {
          handleAllocateProducerIdsResponse(new AllocateProducerIdsResponse(
            new AllocateProducerIdsResponseData().setProducerIdStart(idStart).setProducerIdLen(idLen)))
          if (!isErroneousBlock) {
            idStart += idLen
          }
        } else {
          handleAllocateProducerIdsResponse(new AllocateProducerIdsResponse(
            new AllocateProducerIdsResponseData().setErrorCode(error.code)))
        }
      }, 0)
    }

    override private[transaction] def handleAllocateProducerIdsResponse(response: AllocateProducerIdsResponse): Unit = {
      super.handleAllocateProducerIdsResponse(response)
      capturedFailure.set(nextProducerIdBlock.get == null)
    }

    override private[transaction] def maybeRequestNextBlock(): Unit = {
      if (error == Errors.NONE && !isErroneousBlock) {
        super.maybeRequestNextBlock()
      } else {
        if (remainingRetries > 0) {
          super.maybeRequestNextBlock()
          remainingRetries -= 1
        }
      }
    }
  }

  @Test
  def testGetProducerIdZk(): Unit = {
    var zkVersion: Option[Int] = None
    var data: Array[Byte] = null
    when(zkClient.getDataAndVersion(anyString)).thenAnswer(_ =>
      zkVersion.map(Some(data) -> _).getOrElse(None, 0))

    val capturedVersion: ArgumentCaptor[Int] = ArgumentCaptor.forClass(classOf[Int])
    val capturedData: ArgumentCaptor[Array[Byte]] = ArgumentCaptor.forClass(classOf[Array[Byte]])
    when(zkClient.conditionalUpdatePath(anyString(),
      capturedData.capture(),
      capturedVersion.capture(),
      any[Option[(KafkaZkClient, String, Array[Byte]) => (Boolean, Int)]])
    ).thenAnswer(_ => {
      val newZkVersion = capturedVersion.getValue + 1
      zkVersion = Some(newZkVersion)
      data = capturedData.getValue
      (true, newZkVersion)
    })

    val manager1 = new ZkProducerIdManager(0, zkClient)
    val manager2 = new ZkProducerIdManager(1, zkClient)

    val pid1 = manager1.generateProducerId().get
    val pid2 = manager2.generateProducerId().get

    assertEquals(0, pid1)
    assertEquals(ProducerIdsBlock.PRODUCER_ID_BLOCK_SIZE, pid2)

    for (i <- 1L until ProducerIdsBlock.PRODUCER_ID_BLOCK_SIZE)
      assertEquals(pid1 + i, manager1.generateProducerId().get)

    for (i <- 1L until ProducerIdsBlock.PRODUCER_ID_BLOCK_SIZE)
      assertEquals(pid2 + i, manager2.generateProducerId().get)

    assertEquals(pid2 + ProducerIdsBlock.PRODUCER_ID_BLOCK_SIZE, manager1.generateProducerId().get)
    assertEquals(pid2 + ProducerIdsBlock.PRODUCER_ID_BLOCK_SIZE * 2, manager2.generateProducerId().get)
  }

  @Test
  def testExceedProducerIdLimitZk(): Unit = {
    when(zkClient.getDataAndVersion(anyString)).thenAnswer(_ => {
      val json = ProducerIdBlockZNode.generateProducerIdBlockJson(
        new ProducerIdsBlock(0, Long.MaxValue - ProducerIdsBlock.PRODUCER_ID_BLOCK_SIZE, ProducerIdsBlock.PRODUCER_ID_BLOCK_SIZE))
      (Some(json), 0)
    })
    assertThrows(classOf[KafkaException], () => new ZkProducerIdManager(0, zkClient))
  }

  @ParameterizedTest
  @ValueSource(ints = Array(1, 2, 10, 100))
  def testConcurrentGeneratePidRequests(idBlockLen: Int): Unit = {
    // Send concurrent generateProducerId requests. Ensure that the generated producer id is unique.
    // For each block (total 3 blocks), only "idBlockLen" number of requests should go through.
    // All other requests should fail immediately.

    val numThreads = 5
    val latch = new CountDownLatch(idBlockLen * 3)
    val manager = new MockProducerIdManager(0, 0, idBlockLen)
    val pidMap = mutable.Map[Long, Int]()
    val requestHandlerThreadPool = Executors.newFixedThreadPool(numThreads)

    for ( _ <- 0 until numThreads) {
      requestHandlerThreadPool.submit(() => {
        while (latch.getCount > 0) {
          val result = manager.generateProducerId()
          result match {
            case Success(pid) =>
              pidMap synchronized {
                if (latch.getCount != 0) {
                  val counter = pidMap.getOrElse(pid, 0)
                  pidMap += pid -> (counter + 1)
                  latch.countDown()
                }
              }

            case Failure(exception) =>
              assertEquals(classOf[CoordinatorLoadInProgressException], exception.getClass)
          }
          Thread.sleep(100)
        }
      }, 0)
    }
    assertTrue(latch.await(12000, TimeUnit.MILLISECONDS))
    requestHandlerThreadPool.shutdown()

    assertEquals(idBlockLen * 3, pidMap.size)
    pidMap.foreach { case (pid, count) =>
      assertEquals(1, count)
      assertTrue(pid < (3 * idBlockLen) + numThreads, s"Unexpected pid $pid; " +
        s"non-contiguous blocks generated or did not fully exhaust blocks.")
    }
  }

  @ParameterizedTest
  @EnumSource(value = classOf[Errors], names = Array("UNKNOWN_SERVER_ERROR", "INVALID_REQUEST"))
  def testUnrecoverableErrors(error: Errors): Unit = {
    val time = new MockTime()
    val manager = new MockProducerIdManager(0, 0, 1, time = time)

    verifyNewBlockAndProducerId(manager, new ProducerIdsBlock(0, 0, 1), 0)

    manager.error = error
    time.sleep(RetryBackoffMs)
    verifyFailure(manager)

    manager.error = Errors.NONE
    time.sleep(RetryBackoffMs)
    verifyNewBlockAndProducerId(manager, new ProducerIdsBlock(0, 1, 1), 1)
  }

  @Test
  def testInvalidRanges(): Unit = {
    var manager = new MockProducerIdManager(0, -1, 10, isErroneousBlock = true)
    verifyFailure(manager)

    manager = new MockProducerIdManager(0, 0, -1, isErroneousBlock = true)
    verifyFailure(manager)

    manager = new MockProducerIdManager(0, Long.MaxValue-1, 10, isErroneousBlock = true)
    verifyFailure(manager)
  }

  @Test
  def testRetryBackoff(): Unit = {
    val time = new MockTime()
    val manager = new MockProducerIdManager(0, 0, 1,
      error = Errors.UNKNOWN_SERVER_ERROR, time = time, remainingRetries = 2)

    verifyFailure(manager)
    manager.error = Errors.NONE

    // We should only get a new block once retry backoff ms has passed.
    assertEquals(classOf[CoordinatorLoadInProgressException], manager.generateProducerId().failed.get.getClass)
    time.sleep(RetryBackoffMs)
    verifyNewBlockAndProducerId(manager, new ProducerIdsBlock(0, 0, 1), 0)
  }

  private def verifyFailure(manager: MockProducerIdManager): Unit = {
    assertEquals(classOf[CoordinatorLoadInProgressException], manager.generateProducerId().failed.get.getClass)
    TestUtils.waitUntilTrue(() => {
      manager synchronized {
        manager.capturedFailure.get
      }
    }, "Expected failure")
    manager.capturedFailure.set(false)
  }

  private def verifyNewBlockAndProducerId(manager: MockProducerIdManager,
                                          expectedBlock: ProducerIdsBlock,
                                          expectedPid: Long): Unit = {

    assertEquals(classOf[CoordinatorLoadInProgressException], manager.generateProducerId().failed.get.getClass)
    TestUtils.waitUntilTrue(() => {
      val nextBlock = manager.nextProducerIdBlock.get
      nextBlock != null && nextBlock.equals(expectedBlock)
    }, "failed to generate block")
    assertEquals(expectedPid, manager.generateProducerId().get)
  }
}

