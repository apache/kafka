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

import kafka.server.BrokerToControllerChannelManager
import kafka.zk.{KafkaZkClient, ProducerIdBlockZNode}
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.errors.CoordinatorLoadInProgressException
import org.apache.kafka.common.message.AllocateProducerIdsResponseData
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.AllocateProducerIdsResponse
import org.apache.kafka.server.common.ProducerIdsBlock
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{EnumSource, ValueSource}
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.{any, anyString}
import org.mockito.Mockito.{mock, when}
import java.util.stream.IntStream

class ProducerIdManagerTest {

  var brokerToController: BrokerToControllerChannelManager = mock(classOf[BrokerToControllerChannelManager])
  val zkClient: KafkaZkClient = mock(classOf[KafkaZkClient])

  // Mutable test implementation that lets us easily set the idStart and error
  class MockProducerIdManager(val brokerId: Int, var idStart: Long, val idLen: Int, var error: Errors = Errors.NONE, timeout: Boolean = false)
    extends RPCProducerIdManager(brokerId, () => 1, brokerToController, 100) {

    override private[transaction] def sendRequest(): Unit = {
      if (timeout)
        return

      if (error == Errors.NONE) {
        handleAllocateProducerIdsResponse(new AllocateProducerIdsResponse(
          new AllocateProducerIdsResponseData().setProducerIdStart(idStart).setProducerIdLen(idLen)))
        idStart += idLen
      } else {
        handleAllocateProducerIdsResponse(new AllocateProducerIdsResponse(
          new AllocateProducerIdsResponseData().setErrorCode(error.code)))
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

    val pid1 = manager1.generateProducerId()
    val pid2 = manager2.generateProducerId()

    assertEquals(0, pid1)
    assertEquals(ProducerIdsBlock.PRODUCER_ID_BLOCK_SIZE, pid2)

    for (i <- 1L until ProducerIdsBlock.PRODUCER_ID_BLOCK_SIZE)
      assertEquals(pid1 + i, manager1.generateProducerId())

    for (i <- 1L until ProducerIdsBlock.PRODUCER_ID_BLOCK_SIZE)
      assertEquals(pid2 + i, manager2.generateProducerId())

    assertEquals(pid2 + ProducerIdsBlock.PRODUCER_ID_BLOCK_SIZE, manager1.generateProducerId())
    assertEquals(pid2 + ProducerIdsBlock.PRODUCER_ID_BLOCK_SIZE * 2, manager2.generateProducerId())
  }

  @Test
  def testRPCProducerIdManagerThrowsConcurrentTransactions(): Unit = {
    val manager1 = new MockProducerIdManager(0, 0, 0, timeout = true)
    assertThrows(classOf[CoordinatorLoadInProgressException], () => manager1.generateProducerId())
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
  @ValueSource(ints = Array(1, 2, 10))
  def testContiguousIds(idBlockLen: Int): Unit = {
    val manager = new MockProducerIdManager(0, 0, idBlockLen)

    IntStream.range(0, idBlockLen * 3).forEach { i =>
      assertEquals(i, manager.generateProducerId())
    }
  }

  @ParameterizedTest
  @EnumSource(value = classOf[Errors], names = Array("UNKNOWN_SERVER_ERROR", "INVALID_REQUEST"))
  def testUnrecoverableErrors(error: Errors): Unit = {
    val manager = new MockProducerIdManager(0, 0, 1)
    assertEquals(0, manager.generateProducerId())

    manager.error = error
    assertThrows(classOf[Throwable], () => manager.generateProducerId())

    manager.error = Errors.NONE
    assertEquals(1, manager.generateProducerId())
  }

  @Test
  def testInvalidRanges(): Unit = {
    var manager = new MockProducerIdManager(0, -1, 10)
    assertThrows(classOf[KafkaException], () => manager.generateProducerId())

    manager = new MockProducerIdManager(0, 0, -1)
    assertThrows(classOf[KafkaException], () => manager.generateProducerId())

    manager = new MockProducerIdManager(0, Long.MaxValue-1, 10)
    assertThrows(classOf[KafkaException], () => manager.generateProducerId())
  }
}

