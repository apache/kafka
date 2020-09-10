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

package unit.kafka.server

import kafka.api.LeaderAndIsr
import kafka.server.{AlterIsrItem, AlterIsrManagerImpl, BrokerToControllerChannelManager}
import kafka.utils.{MockScheduler, MockTime}
import kafka.zk.KafkaZkClient
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.requests.{AbstractRequest, AlterIsrRequest}
import org.easymock.EasyMock
import org.junit.Assert._
import org.junit.{Before, Test}


class AlterIsrManagerTest {

  val topic = "test-topic"
  val time = new MockTime
  val metrics = new Metrics
  val brokerId = 1

  var kafkaZkClient: KafkaZkClient = _
  var brokerToController: BrokerToControllerChannelManager = _

  @Before
  def setup(): Unit = {
    kafkaZkClient = EasyMock.createMock(classOf[KafkaZkClient])
    EasyMock.expect(kafkaZkClient.getBrokerEpoch(EasyMock.anyInt())).andReturn(Some(4)).anyTimes()
    EasyMock.replay(kafkaZkClient)

    brokerToController = EasyMock.createMock(classOf[BrokerToControllerChannelManager])
  }

  @Test
  def testBasic(): Unit = {
    EasyMock.expect(brokerToController.sendRequest(EasyMock.anyObject(), EasyMock.anyObject())).once()
    EasyMock.replay(brokerToController)

    val scheduler = new MockScheduler(time)
    val alterIsrManager = new AlterIsrManagerImpl(brokerToController, kafkaZkClient, scheduler, time, brokerId, () => 2)
    alterIsrManager.enqueueIsrUpdate(AlterIsrItem(new TopicPartition(topic, 1), new LeaderAndIsr(1, 1, List(1,2,3), 10), _ => {}))
    time.sleep(50)
    scheduler.tick()

    EasyMock.verify(brokerToController)
  }

  @Test
  def testOverwriteWithinBatch(): Unit = {
    val capture = EasyMock.newCapture[AbstractRequest.Builder[AlterIsrRequest]]()
    EasyMock.expect(brokerToController.sendRequest(EasyMock.capture(capture), EasyMock.anyObject())).once()
    EasyMock.replay(brokerToController)

    val scheduler = new MockScheduler(time)
    val alterIsrManager = new AlterIsrManagerImpl(brokerToController, kafkaZkClient, scheduler, time, brokerId, () => 2)
    // Only send one ISR update for a given topic+partition
    alterIsrManager.enqueueIsrUpdate(AlterIsrItem(new TopicPartition(topic, 1), new LeaderAndIsr(1, 1, List(1,2,3), 10), _ => {}))
    alterIsrManager.enqueueIsrUpdate(AlterIsrItem(new TopicPartition(topic, 1), new LeaderAndIsr(1, 1, List(1,2), 10), _ => {}))

    time.sleep(50)
    scheduler.tick()

    EasyMock.verify(brokerToController)

    val request = capture.getValue.build()
    assertEquals(request.data().topics().size(), 1)
    assertEquals(request.data().topics().get(0).partitions().get(0).newIsr().size(), 2)
  }

  @Test
  def testSingleBatch(): Unit = {
    val capture = EasyMock.newCapture[AbstractRequest.Builder[AlterIsrRequest]]()
    EasyMock.expect(brokerToController.sendRequest(EasyMock.capture(capture), EasyMock.anyObject())).once()
    EasyMock.replay(brokerToController)

    val scheduler = new MockScheduler(time)
    val alterIsrManager = new AlterIsrManagerImpl(brokerToController, kafkaZkClient, scheduler, time, brokerId, () => 2)

    for (i <- 0 to 9) {
      alterIsrManager.enqueueIsrUpdate(AlterIsrItem(new TopicPartition(topic, i), new LeaderAndIsr(1, 1, List(1,2,3), 10), _ => {}))
      time.sleep(1)
    }

    time.sleep(50)
    scheduler.tick()

    // This should not be included in the batch
    alterIsrManager.enqueueIsrUpdate(AlterIsrItem(new TopicPartition(topic, 10), new LeaderAndIsr(1, 1, List(1,2,3), 10), _ => {}))

    EasyMock.verify(brokerToController)

    val request = capture.getValue.build()
    assertEquals(request.data().topics().size(), 1)
    assertEquals(request.data().topics().get(0).partitions().size(), 10)
  }

  @Test(expected = classOf[RuntimeException])
  def testNoBrokerEpoch(): Unit = {
    EasyMock.reset(kafkaZkClient)
    EasyMock.expect(kafkaZkClient.getBrokerEpoch(EasyMock.anyInt())).andReturn(None).anyTimes()
    EasyMock.replay(kafkaZkClient)

    val scheduler = new MockScheduler(time)
    val alterIsrManager = new AlterIsrManagerImpl(brokerToController, kafkaZkClient, scheduler, time, brokerId, () => 2)
    alterIsrManager.enqueueIsrUpdate(AlterIsrItem(new TopicPartition(topic, 1), new LeaderAndIsr(1, 1, List(1,2,3), 10), _ => {}))
    time.sleep(50) // throws
    scheduler.tick()

    EasyMock.verify(brokerToController)
  }
}
