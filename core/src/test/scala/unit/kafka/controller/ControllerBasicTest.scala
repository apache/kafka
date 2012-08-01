/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package kafka.controller

import org.scalatest.junit.JUnit3Suite
import kafka.zk.ZooKeeperTestHarness
import kafka.utils.TestUtils._
import junit.framework.Assert._
import kafka.server.{KafkaServer, KafkaConfig}
import kafka.api._
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger
import kafka.admin.CreateTopicCommand
import kafka.utils.{ZkUtils, ControllerTestUtils, TestUtils}


class ControllerBasicTest extends JUnit3Suite with ZooKeeperTestHarness  {
  val props = createBrokerConfigs(4)
  val configs = props.map(p => new KafkaConfig(p))
  var brokers: Seq[KafkaServer] = null

  override def setUp() {
    super.setUp()
    brokers = configs.map(config => TestUtils.createServer(config))
    CreateTopicCommand.createTopic(zkClient, "test1", 1, 4, "0:1:2:3")
    CreateTopicCommand.createTopic(zkClient, "test2", 1, 4, "0:1:2:3")
  }

  override def tearDown() {
    brokers.foreach(_.shutdown())
    super.tearDown()
  }

  def testControllerFailOver(){
    brokers(0).shutdown()
    brokers(1).shutdown()
    brokers(3).shutdown()
    assertTrue("Controller not elected", TestUtils.waitUntilTrue(() =>
      ZkUtils.readDataMaybeNull(zkClient, ZkUtils.ControllerPath)._1 != null, zookeeper.tickTime))
    var curController = ZkUtils.readDataMaybeNull(zkClient, ZkUtils.ControllerPath)._1
    assertEquals("Controller should move to broker 2", "2", curController)


    brokers(1).startup()
    brokers(2).shutdown()
    assertTrue("Controller not elected", TestUtils.waitUntilTrue(() =>
      ZkUtils.readDataMaybeNull(zkClient, ZkUtils.ControllerPath)._1 != null, zookeeper.tickTime))
    curController = ZkUtils.readDataMaybeNull(zkClient, ZkUtils.ControllerPath)._1
    assertEquals("Controller should move to broker 1", "1", curController)
  }

  def testControllerCommandSend(){
    for(broker <- brokers){
      if(broker.kafkaController.isActive){
        val leaderAndISRRequest = ControllerTestUtils.createTestLeaderAndISRRequest()
        val stopReplicaRequest = ControllerTestUtils.createTestStopReplicaRequest()

        val successCount: AtomicInteger = new AtomicInteger(0)
        val countDownLatch: CountDownLatch = new CountDownLatch(8)

        def compareLeaderAndISRResponseWithExpectedOne(response: RequestOrResponse){
          val expectedResponse = ControllerTestUtils.createTestLeaderAndISRResponse()
          if(response.equals(expectedResponse))
            successCount.addAndGet(1)
          countDownLatch.countDown()
        }

        def compareStopReplicaResponseWithExpectedOne(response: RequestOrResponse){
          val expectedResponse = ControllerTestUtils.createTestStopReplicaResponse()
          if(response.equals(expectedResponse))
            successCount.addAndGet(1)
          countDownLatch.countDown()
        }

        broker.kafkaController.sendRequest(0, leaderAndISRRequest, compareLeaderAndISRResponseWithExpectedOne)
        broker.kafkaController.sendRequest(1, leaderAndISRRequest, compareLeaderAndISRResponseWithExpectedOne)
        broker.kafkaController.sendRequest(2, leaderAndISRRequest, compareLeaderAndISRResponseWithExpectedOne)
        broker.kafkaController.sendRequest(3, leaderAndISRRequest, compareLeaderAndISRResponseWithExpectedOne)
        broker.kafkaController.sendRequest(0, leaderAndISRRequest, compareLeaderAndISRResponseWithExpectedOne)
        broker.kafkaController.sendRequest(1, leaderAndISRRequest, compareLeaderAndISRResponseWithExpectedOne)
        broker.kafkaController.sendRequest(2, leaderAndISRRequest, compareLeaderAndISRResponseWithExpectedOne)
        broker.kafkaController.sendRequest(3, leaderAndISRRequest, compareLeaderAndISRResponseWithExpectedOne)
        countDownLatch.await()

        assertEquals(successCount.get(), 8)
      }
    }
  }
}