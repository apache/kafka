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
import kafka.utils.{ControllerTestUtils, ZkUtils, TestUtils}


class ControllerBasicTest extends JUnit3Suite with ZooKeeperTestHarness  {
  val props = createBrokerConfigs(4)
  val configs = props.map(p => new KafkaConfig(p))
  var brokers: Seq[KafkaServer] = null

  override def setUp() {
    super.setUp()
    brokers = configs.map(config => TestUtils.createServer(config))
  }

  override def tearDown() {
    super.tearDown()
    brokers.foreach(_.shutdown())
  }

  def testControllerFailOver(){
    brokers(0).shutdown()
    brokers(1).shutdown()
    brokers(3).shutdown()
    Thread.sleep(1000)

    var curController = ZkUtils.readDataMaybeNull(zkClient, ZkUtils.ControllerPath)
    assertEquals(curController, "2")

    brokers(1).startup()
    brokers(2).shutdown()
    Thread.sleep(1000)
    curController = ZkUtils.readDataMaybeNull(zkClient, ZkUtils.ControllerPath)
    assertEquals(curController, "1")
  }

  def testControllerCommandSend(){
    Thread.sleep(1000)
    for(broker <- brokers){
      if(broker.kafkaController.isActive){
        val leaderAndISRRequest = ControllerTestUtils.createSampleLeaderAndISRRequest()
        val stopReplicaRequest = ControllerTestUtils.createSampleStopReplicaRequest()

        val successCount: AtomicInteger = new AtomicInteger(0)
        val countDownLatch: CountDownLatch = new CountDownLatch(8)

        def compareLeaderAndISRResponseWithExpectedOne(response: RequestOrResponse){
          val expectedResponse = ControllerTestUtils.createSampleLeaderAndISRResponse()
          if(response.equals(expectedResponse))
            successCount.addAndGet(1)
          countDownLatch.countDown()
        }

        def compareStopReplicaResponseWithExpectedOne(response: RequestOrResponse){
          val expectedResponse = ControllerTestUtils.createSampleStopReplicaResponse()
          if(response.equals(expectedResponse))
            successCount.addAndGet(1)
          countDownLatch.countDown()
        }

        broker.kafkaController.sendRequest(0, leaderAndISRRequest, compareLeaderAndISRResponseWithExpectedOne)
        broker.kafkaController.sendRequest(1, leaderAndISRRequest, compareLeaderAndISRResponseWithExpectedOne)
        broker.kafkaController.sendRequest(2, leaderAndISRRequest, compareLeaderAndISRResponseWithExpectedOne)
        broker.kafkaController.sendRequest(3, leaderAndISRRequest, compareLeaderAndISRResponseWithExpectedOne)
        broker.kafkaController.sendRequest(0, stopReplicaRequest, compareStopReplicaResponseWithExpectedOne)
        broker.kafkaController.sendRequest(1, stopReplicaRequest, compareStopReplicaResponseWithExpectedOne)
        broker.kafkaController.sendRequest(2, stopReplicaRequest, compareStopReplicaResponseWithExpectedOne)
        broker.kafkaController.sendRequest(3, stopReplicaRequest, compareStopReplicaResponseWithExpectedOne)
        countDownLatch.await()

        assertEquals(successCount.get(), 8)
      }
    }
  }
}