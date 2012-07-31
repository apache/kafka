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

package kafka.server

import org.scalatest.junit.JUnit3Suite
import kafka.zk.ZooKeeperTestHarness
import kafka.common.QueueFullException
import junit.framework.Assert._
import kafka.utils.{ZkQueue, TestUtils}

class StateChangeTest extends JUnit3Suite with ZooKeeperTestHarness {

  val brokerId1 = 0
  val port1 = TestUtils.choosePort()
  var stateChangeQ: ZkQueue = null
  val config = new KafkaConfig(TestUtils.createBrokerConfig(brokerId1, port1))

  override def setUp() {
    super.setUp()

    // create a queue
    val queuePath = "/brokers/state/" + config.brokerId
    stateChangeQ = new ZkQueue(zkClient, queuePath, 10)
  }

  override def tearDown() {
    super.tearDown()
  }

  def testZkQueueDrainAll() {
    for(i <- 0 until 5) {
      val itemPath = stateChangeQ.put("test:0:follower")
      val item = itemPath.split("/").last.split("-").last.toInt
      assertEquals(i, item)
    }

    var numItems: Int = 0
    for(i <- 0 until 5) {
      val item = stateChangeQ.take()
      assertEquals("test:0:follower", item._2)
      assertTrue(stateChangeQ.remove(item))
      numItems += 1
    }
    assertEquals(5, numItems)

    for(i <- 5 until 10) {
      val itemPath = stateChangeQ.put("test:1:follower")
      val item = itemPath.split("/").last.split("-").last.toInt
      assertEquals(i+5, item)
    }

    numItems = 0
    for(i <- 0 until 5) {
      val item = stateChangeQ.take()
      assertTrue(stateChangeQ.remove(item))
      assertEquals("test:1:follower", item._2)
      numItems += 1
    }
    assertEquals(5, numItems)
  }

  def testZkQueueFull() {
    for(i <- 0 until 10) {
      val itemPath = stateChangeQ.put("test:0:follower")
      val item = itemPath.split("/").last.split("-").last.toInt
      assertEquals(i, item)
    }

    try {
      stateChangeQ.put("test:0:follower")
      fail("Queue should be full")
    }catch {
      case e:QueueFullException => // expected
    }
  }

  def testStateChangeCommandJson() {
    // test start replica
    val topic = "foo"
    val partition = 0
    val epoch = 1

    val startReplica = new StartReplica(topic, partition, epoch)
    val startReplicaJson = startReplica.toJson()
    val startReplicaFromJson = StateChangeCommand.getStateChangeRequest(startReplicaJson)
    assertEquals(startReplica, startReplicaFromJson)

    // test close replica
    val closeReplica = new StartReplica(topic, partition, epoch)
    val closeReplicaJson = startReplica.toJson()
    val closeReplicaFromJson = StateChangeCommand.getStateChangeRequest(closeReplicaJson)
    assertEquals(closeReplica, closeReplicaFromJson)
  }

  // TODO: Do this after patch for delete topic/delete partition is in
  def testStateChangeRequestValidity() {
    // mock out the StateChangeRequestHandler

    // setup 3 replicas for one topic partition

    // shutdown follower 1

    // restart leader to trigger epoch change

    // start follower 1

    // test follower 1 acted only on one become follower request
  }
}