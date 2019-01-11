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

import kafka.api.LeaderAndIsr
import kafka.cluster.Broker
import kafka.controller.{ControllerChannelManager, ControllerContext, StateChangeLogger}
import kafka.utils.TestUtils
import kafka.utils.TestUtils.createTopic
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.UpdateMetadataRequest.EndPoint
import org.apache.kafka.common.requests._
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.Time
import org.junit.Assert._
import org.junit.{After, Before, Test}

import scala.collection.JavaConverters._

class BrokerEpochIntegrationTest extends ZooKeeperTestHarness {
  val brokerId1 = 0
  val brokerId2 = 1

  var servers: Seq[KafkaServer] = Seq.empty[KafkaServer]

  @Before
  override def setUp() {
    super.setUp()
    val configs = Seq(
      TestUtils.createBrokerConfig(brokerId1, zkConnect),
      TestUtils.createBrokerConfig(brokerId2, zkConnect))

    configs.foreach { config =>
        config.setProperty(KafkaConfig.AutoLeaderRebalanceEnableProp, false.toString)}

    // start both servers
    servers = configs.map(config => TestUtils.createServer(KafkaConfig.fromProps(config)))
  }

  @After
  override def tearDown() {
    TestUtils.shutdownServers(servers)
    super.tearDown()
  }

  @Test
  def testReplicaManagerBrokerEpochMatchesWithZk(): Unit = {
    val brokerAndEpochs = zkClient.getAllBrokerAndEpochsInCluster
    assertEquals(brokerAndEpochs.size, servers.size)
    brokerAndEpochs.foreach {
      case (broker, epoch) =>
        val brokerServer = servers.find(e => e.config.brokerId == broker.id)
        assertTrue(brokerServer.isDefined)
        assertEquals(epoch, brokerServer.get.kafkaController.brokerEpoch)
    }
  }

  @Test
  def testControllerBrokerEpochCacheMatchesWithZk(): Unit = {
    val controller = getController
    val otherBroker = servers.find(e => e.config.brokerId != controller.config.brokerId).get

    // Broker epochs cache matches with zk in steady state
    checkControllerBrokerEpochsCacheMatchesWithZk(controller.kafkaController.controllerContext)

    // Shutdown a broker and make sure broker epochs cache still matches with zk state
    otherBroker.shutdown()
    checkControllerBrokerEpochsCacheMatchesWithZk(controller.kafkaController.controllerContext)

    // Restart a broker and make sure broker epochs cache still matches with zk state
    otherBroker.startup()
    checkControllerBrokerEpochsCacheMatchesWithZk(controller.kafkaController.controllerContext)
  }

  @Test
  def testControlRequestWithCorrectBrokerEpoch() {
    testControlRequestWithBrokerEpoch(false)
  }

  @Test
  def testControlRequestWithStaleBrokerEpoch() {
    testControlRequestWithBrokerEpoch(true)
  }

  private def testControlRequestWithBrokerEpoch(isEpochInRequestStale: Boolean) {
    val tp = new TopicPartition("new-topic", 0)

    // create topic with 1 partition, 2 replicas, one on each broker
    createTopic(zkClient, tp.topic(), partitionReplicaAssignment = Map(0 -> Seq(brokerId1, brokerId2)), servers = servers)

    val controllerId = 2
    val controllerEpoch = zkClient.getControllerEpoch.get._1

    val controllerConfig = KafkaConfig.fromProps(TestUtils.createBrokerConfig(controllerId, zkConnect))
    val securityProtocol = SecurityProtocol.PLAINTEXT
    val listenerName = ListenerName.forSecurityProtocol(securityProtocol)
    val brokerAndEpochs = servers.map(s =>
      (new Broker(s.config.brokerId, "localhost", TestUtils.boundPort(s), listenerName, securityProtocol),
        s.kafkaController.brokerEpoch)).toMap
    val nodes = brokerAndEpochs.keys.map(_.node(listenerName))

    val controllerContext = new ControllerContext
    controllerContext.setLiveBrokerAndEpochs(brokerAndEpochs)
    val metrics = new Metrics
    val controllerChannelManager = new ControllerChannelManager(controllerContext, controllerConfig, Time.SYSTEM,
      metrics, new StateChangeLogger(controllerId, inControllerContext = true, None))
    controllerChannelManager.startup()

    val broker2 = servers(brokerId2)
    val epochInRequest =
      if (isEpochInRequestStale) broker2.kafkaController.brokerEpoch - 1 else broker2.kafkaController.brokerEpoch

    try {
      // Send LeaderAndIsr request with correct broker epoch
      {
        val partitionStates = Map(
          tp -> new LeaderAndIsrRequest.PartitionState(controllerEpoch, brokerId2, LeaderAndIsr.initialLeaderEpoch + 1,
            Seq(brokerId1, brokerId2).map(Integer.valueOf).asJava, LeaderAndIsr.initialZKVersion,
            Seq(0, 1).map(Integer.valueOf).asJava, false)
        )
        val requestBuilder = new LeaderAndIsrRequest.Builder(
          ApiKeys.LEADER_AND_ISR.latestVersion, controllerId, controllerEpoch,
          epochInRequest,
          partitionStates.asJava, nodes.toSet.asJava)

        if (isEpochInRequestStale) {
          sendAndVerifyStaleBrokerEpochInResponse(controllerChannelManager, ApiKeys.LEADER_AND_ISR, requestBuilder)
        }
        else {
          sendAndVerifySuccessfulResponse(controllerChannelManager, ApiKeys.LEADER_AND_ISR, requestBuilder)
          TestUtils.waitUntilLeaderIsKnown(Seq(broker2), tp, 10000)
        }
      }

      // Send UpdateMetadata request with correct broker epoch
      {
        val partitionStates = Map(
          tp -> new UpdateMetadataRequest.PartitionState(controllerEpoch, brokerId2, LeaderAndIsr.initialLeaderEpoch + 1,
            Seq(brokerId1, brokerId2).map(Integer.valueOf).asJava, LeaderAndIsr.initialZKVersion,
            Seq(0, 1).map(Integer.valueOf).asJava, Seq.empty.asJava)
        )
        val liverBrokers = brokerAndEpochs.map { brokerAndEpoch =>
          val broker = brokerAndEpoch._1
          val securityProtocol = SecurityProtocol.PLAINTEXT
          val listenerName = ListenerName.forSecurityProtocol(securityProtocol)
          val node = broker.node(listenerName)
          val endPoints = Seq(new EndPoint(node.host, node.port, securityProtocol, listenerName))
          new UpdateMetadataRequest.Broker(broker.id, endPoints.asJava, broker.rack.orNull)
        }
        val requestBuilder = new UpdateMetadataRequest.Builder(
          ApiKeys.UPDATE_METADATA.latestVersion, controllerId, controllerEpoch,
          epochInRequest,
          partitionStates.asJava, liverBrokers.toSet.asJava)

        if (isEpochInRequestStale) {
          sendAndVerifyStaleBrokerEpochInResponse(controllerChannelManager, ApiKeys.UPDATE_METADATA, requestBuilder)
        }
        else {
          sendAndVerifySuccessfulResponse(controllerChannelManager, ApiKeys.UPDATE_METADATA, requestBuilder)
          TestUtils.waitUntilMetadataIsPropagated(Seq(broker2), tp.topic(), tp.partition(), 10000)
          assertEquals(brokerId2,
            broker2.metadataCache.getPartitionInfo(tp.topic(), tp.partition()).get.basePartitionState.leader)
        }
      }

      // Send StopReplica request with correct broker epoch
      {
        val requestBuilder = new StopReplicaRequest.Builder(
          ApiKeys.STOP_REPLICA.latestVersion, controllerId, controllerEpoch,
          epochInRequest, // Correct broker epoch
          true, Set(tp).asJava)

        if (isEpochInRequestStale) {
          sendAndVerifyStaleBrokerEpochInResponse(controllerChannelManager, ApiKeys.STOP_REPLICA, requestBuilder)
        }
        else {
          sendAndVerifySuccessfulResponse(controllerChannelManager, ApiKeys.STOP_REPLICA, requestBuilder)
          assertTrue(broker2.replicaManager.getPartition(tp).isEmpty)
        }
      }
    } finally {
      controllerChannelManager.shutdown()
      metrics.close()
    }
  }

  private def getController: KafkaServer = {
    val controllerId = TestUtils.waitUntilControllerElected(zkClient)
    servers.filter(s => s.config.brokerId == controllerId).head
  }

  private def checkControllerBrokerEpochsCacheMatchesWithZk(controllerContext: ControllerContext): Unit = {
    val brokerAndEpochs = zkClient.getAllBrokerAndEpochsInCluster
    TestUtils.waitUntilTrue(() => {
      val brokerEpochsInControllerContext = controllerContext.liveBrokerIdAndEpochs
      if (brokerAndEpochs.size != brokerEpochsInControllerContext.size) false
      else {
        brokerAndEpochs.forall {
          case (broker, epoch) => brokerEpochsInControllerContext.get(broker.id).contains(epoch)
        }
      }
    }, "Broker epoch mismatches")
  }

  private def sendAndVerifyStaleBrokerEpochInResponse(controllerChannelManager: ControllerChannelManager, apiKeys: ApiKeys,
                                               builder: AbstractControlRequest.Builder[_ <: AbstractControlRequest]): Unit = {
    var staleBrokerEpochDetected = false
    controllerChannelManager.sendRequest(brokerId2, apiKeys, builder,
      response => {staleBrokerEpochDetected = response.errorCounts().containsKey(Errors.STALE_BROKER_EPOCH)})
    TestUtils.waitUntilTrue(() => staleBrokerEpochDetected, "Broker epoch should be stale")
    assertTrue("Stale broker epoch not detected by the broker", staleBrokerEpochDetected)
  }

  private def sendAndVerifySuccessfulResponse(controllerChannelManager: ControllerChannelManager, apiKeys: ApiKeys,
                                              builder: AbstractControlRequest.Builder[_ <: AbstractControlRequest]): Unit = {
    @volatile var succeed = false
    controllerChannelManager.sendRequest(brokerId2, apiKeys, builder,
      response => {
        succeed = response.errorCounts().isEmpty ||
          (response.errorCounts().containsKey(Errors.NONE) && response.errorCounts().size() == 1)})
    TestUtils.waitUntilTrue(() => succeed, "Should receive response with no errors")
  }
}
