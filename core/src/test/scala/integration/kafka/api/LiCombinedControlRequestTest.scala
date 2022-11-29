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

package integration.kafka.api

import com.yammer.metrics.core.Histogram
import kafka.integration.KafkaServerTestHarness
import kafka.metrics.KafkaYammerMetrics
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.{Logging, TestUtils}
import org.apache.kafka.clients.admin.{Admin, AdminClient, AdminClientConfig}
import org.apache.kafka.common.errors.StaleBrokerEpochException
import org.apache.kafka.common.{Node, Uuid}
import org.apache.kafka.common.message.LiCombinedControlRequestData
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.LiCombinedControlRequest
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}

import java.util.Properties
import java.util
import scala.jdk.CollectionConverters.mapAsScalaMapConverter

/**
 * This class is used to test the LiCombinedControlRequest when the feature is enabled and disabled via
 * the zk-based dynamic config mechanism
 */
class LiCombinedControlRequestTest extends KafkaServerTestHarness  with Logging {
  val numNodes = 2
  val overridingProps = new Properties()
  private var adminClient: Admin = null
  override def generateConfigs = TestUtils.createBrokerConfigs(numNodes, zkConnect)
    .map(KafkaConfig.fromProps(_, overridingProps))

  @BeforeEach
  override def setUp(): Unit = {
    super.setUp()
    adminClient = createAdminClient()
  }

  @AfterEach
  override def tearDown(): Unit = {
    adminClient.close()
    super.tearDown()
  }


  @Test
  def testChangingLiCombinedControlRequestFlag(): Unit = {
    // turn on the feature by setting the /li_combined_control_request_flag to true
    val props = new Properties
    props.put(KafkaConfig.LiCombinedControlRequestEnableProp, "true")
    reconfigureServers(props, perBrokerConfig = false, (KafkaConfig.LiCombinedControlRequestEnableProp, "true"))

    // Each RequestSendThread is already blocked waiting for either a regular request or a combined request.
    // Suppose the next 3 requests coming out of the blocking queue are R1, R2 and R3.
    // After the flag is turned on, R1 will still be sent as a regular request. But all requests starting from R2
    // will be sent using the LiCombinedControlRequest. Thus we need to start measuring after generating some event
    // to pass the R1 phase. Below we create one more topic in order to pass the R1 phase.
    createTopic("topic2")
    assertTrue(createTopicAndGetCombinedRequestCount(Set(3, 4).map("topic" + _)) > 0)

    // turn off the feature now
    props.put(KafkaConfig.LiCombinedControlRequestEnableProp, "false")
    reconfigureServers(props, perBrokerConfig = false, (KafkaConfig.LiCombinedControlRequestEnableProp, "false"))
    // again we create one more topic to pass the R1 phase, as explained in the comment above.
    createTopic("topic5")

    // when the request merging feature is turned off, creating more topics won't cause the following metric
    // to increase any more
    val combinedRequestsSent2 = createTopicAndGetCombinedRequestCount(Set(6, 7).map("topic" + _))
    val combinedRequestsSent3 = createTopicAndGetCombinedRequestCount(Set(8, 9).map("topic" + _))
    assertTrue(combinedRequestsSent2 > 0 && combinedRequestsSent3 > 0)
    assertEquals(combinedRequestsSent2, combinedRequestsSent3)
  }

  @Test
  def testLiCombinedControlResponseV1(): Unit = {
    val topic1Uuid = Uuid.randomUuid()
    val leaderAndIsrPartitionStates = new util.ArrayList[LiCombinedControlRequestData.LeaderAndIsrPartitionState]()
    leaderAndIsrPartitionStates.add(new LiCombinedControlRequestData.LeaderAndIsrPartitionState().setTopicName("topic1")
      .setPartitionIndex(1))

    val topic2Uuid = Uuid.randomUuid()
    val updateMetadataPartitionStates = new util.ArrayList[LiCombinedControlRequestData.UpdateMetadataPartitionState]()
    updateMetadataPartitionStates.add(new LiCombinedControlRequestData.UpdateMetadataPartitionState().setTopicName("topic2")
      .setPartitionIndex(2))

    val topic3Uuid = Uuid.randomUuid()
    val stopReplicaPartitionStates = new util.ArrayList[LiCombinedControlRequestData.StopReplicaPartitionState]()
    stopReplicaPartitionStates.add(new LiCombinedControlRequestData.StopReplicaPartitionState().setTopicName("topic3")
      .setPartitionIndex(3))
    val topicIds = new util.HashMap[String, Uuid]()
    topicIds.put("topic1", topic1Uuid)
    topicIds.put("topic2", topic2Uuid)
    topicIds.put("topic3", topic3Uuid)

    val liCombinedControlRequest = new LiCombinedControlRequest.Builder(1, 0, 0, leaderAndIsrPartitionStates,
      new util.ArrayList[Node](), updateMetadataPartitionStates, new util.ArrayList[LiCombinedControlRequestData.UpdateMetadataBroker](),
      stopReplicaPartitionStates, topicIds).build()

    val errorResponse = liCombinedControlRequest.getErrorResponse(0, new StaleBrokerEpochException("stale broker"))

    val expectedError = Errors.STALE_BROKER_EPOCH.code
    // the per partition error should only be used for version 0, which means it should be empty for in the v1 case
    assertTrue(errorResponse.leaderAndIsrPartitionErrors().isEmpty)
    // check the topic level error is set
    assertEquals(expectedError, errorResponse.leaderAndIsrErrorCode())
    assertEquals(1, errorResponse.leaderAndIsrTopicErrors().size())
    assertEquals(1, errorResponse.leaderAndIsrTopicErrors().find(topic1Uuid).partitionErrors().get(0).partitionIndex())

    assertEquals(expectedError, errorResponse.updateMetadataErrorCode())

    assertEquals(expectedError, errorResponse.stopReplicaErrorCode())
    assertEquals(1, errorResponse.stopReplicaPartitionErrors().size())
    val stopReplicaPartitionError = errorResponse.stopReplicaPartitionErrors().get(0)
    assertEquals(expectedError, stopReplicaPartitionError.errorCode())
    assertEquals("topic3", stopReplicaPartitionError.topicName())
    assertEquals(3, stopReplicaPartitionError.partitionIndex())
  }

  private def createAdminClient(): Admin = {
    val config = new Properties()
    val bootstrapServers = TestUtils.bootstrapServers(servers, ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT))
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    AdminClient.create(config)
  }

  private def waitForConfigOnServer(server: KafkaServer, propName: String, propValue: String, maxWaitMs: Long): Unit = {
    TestUtils.retry(maxWaitMs) {
      assertEquals(propValue, server.config.originals.get(propName))
    }
  }

  private def waitForConfig(propName: String, propValue: String, maxWaitMs: Long = 10000): Unit = {
    servers.foreach { server => waitForConfigOnServer(server, propName, propValue, maxWaitMs) }
  }

  private def reconfigureServers(newProps: Properties, perBrokerConfig: Boolean, aPropToVerify: (String, String)): Unit = {
    val alterResult = TestUtils.incrementalAlterConfigs(servers, adminClient, newProps, perBrokerConfig)
    alterResult.all.get
    waitForConfig(aPropToVerify._1, aPropToVerify._2)
  }

  def createTopicAndGetCombinedRequestCount(topicsToCreate: Set[String]) = {
    for (topic <- topicsToCreate) {
      createTopic(topic, 1, 1)
    }

    val metrics = KafkaYammerMetrics.defaultRegistry.allMetrics.asScala.filter { case (n, metric) =>
      n.getMBeanName.contains("name=brokerRequestRemoteTimeMs,request=LI_COMBINED_CONTROL")
    }

    if (metrics.nonEmpty) {
      assertTrue(metrics.size == 1)
      val metric = metrics.values.head

      metric.asInstanceOf[Histogram].count()
    } else {
      0
    }
  }
}

