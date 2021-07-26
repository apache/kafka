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

package kafka.server

import kafka.network.SocketServer
import kafka.server.IntegrationTestUtils.connectAndReceive
import kafka.testkit.{BrokerNode, KafkaClusterTestKit, TestKitNodes}
import kafka.utils.TestUtils
import org.apache.kafka.clients.admin.{Admin, NewPartitionReassignment, NewTopic}
import org.apache.kafka.common.{TopicPartition, TopicPartitionInfo}
import org.apache.kafka.common.message.DescribeClusterRequestData
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.quota.{ClientQuotaAlteration, ClientQuotaEntity, ClientQuotaFilter, ClientQuotaFilterComponent}
import org.apache.kafka.common.requests.{DescribeClusterRequest, DescribeClusterResponse}
import org.apache.kafka.metadata.BrokerState
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{Tag, Test, Timeout}
import java.util
import java.util.{Arrays, Collections, Optional}

import scala.collection.mutable
import scala.concurrent.duration.{FiniteDuration, MILLISECONDS, SECONDS}
import scala.jdk.CollectionConverters._

@Timeout(120)
@Tag("integration")
class KRaftClusterTest {

  @Test
  def testCreateClusterAndClose(): Unit = {
    val cluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder().
        setNumBrokerNodes(1).
        setNumControllerNodes(1).build()).build()
    try {
      cluster.format()
      cluster.startup()
    } finally {
      cluster.close()
    }
  }

  @Test
  def testCreateClusterAndWaitForBrokerInRunningState(): Unit = {
    val cluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder().
        setNumBrokerNodes(3).
        setNumControllerNodes(3).build()).build()
    try {
      cluster.format()
      cluster.startup()
      TestUtils.waitUntilTrue(() => cluster.brokers().get(0).currentState() == BrokerState.RUNNING,
        "Broker never made it to RUNNING state.")
      TestUtils.waitUntilTrue(() => cluster.raftManagers().get(0).client.leaderAndEpoch().leaderId.isPresent,
        "RaftManager was not initialized.")
      val admin = Admin.create(cluster.clientProperties())
      try {
        assertEquals(cluster.nodes().clusterId().toString,
          admin.describeCluster().clusterId().get())
      } finally {
        admin.close()
      }
    } finally {
      cluster.close()
    }
  }

  @Test
  def testCreateClusterAndCreateListDeleteTopic(): Unit = {
    val cluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder().
        setNumBrokerNodes(3).
        setNumControllerNodes(3).build()).build()
    try {
      cluster.format()
      cluster.startup()
      cluster.waitForReadyBrokers()
      TestUtils.waitUntilTrue(() => cluster.brokers().get(0).currentState() == BrokerState.RUNNING,
        "Broker never made it to RUNNING state.")
      TestUtils.waitUntilTrue(() => cluster.raftManagers().get(0).client.leaderAndEpoch().leaderId.isPresent,
        "RaftManager was not initialized.")

      val admin = Admin.create(cluster.clientProperties())
      try {
        // Create a test topic
        val newTopic = Collections.singletonList(new NewTopic("test-topic", 1, 3.toShort))
        val createTopicResult = admin.createTopics(newTopic)
        createTopicResult.all().get()
        waitForTopicListing(admin, Seq("test-topic"), Seq())

        // Delete topic
        val deleteResult = admin.deleteTopics(Collections.singletonList("test-topic"))
        deleteResult.all().get()

        // List again
        waitForTopicListing(admin, Seq(), Seq("test-topic"))
      } finally {
        admin.close()
      }
    } finally {
      cluster.close()
    }
  }

  @Test
  def testCreateClusterAndCreateAndManyTopics(): Unit = {
    val cluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder().
        setNumBrokerNodes(3).
        setNumControllerNodes(3).build()).build()
    try {
      cluster.format()
      cluster.startup()
      cluster.waitForReadyBrokers()
      TestUtils.waitUntilTrue(() => cluster.brokers().get(0).currentState() == BrokerState.RUNNING,
        "Broker never made it to RUNNING state.")
      TestUtils.waitUntilTrue(() => cluster.raftManagers().get(0).client.leaderAndEpoch().leaderId.isPresent,
        "RaftManager was not initialized.")
      val admin = Admin.create(cluster.clientProperties())
      try {
        // Create many topics
        val newTopic = new util.ArrayList[NewTopic]()
        newTopic.add(new NewTopic("test-topic-1", 2, 3.toShort))
        newTopic.add(new NewTopic("test-topic-2", 2, 3.toShort))
        newTopic.add(new NewTopic("test-topic-3", 2, 3.toShort))
        val createTopicResult = admin.createTopics(newTopic)
        createTopicResult.all().get()

        // List created topics
        waitForTopicListing(admin, Seq("test-topic-1", "test-topic-2", "test-topic-3"), Seq())
      } finally {
        admin.close()
      }
    } finally {
      cluster.close()
    }
  }

  @Test
  def testClientQuotas(): Unit = {
    val cluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder().
        setNumBrokerNodes(1).
        setNumControllerNodes(1).build()).build()
    try {
      cluster.format()
      cluster.startup()
      TestUtils.waitUntilTrue(() => cluster.brokers().get(0).currentState() == BrokerState.RUNNING,
        "Broker never made it to RUNNING state.")
      val admin = Admin.create(cluster.clientProperties())
      try {
        val entity = new ClientQuotaEntity(Map("user" -> "testkit").asJava)
        var filter = ClientQuotaFilter.containsOnly(
          List(ClientQuotaFilterComponent.ofEntity("user", "testkit")).asJava)

        def alterThenDescribe(entity: ClientQuotaEntity,
                              quotas: Seq[ClientQuotaAlteration.Op],
                              filter: ClientQuotaFilter,
                              expectCount: Int): java.util.Map[ClientQuotaEntity, java.util.Map[String, java.lang.Double]] = {
          val alterResult = admin.alterClientQuotas(Seq(new ClientQuotaAlteration(entity, quotas.asJava)).asJava)
          try {
            alterResult.all().get()
          } catch {
            case t: Throwable => fail("AlterClientQuotas request failed", t)
          }

          def describeOrFail(filter: ClientQuotaFilter): java.util.Map[ClientQuotaEntity, java.util.Map[String, java.lang.Double]] = {
            try {
              admin.describeClientQuotas(filter).entities().get()
            } catch {
              case t: Throwable => fail("DescribeClientQuotas request failed", t)
            }
          }

          val (describeResult, ok) = TestUtils.computeUntilTrue(describeOrFail(filter)) {
            results => results.getOrDefault(entity, java.util.Collections.emptyMap[String, java.lang.Double]()).size() == expectCount
          }
          assertTrue(ok, "Broker never saw new client quotas")
          describeResult
        }

        var describeResult = alterThenDescribe(entity,
          Seq(new ClientQuotaAlteration.Op("request_percentage", 0.99)), filter, 1)
        assertEquals(0.99, describeResult.get(entity).get("request_percentage"), 1e-6)

        describeResult = alterThenDescribe(entity, Seq(
          new ClientQuotaAlteration.Op("request_percentage", 0.97),
          new ClientQuotaAlteration.Op("producer_byte_rate", 10000),
          new ClientQuotaAlteration.Op("consumer_byte_rate", 10001)
        ), filter, 3)
        assertEquals(0.97, describeResult.get(entity).get("request_percentage"), 1e-6)
        assertEquals(10000.0, describeResult.get(entity).get("producer_byte_rate"), 1e-6)
        assertEquals(10001.0, describeResult.get(entity).get("consumer_byte_rate"), 1e-6)

        describeResult = alterThenDescribe(entity, Seq(
          new ClientQuotaAlteration.Op("request_percentage", 0.95),
          new ClientQuotaAlteration.Op("producer_byte_rate", null),
          new ClientQuotaAlteration.Op("consumer_byte_rate", null)
        ), filter, 1)
        assertEquals(0.95, describeResult.get(entity).get("request_percentage"), 1e-6)

        describeResult = alterThenDescribe(entity, Seq(
          new ClientQuotaAlteration.Op("request_percentage", null)), filter, 0)

        describeResult = alterThenDescribe(entity,
          Seq(new ClientQuotaAlteration.Op("producer_byte_rate", 9999)), filter, 1)
        assertEquals(9999.0, describeResult.get(entity).get("producer_byte_rate"), 1e-6)

        // Add another quota for a different entity with same user part
        val entity2 = new ClientQuotaEntity(Map("user" -> "testkit", "client-id" -> "some-client").asJava)
        filter = ClientQuotaFilter.containsOnly(
          List(
            ClientQuotaFilterComponent.ofEntity("user", "testkit"),
            ClientQuotaFilterComponent.ofEntity("client-id", "some-client"),
          ).asJava)
        describeResult = alterThenDescribe(entity2,
          Seq(new ClientQuotaAlteration.Op("producer_byte_rate", 9998)), filter, 1)
        assertEquals(9998.0, describeResult.get(entity2).get("producer_byte_rate"), 1e-6)

        // non-strict match
        filter = ClientQuotaFilter.contains(
          List(ClientQuotaFilterComponent.ofEntity("user", "testkit")).asJava)

        TestUtils.tryUntilNoAssertionError(){
          val results = admin.describeClientQuotas(filter).entities().get()
          assertEquals(2, results.size(), "Broker did not see two client quotas")
          assertEquals(9999.0, results.get(entity).get("producer_byte_rate"), 1e-6)
          assertEquals(9998.0, results.get(entity2).get("producer_byte_rate"), 1e-6)
        }
      } finally {
        admin.close()
      }
    } finally {
      cluster.close()
    }
  }

  @Test
  def testCreateClusterWithAdvertisedPortZero(): Unit = {
    val brokerPropertyOverrides: (TestKitNodes, BrokerNode) => Map[String, String] = (nodes, _) => Map(
      (KafkaConfig.ListenersProp, s"${nodes.externalListenerName.value}://localhost:0"),
      (KafkaConfig.AdvertisedListenersProp, s"${nodes.externalListenerName.value}://localhost:0"))

    doOnStartedKafkaCluster(numBrokerNodes = 3, brokerPropertyOverrides = brokerPropertyOverrides) { implicit cluster =>
      sendDescribeClusterRequestToBoundPortUntilAllBrokersPropagated(cluster.nodes.externalListenerName, (15L, SECONDS))
        .nodes.values.forEach { broker =>
          assertEquals("localhost", broker.host,
            "Did not advertise configured advertised host")
          assertEquals(cluster.brokers.get(broker.id).socketServer.boundPort(cluster.nodes.externalListenerName), broker.port,
            "Did not advertise bound socket port")
        }
    }
  }

  @Test
  def testCreateClusterWithAdvertisedHostAndPortDifferentFromSocketServer(): Unit = {
    val brokerPropertyOverrides: (TestKitNodes, BrokerNode) => Map[String, String] = (nodes, broker) => Map(
      (KafkaConfig.ListenersProp, s"${nodes.externalListenerName.value}://localhost:0"),
      (KafkaConfig.AdvertisedListenersProp, s"${nodes.externalListenerName.value}://advertised-host-${broker.id}:${broker.id + 100}"))

    doOnStartedKafkaCluster(numBrokerNodes = 3, brokerPropertyOverrides = brokerPropertyOverrides) { implicit cluster =>
      sendDescribeClusterRequestToBoundPortUntilAllBrokersPropagated(cluster.nodes.externalListenerName, (15L, SECONDS))
        .nodes.values.forEach { broker =>
          assertEquals(s"advertised-host-${broker.id}", broker.host, "Did not advertise configured advertised host")
          assertEquals(broker.id + 100, broker.port, "Did not advertise configured advertised port")
        }
    }
  }

  private def doOnStartedKafkaCluster(numControllerNodes: Int = 1,
                                      numBrokerNodes: Int,
                                      brokerPropertyOverrides: (TestKitNodes, BrokerNode) => Map[String, String])
                                     (action: KafkaClusterTestKit => Unit): Unit = {
    val nodes = new TestKitNodes.Builder()
      .setNumControllerNodes(numControllerNodes)
      .setNumBrokerNodes(numBrokerNodes)
      .build()
    nodes.brokerNodes.values.forEach {
      broker => broker.propertyOverrides.putAll(brokerPropertyOverrides(nodes, broker).asJava)
    }
    val cluster = new KafkaClusterTestKit.Builder(nodes).build()
    try {
      cluster.format()
      cluster.startup()
      action(cluster)
    } finally {
      cluster.close()
    }
  }

  private def sendDescribeClusterRequestToBoundPortUntilAllBrokersPropagated(listenerName: ListenerName,
                                                                             waitTime: FiniteDuration)
                                                                            (implicit cluster: KafkaClusterTestKit): DescribeClusterResponse = {
    val startTime = System.currentTimeMillis
    val runningBrokerServers = waitForRunningBrokers(1, waitTime)
    val remainingWaitTime = waitTime - (System.currentTimeMillis - startTime, MILLISECONDS)
    sendDescribeClusterRequestToBoundPortUntilBrokersPropagated(
      runningBrokerServers.head, listenerName,
      cluster.nodes.brokerNodes.size, remainingWaitTime)
  }

  private def waitForRunningBrokers(count: Int, waitTime: FiniteDuration)
                                   (implicit cluster: KafkaClusterTestKit): Seq[BrokerServer] = {
    def getRunningBrokerServers: Seq[BrokerServer] = cluster.brokers.values.asScala.toSeq
      .filter(brokerServer => brokerServer.currentState() == BrokerState.RUNNING)

    val (runningBrokerServers, hasRunningBrokers) = TestUtils.computeUntilTrue(getRunningBrokerServers, waitTime.toMillis)(_.nonEmpty)
    assertTrue(hasRunningBrokers,
      s"After ${waitTime.toMillis} ms at least $count broker(s) should be in RUNNING state, " +
        s"but only ${runningBrokerServers.size} broker(s) are.")
    runningBrokerServers
  }

  private def sendDescribeClusterRequestToBoundPortUntilBrokersPropagated(destination: BrokerServer,
                                                                          listenerName: ListenerName,
                                                                          expectedBrokerCount: Int,
                                                                          waitTime: FiniteDuration): DescribeClusterResponse = {
    val (describeClusterResponse, metadataUpToDate) = TestUtils.computeUntilTrue(
      compute = sendDescribeClusterRequestToBoundPort(destination.socketServer, listenerName),
      waitTime = waitTime.toMillis
    ) {
      response => response.nodes.size == expectedBrokerCount
    }

    assertTrue(metadataUpToDate,
      s"After ${waitTime.toMillis} ms Broker is only aware of ${describeClusterResponse.nodes.size} brokers, " +
        s"but $expectedBrokerCount are expected.")

    describeClusterResponse
  }

  private def sendDescribeClusterRequestToBoundPort(destination: SocketServer,
                                                    listenerName: ListenerName): DescribeClusterResponse =
    connectAndReceive[DescribeClusterResponse](
      request = new DescribeClusterRequest.Builder(new DescribeClusterRequestData()).build(),
      destination = destination,
      listenerName = listenerName
    )

  @Test
  def testCreateClusterAndPerformReassignment(): Unit = {
    val cluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder().
        setNumBrokerNodes(4).
        setNumControllerNodes(3).build()).build()
    try {
      cluster.format()
      cluster.startup()
      cluster.waitForReadyBrokers()
      val admin = Admin.create(cluster.clientProperties())
      try {
        // Create the topic.
        val assignments = new util.HashMap[Integer, util.List[Integer]]
        assignments.put(0, Arrays.asList(0, 1, 2))
        assignments.put(1, Arrays.asList(1, 2, 3))
        assignments.put(2, Arrays.asList(2, 3, 0))
        assignments.put(3, Arrays.asList(3, 2, 1))
        val createTopicResult = admin.createTopics(Collections.singletonList(
          new NewTopic("foo", assignments)))
        createTopicResult.all().get()
        waitForTopicListing(admin, Seq("foo"), Seq())

        // Start some reassignments.
        assertEquals(Collections.emptyMap(), admin.listPartitionReassignments().reassignments().get())
        val reassignments = new util.HashMap[TopicPartition, Optional[NewPartitionReassignment]]
        reassignments.put(new TopicPartition("foo", 0),
          Optional.of(new NewPartitionReassignment(Arrays.asList(2, 1, 0))))
        reassignments.put(new TopicPartition("foo", 1),
          Optional.of(new NewPartitionReassignment(Arrays.asList(0, 1, 2))))
        reassignments.put(new TopicPartition("foo", 2),
          Optional.of(new NewPartitionReassignment(Arrays.asList(2, 3))))
        reassignments.put(new TopicPartition("foo", 3),
          Optional.of(new NewPartitionReassignment(Arrays.asList(3, 2, 0, 1))))
        admin.alterPartitionReassignments(reassignments).all().get()
        TestUtils.waitUntilTrue(
          () => admin.listPartitionReassignments().reassignments().get().isEmpty(),
          "The reassignment never completed.")
        var currentMapping: Seq[Seq[Int]] = Seq()
        val expectedMapping = Seq(Seq(2, 1, 0), Seq(0, 1, 2), Seq(2, 3), Seq(3, 2, 0, 1))
        TestUtils.waitUntilTrue( () => {
          val topicInfoMap = admin.describeTopics(Collections.singleton("foo")).all().get()
          if (topicInfoMap.containsKey("foo")) {
            currentMapping = translatePartitionInfoToSeq(topicInfoMap.get("foo").partitions())
            expectedMapping.equals(currentMapping)
          } else {
            false
          }
        }, "Timed out waiting for replica assignments for topic foo. " +
          s"Wanted: ${expectedMapping}. Got: ${currentMapping}")
      } finally {
        admin.close()
      }
    } finally {
      cluster.close()
    }
  }

  private def translatePartitionInfoToSeq(partitions: util.List[TopicPartitionInfo]): Seq[Seq[Int]] = {
    partitions.asScala.map(partition => partition.replicas().asScala.map(_.id()).toSeq).toSeq
  }

  private def waitForTopicListing(admin: Admin,
                                  expectedPresent: Seq[String],
                                  expectedAbsent: Seq[String]): Unit = {
    val topicsNotFound = new util.HashSet[String]
    var extraTopics: mutable.Set[String] = null
    expectedPresent.foreach(topicsNotFound.add(_))
    TestUtils.waitUntilTrue(() => {
      admin.listTopics().names().get().forEach(name => topicsNotFound.remove(name))
      extraTopics = admin.listTopics().names().get().asScala.filter(expectedAbsent.contains(_))
      topicsNotFound.isEmpty && extraTopics.isEmpty
    }, s"Failed to find topic(s): ${topicsNotFound.asScala} and NOT find topic(s): ${extraTopics}")
  }
}
