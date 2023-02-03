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
import org.apache.kafka.clients.admin.AlterConfigOp.OpType
import org.apache.kafka.clients.admin._
import org.apache.kafka.common.acl.{AclBinding, AclBindingFilter}
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.config.ConfigResource.Type
import org.apache.kafka.common.message.DescribeClusterRequestData
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.Errors._
import org.apache.kafka.common.quota.{ClientQuotaAlteration, ClientQuotaEntity, ClientQuotaFilter, ClientQuotaFilterComponent}
import org.apache.kafka.common.requests.{ApiError, DescribeClusterRequest, DescribeClusterResponse}
import org.apache.kafka.common.{Endpoint, TopicPartition, TopicPartitionInfo}
import org.apache.kafka.image.ClusterImage
import org.apache.kafka.metadata.BrokerState
import org.apache.kafka.server.authorizer._
import org.apache.kafka.server.common.MetadataVersion
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{Tag, Test, Timeout}
import org.slf4j.LoggerFactory

import java.io.File
import java.nio.file.{FileSystems, Path}
import java.{lang, util}
import java.util.concurrent.CompletionStage
import java.util.{Arrays, Collections, Optional, OptionalLong, Properties}
import scala.annotation.nowarn
import scala.collection.mutable
import scala.concurrent.ExecutionException
import scala.concurrent.duration.{FiniteDuration, MILLISECONDS, SECONDS}
import scala.jdk.CollectionConverters._

@Timeout(120)
@Tag("integration")
class KRaftClusterTest {
  val log = LoggerFactory.getLogger(classOf[KRaftClusterTest])
  val log2 = LoggerFactory.getLogger(classOf[KRaftClusterTest].getCanonicalName + "2")

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
  def testCreateClusterAndRestartNode(): Unit = {
    val cluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder().
        setNumBrokerNodes(1).
        setNumControllerNodes(1).build()).build()
    try {
      cluster.format()
      cluster.startup()
      val broker = cluster.brokers().values().iterator().next()
      broker.shutdown()
      broker.startup()
    } finally {
      cluster.close()
    }
  }

  @Test
  def testCreateClusterAndWaitForBrokerInRunningState(): Unit = {
    val cluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder().
        setNumBrokerNodes(1).
        setNumControllerNodes(1).build()).build()
    try {
      cluster.format()
      cluster.startup()
      TestUtils.waitUntilTrue(() => cluster.brokers().get(0).brokerState == BrokerState.RUNNING,
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
      TestUtils.waitUntilTrue(() => cluster.brokers().get(0).brokerState == BrokerState.RUNNING,
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
      TestUtils.waitUntilTrue(() => cluster.brokers().get(0).brokerState == BrokerState.RUNNING,
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
      TestUtils.waitUntilTrue(() => cluster.brokers().get(0).brokerState == BrokerState.RUNNING,
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

  @Test
  def testCreateClusterInvalidMetadataVersion(): Unit = {
    assertEquals("Bootstrap metadata versions before 3.3-IV0 are not supported. Can't load " +
      "metadata from testkit", assertThrows(classOf[RuntimeException], () => {
        new KafkaClusterTestKit.Builder(
          new TestKitNodes.Builder().
            setBootstrapMetadataVersion(MetadataVersion.IBP_2_7_IV0).
            setNumBrokerNodes(1).
            setNumControllerNodes(1).build()).build()
    }).getMessage)
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
      .filter(brokerServer => brokerServer.brokerState == BrokerState.RUNNING)

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
          () => admin.listPartitionReassignments().reassignments().get().isEmpty,
          "The reassignment never completed.")
        var currentMapping: Seq[Seq[Int]] = Seq()
        val expectedMapping = Seq(Seq(2, 1, 0), Seq(0, 1, 2), Seq(2, 3), Seq(3, 2, 0, 1))
        TestUtils.waitUntilTrue( () => {
          val topicInfoMap = admin.describeTopics(Collections.singleton("foo")).allTopicNames().get()
          if (topicInfoMap.containsKey("foo")) {
            currentMapping = translatePartitionInfoToSeq(topicInfoMap.get("foo").partitions())
            expectedMapping.equals(currentMapping)
          } else {
            false
          }
        }, "Timed out waiting for replica assignments for topic foo. " +
          s"Wanted: ${expectedMapping}. Got: ${currentMapping}")

        TestUtils.retry(60000) {
          checkReplicaManager(
            cluster,
            List(
              (0, List(true, true, false, true)),
              (1, List(true, true, false, true)),
              (2, List(true, true, true, true)),
              (3, List(false, false, true, true))
            )
          )
        }
      } finally {
        admin.close()
      }
    } finally {
      cluster.close()
    }
  }

  private def checkReplicaManager(cluster: KafkaClusterTestKit, expectedHosting: List[(Int, List[Boolean])]): Unit = {
    for ((brokerId, partitionsIsHosted) <- expectedHosting) {
      val broker = cluster.brokers().get(brokerId)

      for ((isHosted, partitionId) <- partitionsIsHosted.zipWithIndex) {
        val topicPartition = new TopicPartition("foo", partitionId)
        if (isHosted) {
          assertNotEquals(
            HostedPartition.None,
            broker.replicaManager.getPartition(topicPartition),
            s"topicPartition = $topicPartition"
          )
        } else {
          assertEquals(
            HostedPartition.None,
            broker.replicaManager.getPartition(topicPartition),
            s"topicPartition = $topicPartition"
          )
        }
      }
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
    expectedPresent.foreach(topicsNotFound.add)
    TestUtils.waitUntilTrue(() => {
      admin.listTopics().names().get().forEach(name => topicsNotFound.remove(name))
      extraTopics = admin.listTopics().names().get().asScala.filter(expectedAbsent.contains(_))
      topicsNotFound.isEmpty && extraTopics.isEmpty
    }, s"Failed to find topic(s): ${topicsNotFound.asScala} and NOT find topic(s): ${extraTopics}")
  }

  private def incrementalAlter(
    admin: Admin,
    changes: Seq[(ConfigResource, Seq[AlterConfigOp])]
  ): Seq[ApiError] = {
    val configs = new util.HashMap[ConfigResource, util.Collection[AlterConfigOp]]()
    changes.foreach {
      case (resource, ops) => configs.put(resource, ops.asJava)
    }
    val values = admin.incrementalAlterConfigs(configs).values()
    changes.map {
      case (resource, _) => try {
        values.get(resource).get()
        ApiError.NONE
      } catch {
        case e: ExecutionException => ApiError.fromThrowable(e.getCause)
        case t: Throwable => ApiError.fromThrowable(t)
      }
    }
  }

  private def validateConfigs(
    admin: Admin,
    expected: Map[ConfigResource, Seq[(String, String)]],
    exhaustive: Boolean = false
  ): Map[ConfigResource, util.Map[String, String]] = {
    val results = new mutable.HashMap[ConfigResource, util.Map[String, String]]()
    TestUtils.retry(60000) {
      try {
        val values = admin.describeConfigs(expected.keySet.asJava).values()
        results.clear()
        assertEquals(expected.keySet, values.keySet().asScala)
        expected.foreach {
          case (resource, pairs) =>
            val config = values.get(resource).get()
            val actual = new util.TreeMap[String, String]()
            val expected = new util.TreeMap[String, String]()
            config.entries().forEach {
              case entry =>
                actual.put(entry.name(), entry.value())
                if (!exhaustive) {
                  expected.put(entry.name(), entry.value())
                }
            }
            pairs.foreach {
              case (k, v) => expected.put(k, v)
            }
            assertEquals(expected, actual)
            results.put(resource, actual)
        }
      } catch {
        case t: Throwable =>
          log.warn(s"Unable to describeConfigs(${expected.keySet.asJava})", t)
          throw t
      }
    }
    results.toMap
  }

  @Test
  def testIncrementalAlterConfigs(): Unit = {
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
        assertEquals(Seq(ApiError.NONE), incrementalAlter(admin, Seq(
          (new ConfigResource(Type.BROKER, ""), Seq(
            new AlterConfigOp(new ConfigEntry("log.roll.ms", "1234567"), OpType.SET),
            new AlterConfigOp(new ConfigEntry("max.connections.per.ip", "6"), OpType.SET))))))
        validateConfigs(admin, Map(new ConfigResource(Type.BROKER, "") -> Seq(
          ("log.roll.ms", "1234567"),
          ("max.connections.per.ip", "6"))), true)

        admin.createTopics(Arrays.asList(
          new NewTopic("foo", 2, 3.toShort),
          new NewTopic("bar", 2, 3.toShort))).all().get()
        TestUtils.waitForAllPartitionsMetadata(cluster.brokers().values().asScala.toSeq, "foo", 2)
        TestUtils.waitForAllPartitionsMetadata(cluster.brokers().values().asScala.toSeq, "bar", 2)

        validateConfigs(admin, Map(new ConfigResource(Type.TOPIC, "bar") -> Seq()))

        assertEquals(Seq(ApiError.NONE,
            new ApiError(INVALID_CONFIG, "Unknown topic config name: not.a.real.topic.config"),
            new ApiError(UNKNOWN_TOPIC_OR_PARTITION, "The topic 'baz' does not exist.")),
          incrementalAlter(admin, Seq(
            (new ConfigResource(Type.TOPIC, "foo"), Seq(
              new AlterConfigOp(new ConfigEntry("segment.jitter.ms", "345"), OpType.SET))),
            (new ConfigResource(Type.TOPIC, "bar"), Seq(
              new AlterConfigOp(new ConfigEntry("not.a.real.topic.config", "789"), OpType.SET))),
            (new ConfigResource(Type.TOPIC, "baz"), Seq(
              new AlterConfigOp(new ConfigEntry("segment.jitter.ms", "678"), OpType.SET))))))

        validateConfigs(admin, Map(new ConfigResource(Type.TOPIC, "foo") -> Seq(
          ("segment.jitter.ms", "345"))))

        assertEquals(Seq(ApiError.NONE), incrementalAlter(admin, Seq(
          (new ConfigResource(Type.BROKER, "2"), Seq(
            new AlterConfigOp(new ConfigEntry("max.connections.per.ip", "7"), OpType.SET))))))

        validateConfigs(admin, Map(new ConfigResource(Type.BROKER, "2") -> Seq(
          ("max.connections.per.ip", "7"))))
      } finally {
        admin.close()
      }
    } finally {
      cluster.close()
    }
  }

  @Test
  def testSetLog4jConfigurations(): Unit = {
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
        Seq(log, log2).foreach(_.debug("setting log4j"))

        val broker2 = new ConfigResource(Type.BROKER_LOGGER, "2")
        val broker3 = new ConfigResource(Type.BROKER_LOGGER, "3")
        val initialLog4j = validateConfigs(admin, Map(broker2 -> Seq()))

        assertEquals(Seq(ApiError.NONE,
            new ApiError(INVALID_REQUEST, "APPEND operation is not allowed for the BROKER_LOGGER resource")),
          incrementalAlter(admin, Seq(
            (broker2, Seq(
              new AlterConfigOp(new ConfigEntry(log.getName, "TRACE"), OpType.SET),
              new AlterConfigOp(new ConfigEntry(log2.getName, "TRACE"), OpType.SET))),
            (broker3, Seq(
              new AlterConfigOp(new ConfigEntry(log.getName, "TRACE"), OpType.APPEND),
              new AlterConfigOp(new ConfigEntry(log2.getName, "TRACE"), OpType.APPEND))))))

        validateConfigs(admin, Map(broker2 -> Seq(
          (log.getName, "TRACE"),
          (log2.getName, "TRACE"))))

        assertEquals(Seq(ApiError.NONE,
          new ApiError(INVALID_REQUEST, "SUBTRACT operation is not allowed for the BROKER_LOGGER resource")),
          incrementalAlter(admin, Seq(
            (broker2, Seq(
              new AlterConfigOp(new ConfigEntry(log.getName, ""), OpType.DELETE),
              new AlterConfigOp(new ConfigEntry(log2.getName, ""), OpType.DELETE))),
            (broker3, Seq(
              new AlterConfigOp(new ConfigEntry(log.getName, "TRACE"), OpType.SUBTRACT),
              new AlterConfigOp(new ConfigEntry(log2.getName, "TRACE"), OpType.SUBTRACT))))))

        validateConfigs(admin, Map(broker2 -> Seq(
          (log.getName, initialLog4j(broker2).get(log.getName)),
          (log2.getName, initialLog4j(broker2).get(log2.getName)))))
      } finally {
        admin.close()
      }
    } finally {
      cluster.close()
    }
  }

  @nowarn("cat=deprecation") // Suppress warnings about using legacy alterConfigs
  def legacyAlter(
    admin: Admin,
    resources: Map[ConfigResource, Seq[ConfigEntry]]
  ): Seq[ApiError] = {
    val configs = new util.HashMap[ConfigResource, Config]()
    resources.foreach {
      case (resource, entries) => configs.put(resource, new Config(entries.asJava))
    }
    val values = admin.alterConfigs(configs).values()
    resources.map {
      case (resource, _) => try {
        values.get(resource).get()
        ApiError.NONE
      } catch {
        case e: ExecutionException => ApiError.fromThrowable(e.getCause)
        case t: Throwable => ApiError.fromThrowable(t)
      }
    }.toSeq
  }

  @Test
  def testLegacyAlterConfigs(): Unit = {
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
        val defaultBroker = new ConfigResource(Type.BROKER, "")

        assertEquals(Seq(ApiError.NONE), legacyAlter(admin, Map(defaultBroker -> Seq(
          new ConfigEntry("log.roll.ms", "1234567"),
          new ConfigEntry("max.connections.per.ip", "6")))))

        validateConfigs(admin, Map(defaultBroker -> Seq(
          ("log.roll.ms", "1234567"),
          ("max.connections.per.ip", "6"))), true)

        assertEquals(Seq(ApiError.NONE), legacyAlter(admin, Map(defaultBroker -> Seq(
          new ConfigEntry("log.roll.ms", "1234567")))))

        // Since max.connections.per.ip was left out of the previous legacyAlter, it is removed.
        validateConfigs(admin, Map(defaultBroker -> Seq(
          ("log.roll.ms", "1234567"))), true)

        admin.createTopics(Arrays.asList(
          new NewTopic("foo", 2, 3.toShort),
          new NewTopic("bar", 2, 3.toShort))).all().get()
        TestUtils.waitForAllPartitionsMetadata(cluster.brokers().values().asScala.toSeq, "foo", 2)
        TestUtils.waitForAllPartitionsMetadata(cluster.brokers().values().asScala.toSeq, "bar", 2)
        assertEquals(Seq(ApiError.NONE,
            new ApiError(INVALID_CONFIG, "Unknown topic config name: not.a.real.topic.config"),
            new ApiError(UNKNOWN_TOPIC_OR_PARTITION, "The topic 'baz' does not exist.")),
          legacyAlter(admin, Map(
            new ConfigResource(Type.TOPIC, "foo") -> Seq(
              new ConfigEntry("segment.jitter.ms", "345")),
            new ConfigResource(Type.TOPIC, "bar") -> Seq(
              new ConfigEntry("not.a.real.topic.config", "789")),
            new ConfigResource(Type.TOPIC, "baz") -> Seq(
              new ConfigEntry("segment.jitter.ms", "678")))))

        validateConfigs(admin, Map(new ConfigResource(Type.TOPIC, "foo") -> Seq(
          ("segment.jitter.ms", "345"))))

      } finally {
        admin.close()
      }
    } finally {
      cluster.close()
    }
  }

  private def clusterImage(
    cluster: KafkaClusterTestKit,
    brokerId: Int
  ): ClusterImage = {
    cluster.brokers().get(brokerId).metadataCache.currentImage().cluster()
  }

  private def brokerIsUnfenced(
    image: ClusterImage,
    brokerId: Int
  ): Boolean = {
    Option(image.brokers().get(brokerId)).exists(registration => !registration.fenced())
  }

  private def brokerIsAbsent(
    image: ClusterImage,
    brokerId: Int
  ): Boolean = {
    Option(image.brokers().get(brokerId)).isEmpty
  }

  @Test
  def testUnregisterBroker(): Unit = {
    val cluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder().
        setNumBrokerNodes(4).
        setNumControllerNodes(3).build()).build()
    try {
      cluster.format()
      cluster.startup()
      cluster.waitForReadyBrokers()
      TestUtils.waitUntilTrue(() => brokerIsUnfenced(clusterImage(cluster, 1), 0),
        "Timed out waiting for broker 0 to be unfenced.")
      cluster.brokers().get(0).shutdown()
      TestUtils.waitUntilTrue(() => !brokerIsUnfenced(clusterImage(cluster, 1), 0),
        "Timed out waiting for broker 0 to be fenced.")
      val admin = Admin.create(cluster.clientProperties())
      try {
        admin.unregisterBroker(0)
      } finally {
        admin.close()
      }
      TestUtils.waitUntilTrue(() => brokerIsAbsent(clusterImage(cluster, 1), 0),
        "Timed out waiting for broker 0 to be fenced.")
    } finally {
      cluster.close()
    }
  }

  def createAdminClient(cluster: KafkaClusterTestKit): Admin = {
    var props: Properties = null
    props = cluster.clientProperties()
    props.put(AdminClientConfig.CLIENT_ID_CONFIG, this.getClass.getName)
    Admin.create(props)
  }

  @Test
  def testDescribeQuorumRequestToBrokers() : Unit = {
    val cluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder().
        setNumBrokerNodes(4).
        setNumControllerNodes(3).build()).build()
    try {
      cluster.format
      cluster.startup
      for (i <- 0 to 3) {
        TestUtils.waitUntilTrue(() => cluster.brokers.get(i).brokerState == BrokerState.RUNNING,
          "Broker Never started up")
      }
      val admin = createAdminClient(cluster)
      try {
        val quorumState = admin.describeMetadataQuorum(new DescribeMetadataQuorumOptions)
        val quorumInfo = quorumState.quorumInfo.get()

        assertEquals(cluster.controllers.asScala.keySet, quorumInfo.voters.asScala.map(_.replicaId).toSet)
        assertTrue(cluster.controllers.asScala.keySet.contains(quorumInfo.leaderId),
          s"Leader ID ${quorumInfo.leaderId} was not a controller ID.")

        val (voters, voterResponseValid) =
          TestUtils.computeUntilTrue(
            admin.describeMetadataQuorum(new DescribeMetadataQuorumOptions)
              .quorumInfo().get().voters()
          ) { voters => voters.stream
            .allMatch(voter => (voter.logEndOffset > 0
              && voter.lastFetchTimestamp() != OptionalLong.empty()
              && voter.lastCaughtUpTimestamp() != OptionalLong.empty()))
          }

        assertTrue(voterResponseValid, s"At least one voter did not return the expected state within timeout." +
          s"The responses gathered for all the voters: ${voters.toString}")

        val (observers, observerResponseValid) =
          TestUtils.computeUntilTrue(
            admin.describeMetadataQuorum(new DescribeMetadataQuorumOptions)
              .quorumInfo().get().observers()
          ) { observers =>
            (
              cluster.brokers.asScala.keySet == observers.asScala.map(_.replicaId).toSet
                && observers.stream.allMatch(observer => (observer.logEndOffset > 0
                && observer.lastFetchTimestamp() != OptionalLong.empty()
                && observer.lastCaughtUpTimestamp() != OptionalLong.empty())))
          }

        assertTrue(observerResponseValid, s"At least one observer did not return the expected state within timeout." +
            s"The responses gathered for all the observers: ${observers.toString}")
      } finally {
        admin.close()
      }
    } finally {
      cluster.close()
    }
  }


  @Test
  def testUpdateMetadataVersion(): Unit = {
    val cluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder().
        setBootstrapMetadataVersion(MetadataVersion.MINIMUM_BOOTSTRAP_VERSION).
        setNumBrokerNodes(4).
        setNumControllerNodes(3).build()).build()
    try {
      cluster.format()
      cluster.startup()
      cluster.waitForReadyBrokers()
      val admin = Admin.create(cluster.clientProperties())
      try {
        admin.updateFeatures(
          Map(MetadataVersion.FEATURE_NAME ->
            new FeatureUpdate(MetadataVersion.latest().featureLevel(), FeatureUpdate.UpgradeType.UPGRADE)).asJava, new UpdateFeaturesOptions
        )
      } finally {
        admin.close()
      }
      TestUtils.waitUntilTrue(() => cluster.brokers().get(1).metadataCache.currentImage().features().metadataVersion().equals(MetadataVersion.latest()),
        "Timed out waiting for metadata version update.")
    } finally {
      cluster.close()
    }
  }

  @Test
  def testRemoteLogManagerInstantiation(): Unit = {
    val cluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder().
        setNumBrokerNodes(1).
        setNumControllerNodes(1).build())
      .setConfigProp(RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, true.toString)
      .setConfigProp(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP,
        "org.apache.kafka.server.log.remote.storage.NoOpRemoteLogMetadataManager")
      .setConfigProp(RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CLASS_NAME_PROP,
        "org.apache.kafka.server.log.remote.storage.NoOpRemoteStorageManager")
      .build()
    try {
      cluster.format()
      cluster.startup()
      cluster.brokers().forEach((_, server) => {
        server.remoteLogManager match {
          case Some(_) =>
          case None => fail("RemoteLogManager should be initialized")
        }
      })
    } finally {
      cluster.close()
    }
  }

  @Test
  def testSnapshotCount(): Unit = {
    val cluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder().
        setNumBrokerNodes(0).
        setNumControllerNodes(1).build())
      .setConfigProp("metadata.log.max.snapshot.interval.ms", "500")
      .setConfigProp("metadata.max.idle.interval.ms", "50") // Set this low to generate metadata
      .build()

    try {
      cluster.format()
      cluster.startup()
      def snapshotCounter(path: Path): Long = {
       path.toFile.listFiles((_: File, name: String) => {
          name.toLowerCase.endsWith("checkpoint")
        }).length
      }

      val metaLog = FileSystems.getDefault.getPath(cluster.controllers().get(3000).config.metadataLogDir, "__cluster_metadata-0")
      TestUtils.waitUntilTrue(() => { snapshotCounter(metaLog) > 0 }, "Failed to see at least one snapshot")
      Thread.sleep(500 * 10) // Sleep for 10 snapshot intervals
      val countAfterTenIntervals = snapshotCounter(metaLog)
      assertTrue(countAfterTenIntervals > 1, s"Expected to see at least one more snapshot, saw $countAfterTenIntervals")
      assertTrue(countAfterTenIntervals < 20, s"Did not expect to see more than twice as many snapshots as snapshot intervals, saw $countAfterTenIntervals")
    } finally {
      cluster.close()
    }
  }

  @Test
  def testAuthorizerFailureFoundInControllerStartup(): Unit = {
    val cluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder().
        setNumControllerNodes(3).build()).
      setConfigProp("authorizer.class.name", classOf[BadAuthorizer].getName).build()
    try {
      cluster.format()
      val exception = assertThrows(classOf[ExecutionException], () => cluster.startup())
      assertEquals("java.lang.IllegalStateException: test authorizer exception", exception.getMessage)
    } finally {
      cluster.close()
    }
  }
}

class BadAuthorizer() extends Authorizer {

  override def start(serverInfo: AuthorizerServerInfo): java.util.Map[Endpoint, _ <: CompletionStage[Void]] = {
    throw new IllegalStateException("test authorizer exception")
  }

  override def authorize(requestContext: AuthorizableRequestContext, actions: util.List[Action]): util.List[AuthorizationResult] = ???

  override def acls(filter: AclBindingFilter): lang.Iterable[AclBinding] = ???

  override def close(): Unit = {}

  override def configure(configs: util.Map[String, _]): Unit = {}

  override def createAcls(requestContext: AuthorizableRequestContext, aclBindings: util.List[AclBinding]): util.List[_ <: CompletionStage[AclCreateResult]] = ???

  override def deleteAcls(requestContext: AuthorizableRequestContext, aclBindingFilters: util.List[AclBindingFilter]): util.List[_ <: CompletionStage[AclDeleteResult]] = ???
}
