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

import kafka.log.UnifiedLog
import kafka.network.SocketServer
import kafka.server.IntegrationTestUtils.connectAndReceive
import kafka.testkit.{KafkaClusterTestKit, TestKitNodes}
import kafka.utils.TestUtils
import org.apache.commons.io.FileUtils
import org.apache.kafka.clients.admin.AlterConfigOp.OpType
import org.apache.kafka.clients.admin._
import org.apache.kafka.common.acl.{AclBinding, AclBindingFilter}
import org.apache.kafka.common.config.{ConfigException, ConfigResource}
import org.apache.kafka.common.config.ConfigResource.Type
import org.apache.kafka.common.errors.{InvalidPartitionsException, PolicyViolationException, UnsupportedVersionException}
import org.apache.kafka.common.message.DescribeClusterRequestData
import org.apache.kafka.common.metadata.{ConfigRecord, FeatureLevelRecord}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.Errors._
import org.apache.kafka.common.quota.ClientQuotaAlteration.Op
import org.apache.kafka.common.quota.{ClientQuotaAlteration, ClientQuotaEntity, ClientQuotaFilter, ClientQuotaFilterComponent}
import org.apache.kafka.common.requests.{ApiError, DescribeClusterRequest, DescribeClusterResponse}
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.{Cluster, Endpoint, Reconfigurable, TopicPartition, TopicPartitionInfo}
import org.apache.kafka.controller.{QuorumController, QuorumControllerIntegrationTestUtils}
import org.apache.kafka.image.ClusterImage
import org.apache.kafka.metadata.BrokerState
import org.apache.kafka.metadata.bootstrap.BootstrapMetadata
import org.apache.kafka.network.SocketServerConfigs
import org.apache.kafka.server.authorizer._
import org.apache.kafka.server.common.{ApiMessageAndVersion, MetadataVersion}
import org.apache.kafka.server.config.KRaftConfigs
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig
import org.apache.kafka.server.quota
import org.apache.kafka.server.quota.{ClientQuotaCallback, ClientQuotaType}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{Tag, Test, Timeout}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.slf4j.LoggerFactory

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{FileSystems, Files, Path}
import java.{lang, util}
import java.util.concurrent.{CompletableFuture, CompletionStage, ExecutionException, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger
import java.util.{Collections, Optional, OptionalLong, Properties}
import scala.annotation.nowarn
import scala.collection.mutable
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
  def testCreateClusterAndRestartBrokerNode(): Unit = {
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
  def testCreateClusterAndRestartControllerNode(): Unit = {
    val cluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder().
        setNumBrokerNodes(1).
        setNumControllerNodes(3).build()).build()
    try {
      cluster.format()
      cluster.startup()
      val controller = cluster.controllers().values().iterator().asScala.filter(_.controller.isActive).next()
      val port = controller.socketServer.boundPort(controller.config.controllerListeners.head.listenerName)

      // shutdown active controller
      controller.shutdown()
      // Rewrite The `listeners` config to avoid controller socket server init using different port
      val config = controller.sharedServer.controllerConfig.props
      config.asInstanceOf[util.HashMap[String,String]].put(SocketServerConfigs.LISTENERS_CONFIG, s"CONTROLLER://localhost:$port")
      controller.sharedServer.controllerConfig.updateCurrentConfig(new KafkaConfig(config))
      //  metrics will be set to null when closing a controller, so we should recreate it for testing
      controller.sharedServer.metrics = new Metrics()

      // restart controller
      controller.startup()
      TestUtils.waitUntilTrue(() => cluster.controllers().values().iterator().asScala.exists(_.controller.isActive), "Timeout waiting for new controller election")
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
                              expectCount: Int): util.Map[ClientQuotaEntity, util.Map[String, lang.Double]] = {
          val alterResult = admin.alterClientQuotas(Seq(new ClientQuotaAlteration(entity, quotas.asJava)).asJava)
          try {
            alterResult.all().get()
          } catch {
            case t: Throwable => fail("AlterClientQuotas request failed", t)
          }

          def describeOrFail(filter: ClientQuotaFilter): util.Map[ClientQuotaEntity, util.Map[String, lang.Double]] = {
            try {
              admin.describeClientQuotas(filter).entities().get()
            } catch {
              case t: Throwable => fail("DescribeClientQuotas request failed", t)
            }
          }

          val (describeResult, ok) = TestUtils.computeUntilTrue(describeOrFail(filter)) {
            results => results.getOrDefault(entity, util.Collections.emptyMap[String, lang.Double]()).size() == expectCount
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

        TestUtils.tryUntilNoAssertionError() {
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

  def setConsumerByteRate(
    admin: Admin,
    entity: ClientQuotaEntity,
    value: Long
  ): Unit = {
    admin.alterClientQuotas(Collections.singletonList(
      new ClientQuotaAlteration(entity, Collections.singletonList(
        new Op("consumer_byte_rate", value.doubleValue()))))).
        all().get()
  }

  def getConsumerByteRates(admin: Admin): Map[ClientQuotaEntity, Long] = {
    val allFilter = ClientQuotaFilter.contains(Collections.emptyList())
    val results = new util.HashMap[ClientQuotaEntity, Long]
    admin.describeClientQuotas(allFilter).entities().get().forEach {
      case (entity, entityMap) =>
        Option(entityMap.get("consumer_byte_rate")).foreach(value => results.put(entity, value.longValue()))
    }
    results.asScala.toMap
  }

  @Test
  def testDefaultClientQuotas(): Unit = {
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
        val defaultUser = new ClientQuotaEntity(Collections.singletonMap[String, String]("user", null))
        val bobUser = new ClientQuotaEntity(Collections.singletonMap[String, String]("user", "bob"))
        TestUtils.retry(30000) {
          assertEquals(Map(), getConsumerByteRates(admin))
        }
        setConsumerByteRate(admin, defaultUser, 100L)
        TestUtils.retry(30000) {
          assertEquals(Map(
              defaultUser -> 100L
            ), getConsumerByteRates(admin))
        }
        setConsumerByteRate(admin, bobUser, 1000L)
        TestUtils.retry(30000) {
          assertEquals(Map(
            defaultUser -> 100L,
            bobUser -> 1000L
          ), getConsumerByteRates(admin))
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
    val brokerPropertyOverrides: util.Map[Integer, util.Map[String, String]] = new util.HashMap[Integer, util.Map[String, String]]()
    Seq.range(0, 3).asJava.forEach(brokerId => {
      val props = new util.HashMap[String, String]()
      props.put(SocketServerConfigs.LISTENERS_CONFIG, "EXTERNAL://localhost:0")
      props.put(SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG, "EXTERNAL://localhost:0")
      brokerPropertyOverrides.put(brokerId, props)
    })

    val nodes = new TestKitNodes.Builder()
      .setNumControllerNodes(1)
      .setNumBrokerNodes(3)
      .setPerServerProperties(brokerPropertyOverrides)
      .build()

    doOnStartedKafkaCluster(nodes) { implicit cluster =>
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
    val brokerPropertyOverrides: util.Map[Integer, util.Map[String, String]] = new util.HashMap[Integer, util.Map[String, String]]()
    Seq.range(0, 3).asJava.forEach(brokerId => {
      val props = new util.HashMap[String, String]()
      props.put(SocketServerConfigs.LISTENERS_CONFIG, "EXTERNAL://localhost:0")
      props.put(SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG, s"EXTERNAL://advertised-host-$brokerId:${brokerId + 100}")
      brokerPropertyOverrides.put(brokerId, props)
    })

    val nodes = new TestKitNodes.Builder()
      .setNumControllerNodes(1)
      .setNumBrokerNodes(3)
      .setNumDisksPerBroker(1)
      .setPerServerProperties(brokerPropertyOverrides)
      .build()

    doOnStartedKafkaCluster(nodes) { implicit cluster =>
      sendDescribeClusterRequestToBoundPortUntilAllBrokersPropagated(cluster.nodes.externalListenerName, (15L, SECONDS))
        .nodes.values.forEach { broker =>
          assertEquals(s"advertised-host-${broker.id}", broker.host, "Did not advertise configured advertised host")
          assertEquals(broker.id + 100, broker.port, "Did not advertise configured advertised port")
        }
    }
  }

  @Test
  def testCreateClusterInvalidMetadataVersion(): Unit = {
    assertEquals("Bootstrap metadata.version before 3.3-IV0 are not supported. Can't load " +
      "metadata from testkit", assertThrows(classOf[RuntimeException], () => {
        new KafkaClusterTestKit.Builder(
          new TestKitNodes.Builder().
            setBootstrapMetadataVersion(MetadataVersion.IBP_2_7_IV0).
            setNumBrokerNodes(1).
            setNumControllerNodes(1).build()).build()
    }).getMessage)
  }

  private def doOnStartedKafkaCluster(nodes: TestKitNodes)
                                     (action: KafkaClusterTestKit => Unit): Unit = {
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
        assignments.put(0, util.Arrays.asList(0, 1, 2))
        assignments.put(1, util.Arrays.asList(1, 2, 3))
        assignments.put(2, util.Arrays.asList(2, 3, 0))
        assignments.put(3, util.Arrays.asList(3, 2, 1))
        val createTopicResult = admin.createTopics(Collections.singletonList(
          new NewTopic("foo", assignments)))
        createTopicResult.all().get()
        waitForTopicListing(admin, Seq("foo"), Seq())

        // Start some reassignments.
        assertEquals(Collections.emptyMap(), admin.listPartitionReassignments().reassignments().get())
        val reassignments = new util.HashMap[TopicPartition, Optional[NewPartitionReassignment]]
        reassignments.put(new TopicPartition("foo", 0),
          Optional.of(new NewPartitionReassignment(util.Arrays.asList(2, 1, 0))))
        reassignments.put(new TopicPartition("foo", 1),
          Optional.of(new NewPartitionReassignment(util.Arrays.asList(0, 1, 2))))
        reassignments.put(new TopicPartition("foo", 2),
          Optional.of(new NewPartitionReassignment(util.Arrays.asList(2, 3))))
        reassignments.put(new TopicPartition("foo", 3),
          Optional.of(new NewPartitionReassignment(util.Arrays.asList(3, 2, 0, 1))))
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
          s"Wanted: $expectedMapping. Got: $currentMapping")

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
    }, s"Failed to find topic(s): ${topicsNotFound.asScala} and NOT find topic(s): $extraTopics")
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
              entry =>
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
            new AlterConfigOp(new ConfigEntry("max.connections.per.ip", "60"), OpType.SET))))))
        validateConfigs(admin, Map(new ConfigResource(Type.BROKER, "") -> Seq(
          ("log.roll.ms", "1234567"),
          ("max.connections.per.ip", "60"))), exhaustive = true)

        admin.createTopics(util.Arrays.asList(
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
          ("max.connections.per.ip", "6"))), exhaustive = true)

        assertEquals(Seq(ApiError.NONE), legacyAlter(admin, Map(defaultBroker -> Seq(
          new ConfigEntry("log.roll.ms", "1234567")))))

        // Since max.connections.per.ip was left out of the previous legacyAlter, it is removed.
        validateConfigs(admin, Map(defaultBroker -> Seq(
          ("log.roll.ms", "1234567"))), exhaustive = true)

        admin.createTopics(util.Arrays.asList(
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

  @ParameterizedTest
  @ValueSource(strings = Array("3.7-IV0", "3.7-IV2"))
  def testCreatePartitions(metadataVersionString: String): Unit = {
    val cluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder().
        setNumBrokerNodes(4).
        setBootstrapMetadataVersion(MetadataVersion.fromVersionString(metadataVersionString)).
        setNumControllerNodes(3).build()).
      build()
    try {
      cluster.format()
      cluster.startup()
      cluster.waitForReadyBrokers()
      val admin = Admin.create(cluster.clientProperties())
      try {
        val createResults = admin.createTopics(util.Arrays.asList(
          new NewTopic("foo", 1, 3.toShort),
          new NewTopic("bar", 2, 3.toShort))).values()
        createResults.get("foo").get()
        createResults.get("bar").get()
        val increaseResults = admin.createPartitions(Map(
          "foo" -> NewPartitions.increaseTo(3),
          "bar" -> NewPartitions.increaseTo(2)).asJava).values()
        increaseResults.get("foo").get()
        assertEquals(classOf[InvalidPartitionsException], assertThrows(
          classOf[ExecutionException], () => increaseResults.get("bar").get()).getCause.getClass)
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
    Option(image.brokers().get(brokerId)) match {
      case None => false
      case Some(registration) => !registration.fenced()
    }
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
      cluster.format()
      cluster.startup()
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
                && observers.stream.allMatch(observer => observer.logEndOffset > 0
                && observer.lastFetchTimestamp() != OptionalLong.empty()
                && observer.lastCaughtUpTimestamp() != OptionalLong.empty()))
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
            new FeatureUpdate(MetadataVersion.latestTesting().featureLevel(), FeatureUpdate.UpgradeType.UPGRADE)).asJava, new UpdateFeaturesOptions
        )
      } finally {
        admin.close()
      }
      TestUtils.waitUntilTrue(() => cluster.brokers().get(1).metadataCache.currentImage().features().metadataVersion().equals(MetadataVersion.latestTesting()),
        "Timed out waiting for metadata.version update.")
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
        server.remoteLogManagerOpt match {
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
      TestUtils.waitUntilTrue(() => {
        val emitterMetrics = cluster.controllers().values().iterator().next().
          sharedServer.snapshotEmitter.metrics()
        emitterMetrics.latestSnapshotGeneratedBytes() > 0
      }, "Failed to see latestSnapshotGeneratedBytes > 0")
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
      cluster.fatalFaultHandler().setIgnore(true)
    } finally {
      cluster.close()
    }
  }

  /**
   * Test a single broker, single controller cluster at the minimum bootstrap level. This tests
   * that we can function without having periodic NoOpRecords written.
   */
  @Test
  def testSingleControllerSingleBrokerCluster(): Unit = {
    val cluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder().
        setBootstrapMetadataVersion(MetadataVersion.MINIMUM_BOOTSTRAP_VERSION).
        setNumBrokerNodes(1).
        setNumControllerNodes(1).build()).build()
    try {
      cluster.format()
      cluster.startup()
      cluster.waitForReadyBrokers()
    } finally {
      cluster.close()
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(false, true))
  def testReconfigureControllerClientQuotas(combinedController: Boolean): Unit = {
    val cluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder().
        setNumBrokerNodes(1).
        setCombined(combinedController).
        setNumControllerNodes(1).build()).
      setConfigProp("client.quota.callback.class", classOf[DummyClientQuotaCallback].getName).
      setConfigProp(DummyClientQuotaCallback.dummyClientQuotaCallbackValueConfigKey, "0").
      build()

    def assertConfigValue(expected: Int): Unit = {
      TestUtils.retry(60000) {
        assertEquals(expected, cluster.controllers().values().iterator().next().
          quotaManagers.clientQuotaCallback.get.asInstanceOf[DummyClientQuotaCallback].value)
        assertEquals(expected, cluster.brokers().values().iterator().next().
          quotaManagers.clientQuotaCallback.get.asInstanceOf[DummyClientQuotaCallback].value)
      }
    }

    try {
      cluster.format()
      cluster.startup()
      cluster.waitForReadyBrokers()
      assertConfigValue(0)
      val admin = Admin.create(cluster.clientProperties())
      try {
        admin.incrementalAlterConfigs(
          Collections.singletonMap(new ConfigResource(Type.BROKER, ""),
            Collections.singletonList(new AlterConfigOp(
              new ConfigEntry(DummyClientQuotaCallback.dummyClientQuotaCallbackValueConfigKey, "1"), OpType.SET)))).
          all().get()
      } finally {
        admin.close()
      }
      assertConfigValue(1)
    } finally {
      cluster.close()
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(false, true))
  def testReconfigureControllerAuthorizer(combinedMode: Boolean): Unit = {
    val cluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder().
        setNumBrokerNodes(1).
        setCombined(combinedMode).
        setNumControllerNodes(1).build()).
      setConfigProp("authorizer.class.name", classOf[FakeConfigurableAuthorizer].getName).
      build()

    def assertFoobarValue(expected: Int): Unit = {
      TestUtils.retry(60000) {
        assertEquals(expected, cluster.controllers().values().iterator().next().
          authorizer.get.asInstanceOf[FakeConfigurableAuthorizer].foobar.get())
        assertEquals(expected, cluster.brokers().values().iterator().next().
          authorizer.get.asInstanceOf[FakeConfigurableAuthorizer].foobar.get())
      }
    }

    try {
      cluster.format()
      cluster.startup()
      cluster.waitForReadyBrokers()
      assertFoobarValue(0)
      val admin = Admin.create(cluster.clientProperties())
      try {
        admin.incrementalAlterConfigs(
          Collections.singletonMap(new ConfigResource(Type.BROKER, ""),
            Collections.singletonList(new AlterConfigOp(
              new ConfigEntry(FakeConfigurableAuthorizer.foobarConfigKey, "123"), OpType.SET)))).
          all().get()
      } finally {
        admin.close()
      }
      assertFoobarValue(123)
    } finally {
      cluster.close()
    }
  }

  @Test
  def testOverlyLargeCreateTopics(): Unit = {
    val cluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder().
        setNumBrokerNodes(1).
        setNumControllerNodes(1).build()).build()
    try {
      cluster.format()
      cluster.startup()
      val admin = Admin.create(cluster.clientProperties())
      try {
        val newTopics = new util.ArrayList[NewTopic]()
        for (i <- 0 to 10000) {
          newTopics.add(new NewTopic("foo" + i, 100000, 1.toShort))
        }
        val executionException = assertThrows(classOf[ExecutionException],
            () => admin.createTopics(newTopics).all().get())
        assertNotNull(executionException.getCause)
        assertEquals(classOf[PolicyViolationException], executionException.getCause.getClass)
        assertEquals("Unable to perform excessively large batch operation.",
          executionException.getCause.getMessage)
      } finally {
        admin.close()
      }
    } finally {
      cluster.close()
    }
  }

  @Test
  def testTimedOutHeartbeats(): Unit = {
    val cluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder().
        setNumBrokerNodes(3).
        setNumControllerNodes(1).build()).
      setConfigProp(KRaftConfigs.BROKER_HEARTBEAT_INTERVAL_MS_CONFIG, 10.toString).
      setConfigProp(KRaftConfigs.BROKER_SESSION_TIMEOUT_MS_CONFIG, 1000.toString).
      build()
    try {
      cluster.format()
      cluster.startup()
      val controller = cluster.controllers().values().iterator().next()
      controller.controller.waitForReadyBrokers(3).get()
      TestUtils.retry(60000) {
        val latch = QuorumControllerIntegrationTestUtils.pause(controller.controller.asInstanceOf[QuorumController])
        Thread.sleep(1001)
        latch.countDown()
        assertEquals(0, controller.sharedServer.controllerServerMetrics.fencedBrokerCount())
        assertTrue(controller.quorumControllerMetrics.timedOutHeartbeats() > 0,
          "Expected timedOutHeartbeats to be greater than 0.")
      }
    } finally {
      cluster.close()
    }
  }

  @Test
  def testRegisteredControllerEndpoints(): Unit = {
    val cluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder().
        setNumBrokerNodes(1).
        setNumControllerNodes(3).build()).
      build()
    try {
      cluster.format()
      cluster.startup()
      TestUtils.retry(60000) {
        val controller = cluster.controllers().values().iterator().next()
        val registeredControllers = controller.registrationsPublisher.controllers()
        assertEquals(3, registeredControllers.size(), "Expected 3 controller registrations")
        registeredControllers.values().forEach(registration => {
          assertNotNull(registration.listeners.get("CONTROLLER"))
          assertNotEquals(0, registration.listeners.get("CONTROLLER").port())
        })
      }
    } finally {
      cluster.close()
    }
  }

  @Test
  def testDirectToControllerCommunicationFailsOnOlderMetadataVersion(): Unit = {
    val cluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder().
        setBootstrapMetadataVersion(MetadataVersion.IBP_3_6_IV2).
        setNumBrokerNodes(1).
        setNumControllerNodes(1).build()).
      build()
    try {
      cluster.format()
      cluster.startup()
      val admin = Admin.create(cluster.newClientPropertiesBuilder().
        setUsingBootstrapControllers(true).
        build())
      try {
        val exception = assertThrows(classOf[ExecutionException],
          () => admin.describeCluster().clusterId().get(1, TimeUnit.MINUTES))
        assertNotNull(exception.getCause)
        assertEquals(classOf[UnsupportedVersionException], exception.getCause.getClass)
      } finally {
        admin.close()
      }
    } finally {
      cluster.close()
    }
  }

  @Test
  def testStartupWithNonDefaultKControllerDynamicConfiguration(): Unit = {
    val bootstrapRecords = util.Arrays.asList(
      new ApiMessageAndVersion(new FeatureLevelRecord().
        setName(MetadataVersion.FEATURE_NAME).
        setFeatureLevel(MetadataVersion.IBP_3_7_IV0.featureLevel), 0.toShort),
      new ApiMessageAndVersion(new ConfigRecord().
        setResourceType(ConfigResource.Type.BROKER.id).
        setResourceName("").
        setName("num.io.threads").
        setValue("9"), 0.toShort))
    val cluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder().
        setBootstrapMetadata(BootstrapMetadata.fromRecords(bootstrapRecords, "testRecords")).
        setNumBrokerNodes(1).
        setNumControllerNodes(1).build()).
      build()
    try {
      cluster.format()
      cluster.startup()
      val controller = cluster.controllers().values().iterator().next()
      TestUtils.retry(60000) {
        assertNotNull(controller.controllerApisHandlerPool)
        assertEquals(9, controller.controllerApisHandlerPool.threadPoolSize.get())
      }
    } finally {
      cluster.close()
    }
  }

  @Test
  def testTopicDeletedAndRecreatedWhileBrokerIsDown(): Unit = {
    val cluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder().
        setBootstrapMetadataVersion(MetadataVersion.IBP_3_6_IV2).
        setNumBrokerNodes(3).
        setNumControllerNodes(1).build()).
      build()
    try {
      cluster.format()
      cluster.startup()
      val admin = Admin.create(cluster.clientProperties())
      try {
        val broker0 = cluster.brokers().get(0)
        val broker1 = cluster.brokers().get(1)
        val foo0 = new TopicPartition("foo", 0)

        admin.createTopics(util.Arrays.asList(
          new NewTopic("foo", 3, 3.toShort))).all().get()

        // Wait until foo-0 is created on broker0.
        TestUtils.retry(60000) {
          assertTrue(broker0.logManager.getLog(foo0).isDefined)
        }

        // Shut down broker0 and wait until the ISR of foo-0 is set to [1, 2]
        broker0.shutdown()
        TestUtils.retry(60000) {
          val info = broker1.metadataCache.getPartitionInfo("foo", 0)
          assertTrue(info.isDefined)
          assertEquals(Set(1, 2), info.get.isr().asScala.toSet)
        }

        // Modify foo-0 so that it has the wrong topic ID.
        val logDir = broker0.logManager.getLog(foo0).get.dir
        val partitionMetadataFile = new File(logDir, "partition.metadata")
        Files.write(partitionMetadataFile.toPath,
          "version: 0\ntopic_id: AAAAAAAAAAAAA7SrBWaJ7g\n".getBytes(StandardCharsets.UTF_8))

        // Start up broker0 and wait until the ISR of foo-0 is set to [0, 1, 2]
        broker0.startup()
        TestUtils.retry(60000) {
          val info = broker1.metadataCache.getPartitionInfo("foo", 0)
          assertTrue(info.isDefined)
          assertEquals(Set(0, 1, 2), info.get.isr().asScala.toSet)
        }
      } finally {
        admin.close()
      }
    } finally {
      cluster.close()
    }
  }

  @Test
  def testAbandonedFutureReplicaRecovered_mainReplicaInOfflineLogDir(): Unit = {
    val cluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder().
        setBootstrapMetadataVersion(MetadataVersion.IBP_3_7_IV2).
        setNumBrokerNodes(3).
        setNumDisksPerBroker(2).
        setNumControllerNodes(1).build()).
      build()
    try {
      cluster.format()
      cluster.startup()
      val admin = Admin.create(cluster.clientProperties())
      try {
        val broker0 = cluster.brokers().get(0)
        val broker1 = cluster.brokers().get(1)
        val foo0 = new TopicPartition("foo", 0)

        admin.createTopics(util.Arrays.asList(
          new NewTopic("foo", 3, 3.toShort))).all().get()

        // Wait until foo-0 is created on broker0.
        TestUtils.retry(60000) {
          assertTrue(broker0.logManager.getLog(foo0).isDefined)
        }

        // Shut down broker0 and wait until the ISR of foo-0 is set to [1, 2]
        broker0.shutdown()
        TestUtils.retry(60000) {
          val info = broker1.metadataCache.getPartitionInfo("foo", 0)
          assertTrue(info.isDefined)
          assertEquals(Set(1, 2), info.get.isr().asScala.toSet)
        }

        // Modify foo-0 so that it refers to a future replica.
        // This is equivalent to a failure during the promotion of the future replica and a restart with directory for
        // the main replica being offline
        val log = broker0.logManager.getLog(foo0).get
        log.renameDir(UnifiedLog.logFutureDirName(foo0), shouldReinitialize = false)

        // Start up broker0 and wait until the ISR of foo-0 is set to [0, 1, 2]
        broker0.startup()
        TestUtils.retry(60000) {
          val info = broker1.metadataCache.getPartitionInfo("foo", 0)
          assertTrue(info.isDefined)
          assertEquals(Set(0, 1, 2), info.get.isr().asScala.toSet)
          assertTrue(broker0.logManager.getLog(foo0, isFuture = true).isEmpty)
        }
      } finally {
        admin.close()
      }
    } finally {
      cluster.close()
    }
  }

  @Test
  def testAbandonedFutureReplicaRecovered_mainReplicaInOnlineLogDir(): Unit = {
    val cluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder().
        setBootstrapMetadataVersion(MetadataVersion.IBP_3_7_IV2).
        setNumBrokerNodes(3).
        setNumDisksPerBroker(2).
        setNumControllerNodes(1).build()).
      build()
    try {
      cluster.format()
      cluster.startup()
      val admin = Admin.create(cluster.clientProperties())
      try {
        val broker0 = cluster.brokers().get(0)
        val broker1 = cluster.brokers().get(1)
        val foo0 = new TopicPartition("foo", 0)

        admin.createTopics(util.Arrays.asList(
          new NewTopic("foo", 3, 3.toShort))).all().get()

        // Wait until foo-0 is created on broker0.
        TestUtils.retry(60000) {
          assertTrue(broker0.logManager.getLog(foo0).isDefined)
        }

        // Shut down broker0 and wait until the ISR of foo-0 is set to [1, 2]
        broker0.shutdown()
        TestUtils.retry(60000) {
          val info = broker1.metadataCache.getPartitionInfo("foo", 0)
          assertTrue(info.isDefined)
          assertEquals(Set(1, 2), info.get.isr().asScala.toSet)
        }

        val log = broker0.logManager.getLog(foo0).get

        // Copy foo-0 to targetParentDir
        // This is so that we can rename the main replica to a future down below
        val parentDir = log.parentDir
        val targetParentDir = broker0.config.logDirs.filter(_ != parentDir).head
        val targetDirFile = new File(targetParentDir, log.dir.getName)
        FileUtils.copyDirectory(log.dir, targetDirFile)
        assertTrue(targetDirFile.exists())

        // Rename original log to a future
        // This is equivalent to a failure during the promotion of the future replica and a restart with directory for
        // the main replica being online
        val originalLogFile = log.dir
        log.renameDir(UnifiedLog.logFutureDirName(foo0), shouldReinitialize = false)
        assertFalse(originalLogFile.exists())

        // Start up broker0 and wait until the ISR of foo-0 is set to [0, 1, 2]
        broker0.startup()
        TestUtils.retry(60000) {
          val info = broker1.metadataCache.getPartitionInfo("foo", 0)
          assertTrue(info.isDefined)
          assertEquals(Set(0, 1, 2), info.get.isr().asScala.toSet)
          assertTrue(broker0.logManager.getLog(foo0, isFuture = true).isEmpty)
          assertFalse(targetDirFile.exists())
          assertTrue(originalLogFile.exists())
        }
      } finally {
        admin.close()
      }
    } finally {
      cluster.close()
    }
  }
}

class BadAuthorizer extends Authorizer {

  override def start(serverInfo: AuthorizerServerInfo): util.Map[Endpoint, _ <: CompletionStage[Void]] = {
    throw new IllegalStateException("test authorizer exception")
  }

  override def authorize(requestContext: AuthorizableRequestContext, actions: util.List[Action]): util.List[AuthorizationResult] = ???

  override def acls(filter: AclBindingFilter): lang.Iterable[AclBinding] = ???

  override def close(): Unit = {}

  override def configure(configs: util.Map[String, _]): Unit = {}

  override def createAcls(requestContext: AuthorizableRequestContext, aclBindings: util.List[AclBinding]): util.List[_ <: CompletionStage[AclCreateResult]] = ???

  override def deleteAcls(requestContext: AuthorizableRequestContext, aclBindingFilters: util.List[AclBindingFilter]): util.List[_ <: CompletionStage[AclDeleteResult]] = ???
}

object DummyClientQuotaCallback {
  val dummyClientQuotaCallbackValueConfigKey = "dummy.client.quota.callback.value"
}

class DummyClientQuotaCallback extends ClientQuotaCallback with Reconfigurable {
  var value = 0
  override def quotaMetricTags(quotaType: ClientQuotaType, principal: KafkaPrincipal, clientId: String): util.Map[String, String] = Collections.emptyMap()

  override def quotaLimit(quotaType: ClientQuotaType, metricTags: util.Map[String, String]): lang.Double = 1.0

  override def updateQuota(quotaType: ClientQuotaType, quotaEntity: quota.ClientQuotaEntity, newValue: Double): Unit = {}

  override def removeQuota(quotaType: ClientQuotaType, quotaEntity: quota.ClientQuotaEntity): Unit = {}

  override def quotaResetRequired(quotaType: ClientQuotaType): Boolean = true

  override def updateClusterMetadata(cluster: Cluster): Boolean = false

  override def close(): Unit = {}

  override def configure(configs: util.Map[String, _]): Unit = {
    val newValue = configs.get(DummyClientQuotaCallback.dummyClientQuotaCallbackValueConfigKey)
    if (newValue != null) {
      value = Integer.parseInt(newValue.toString)
    }
  }

  override def reconfigurableConfigs(): util.Set[String] = Set(DummyClientQuotaCallback.dummyClientQuotaCallbackValueConfigKey).asJava

  override def validateReconfiguration(configs: util.Map[String, _]): Unit = {
  }

  override def reconfigure(configs: util.Map[String, _]): Unit = configure(configs)
}

object FakeConfigurableAuthorizer {
  val foobarConfigKey = "fake.configurable.authorizer.foobar.config"

  def fakeConfigurableAuthorizerConfigToInt(configs: util.Map[String, _]): Int = {
    val result = configs.get(foobarConfigKey)
    if (result == null) {
      0
    } else {
      val resultString = result.toString.trim()
      try {
        Integer.valueOf(resultString)
      } catch {
        case _: NumberFormatException => throw new ConfigException(s"Bad value of $foobarConfigKey: $resultString")
      }
    }
  }
}

class FakeConfigurableAuthorizer extends Authorizer with Reconfigurable {
  import FakeConfigurableAuthorizer._

  val foobar = new AtomicInteger(0)

  override def start(serverInfo: AuthorizerServerInfo): util.Map[Endpoint, _ <: CompletionStage[Void]] = {
    serverInfo.endpoints().asScala.map(e => e -> {
      val future = new CompletableFuture[Void]
      future.complete(null)
      future
    }).toMap.asJava
  }

  override def reconfigurableConfigs(): util.Set[String] = Set(foobarConfigKey).asJava

  override def validateReconfiguration(configs: util.Map[String, _]): Unit = {
    fakeConfigurableAuthorizerConfigToInt(configs)
  }

  override def reconfigure(configs: util.Map[String, _]): Unit = {
    foobar.set(fakeConfigurableAuthorizerConfigToInt(configs))
  }

  override def authorize(requestContext: AuthorizableRequestContext, actions: util.List[Action]): util.List[AuthorizationResult] = {
    actions.asScala.map(_ => AuthorizationResult.ALLOWED).toList.asJava
  }

  override def acls(filter: AclBindingFilter): lang.Iterable[AclBinding] = List[AclBinding]().asJava

  override def close(): Unit = {}

  override def configure(configs: util.Map[String, _]): Unit = {
    foobar.set(fakeConfigurableAuthorizerConfigToInt(configs))
  }

  override def createAcls(
    requestContext: AuthorizableRequestContext,
    aclBindings: util.List[AclBinding]
  ): util.List[_ <: CompletionStage[AclCreateResult]] = {
    Collections.emptyList()
  }

  override def deleteAcls(
    requestContext: AuthorizableRequestContext,
    aclBindingFilters: util.List[AclBindingFilter]
  ): util.List[_ <: CompletionStage[AclDeleteResult]] = {
    Collections.emptyList()
  }
}
