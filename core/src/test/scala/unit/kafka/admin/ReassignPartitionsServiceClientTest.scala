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


package unit.kafka.admin

import java.util.concurrent.ExecutionException
import java.util.{Collections, Properties}

import kafka.admin.{AdminClientReassignCommandService, BrokerMetadata, ZkClientReassignCommandService}
import kafka.server.{ConfigType, KafkaServer}
import kafka.utils.{Logging, TestUtils}
import kafka.zk.{AdminZkClient, KafkaZkClient, ZooKeeperTestHarness}
import org.apache.kafka.clients.admin.{Admin, MockAdminClient}
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException
import org.apache.kafka.common.{Node, TopicPartition, TopicPartitionInfo}
import org.easymock.EasyMock._
import org.junit.{After, Test}
import org.junit.Assert.{assertEquals, assertFalse}

import scala.collection.Seq
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

/*
 * Validate methods of the ZkServiceClient used by the ReassignPartitionsCommand.
 * Many methods are just passthroughs, and are not tested (presumed to be tested elsewhere).
 * Any method with "real" logic should be validated.
 */
class ReassignPartitionsServiceClientZkTest extends ZooKeeperTestHarness with Logging {
  var servers: Seq[KafkaServer] = Seq()
  var calls = 0
  val TestTopic1Name = "test1"

  @After
  override def tearDown(): Unit = {
    TestUtils.shutdownServers(servers)
    super.tearDown()
  }

  // Test update topic configs and broker configs
  @Test
  def testUpdateShouldOverrideEmptyTopicConfigs() : Unit = {
    val configUpdatesMap = Map("property1" -> "0", "property2" -> "1")

    // Test: Have empty properties, this should just set the ones we expect.
    val test1Properties = new Properties()
    configUpdatesMap.foreach({ case (k, v) => test1Properties.setProperty(k: String, v: String) })
    val mockZkClient: KafkaZkClient = createMock(classOf[KafkaZkClient])
    val mockAdminZkClient: AdminZkClient = createMock(classOf[AdminZkClient])
    expect(mockAdminZkClient.fetchEntityConfig(ConfigType.Topic, TestTopic1Name)).andReturn(new Properties)
    expect(mockAdminZkClient.changeTopicConfig(TestTopic1Name, test1Properties))
    replay(mockZkClient)
    replay(mockAdminZkClient)

    val service = new ZkClientReassignCommandService(mockZkClient, Some(mockAdminZkClient))

    service.updateTopicConfigs(TestTopic1Name, configUpdatesMap)
  }

  @Test
  def testUpdateShouldNotOverrideOtherTopicConfigs() : Unit = {
    val configUpdatesMap = Map("property1" -> "0", "property2" -> "1")

    // Test: Hove other Properties that are completely disjoint from the ones we're updating.
    val mockZkClient: KafkaZkClient = createMock(classOf[KafkaZkClient])
    val mockAdminZkClient: AdminZkClient = createMock(classOf[AdminZkClient])
    val test2Properties = new Properties
    val test2XtraPropertiesMap = Map("otherProp1" -> "otherVal1", "otherProp2" -> "otherVal2")
    for ((k, v) <- test2XtraPropertiesMap) {
      test2Properties.setProperty(k, v)
    }
    val test2ExpectedPropertiesMap = configUpdatesMap ++ test2XtraPropertiesMap
    val test2ExpectedProperties = new Properties
    for ((k, v) <- test2ExpectedPropertiesMap) {
      test2ExpectedProperties.setProperty(k, v)
    }

    expect(mockAdminZkClient.fetchEntityConfig(ConfigType.Topic, TestTopic1Name)).andReturn(test2Properties)
    expect(mockAdminZkClient.changeTopicConfig(TestTopic1Name, test2ExpectedProperties))
    replay(mockZkClient)
    replay(mockAdminZkClient)

    val serviceTest2 = new ZkClientReassignCommandService(mockZkClient, Some(mockAdminZkClient))

    serviceTest2.updateTopicConfigs(TestTopic1Name, configUpdatesMap)
  }

  @Test
  def testUpdateShouldOverrideExistingTopicConfigs() : Unit = {
    val configUpdatesMap = Map("property1" -> "0", "property2" -> "1")

    // Test. Overwrite existing properties.
    val mockZkClient: KafkaZkClient = createMock(classOf[KafkaZkClient])
    val mockAdminZkClient: AdminZkClient = createMock(classOf[AdminZkClient])

    val test3Properties = new Properties
    val test3PropertiesMap = Map("otherProp1" -> "otherVal1", "property2" -> "origVal2")
    val test3ExpectedProperties = new Properties()
    for ((k,v) <- test3PropertiesMap) {
      test3Properties.setProperty(k, v)
      test3ExpectedProperties.setProperty(k, v)
    }
    for ((k,v) <- configUpdatesMap) {
      test3ExpectedProperties.setProperty(k, v)
    }

    expect(mockAdminZkClient.fetchEntityConfig(ConfigType.Topic, TestTopic1Name)).andReturn(test3Properties)
    expect(mockAdminZkClient.changeTopicConfig(TestTopic1Name, test3ExpectedProperties))
    replay(mockZkClient)
    replay(mockAdminZkClient)

    val serviceTest3 = new ZkClientReassignCommandService(mockZkClient, Some(mockAdminZkClient))

    serviceTest3.updateTopicConfigs(TestTopic1Name, configUpdatesMap)
  }

  // Test update broker configs. This behaves similarly to the topic test but the
  // interface is slightly different.
  @Test
  def testUpdateShouldOverrideEmptyBrokerConfigs() : Unit = {
    val testBrokerId = 17
    val configUpdatesMap = Map("property1" -> "0", "property2" -> "1")

    // Test 1: Have empty properties, this should just set the ones we expect.
    val test1Properties = new Properties()
    configUpdatesMap.foreach({ case (k, v) => test1Properties.setProperty(k: String, v: String) })
    val mockZkClient: KafkaZkClient = createMock(classOf[KafkaZkClient])
    val mockAdminZkClient: AdminZkClient = createMock(classOf[AdminZkClient])
    expect(mockAdminZkClient.fetchEntityConfig(ConfigType.Broker, testBrokerId.toString)).andReturn(new Properties)
    expect(mockAdminZkClient.changeBrokerConfig(Seq(testBrokerId), test1Properties))
    replay(mockZkClient)
    replay(mockAdminZkClient)

    val service = new ZkClientReassignCommandService(mockZkClient, Some(mockAdminZkClient))

    service.updateBrokerConfigs(testBrokerId, configUpdatesMap)
  }

  @Test
  def testUpdateShouldNotOverrideOtherBrokerConfigs() : Unit = {
    val testBrokerId = 17
    val configUpdatesMap = Map("property1" -> "0", "property2" -> "1")

    // Test: Hove other Properties that are completely disjoint from the ones we're updating.
    val test2Properties = new Properties
    val test2XtraPropertiesMap = Map("otherProp1" -> "otherVal1", "otherProp2" -> "otherVal2")
    for ((k, v) <- test2XtraPropertiesMap) {
      test2Properties.setProperty(k, v)
    }
    val test2ExpectedPropertiesMap = configUpdatesMap ++ test2XtraPropertiesMap
    val test2ExpectedProperties = new Properties
    for ((k, v) <- test2ExpectedPropertiesMap) {
      test2ExpectedProperties.setProperty(k, v)
    }

    val mockZkClient: KafkaZkClient = createMock(classOf[KafkaZkClient])
    val mockAdminZkClient: AdminZkClient = createMock(classOf[AdminZkClient])

    expect(mockAdminZkClient.fetchEntityConfig(ConfigType.Broker, testBrokerId.toString)).andReturn(test2Properties)
    expect(mockAdminZkClient.changeBrokerConfig(Seq(testBrokerId), test2ExpectedProperties))
    replay(mockZkClient)
    replay(mockAdminZkClient)

    val serviceTest2 = new ZkClientReassignCommandService(mockZkClient, Some(mockAdminZkClient))

    serviceTest2.updateBrokerConfigs(testBrokerId, configUpdatesMap)
  }

  @Test
  def testUpdateShouldOverrideExistingBrokerConfigs() : Unit = {
    val testBrokerId = 17
    val configUpdatesMap = Map("property1" -> "0", "property2" -> "1")

    // Test. Overwrite some existing broker properties.
    val test3Properties = new Properties
    val test3PropertiesMap = Map("otherProp1" -> "otherVal1", "property2" -> "origVal2")
    val test3ExpectedProperties = new Properties()
    for ((k,v) <- test3PropertiesMap) {
      test3Properties.setProperty(k, v)
      test3ExpectedProperties.setProperty(k, v)
    }
    for ((k,v) <- configUpdatesMap) {
      test3ExpectedProperties.setProperty(k, v)
    }
    val mockZkClient: KafkaZkClient = createMock(classOf[KafkaZkClient])
    val mockAdminZkClient: AdminZkClient = createMock(classOf[AdminZkClient])

    expect(mockAdminZkClient.fetchEntityConfig(ConfigType.Broker, testBrokerId.toString)).andReturn(test3Properties)
    expect(mockAdminZkClient.changeBrokerConfig(Seq(testBrokerId), test3ExpectedProperties))
    replay(mockZkClient)
    replay(mockAdminZkClient)

    val serviceTest3 = new ZkClientReassignCommandService(mockZkClient, Some(mockAdminZkClient))

    serviceTest3.updateBrokerConfigs(testBrokerId, configUpdatesMap)
  }

}

/**
 * Validate the AdminServiceClient used by the ReassignPartitionsCommand.
 */
class ReassignPartitionsServiceClientAdminTest {
  // Define a standardized test cluster
  val brokerIds = List(1, 4, 6, 7)
  val portBase = 9000
  // Try with a set of brokers where some have rack info and some don't
  val brokerList = brokerIds.map { id => new Node(id, s"broker-$id", portBase, if (id % 2 == 0) { "" } else { s"rack-$id" }) }
  val TestTopic1Name = "test1"
  // Topic test 1 has 2 partitions, 3RF, one partition is UR
  // XXX: MockAdminClient doesn't allow partitions with different replica sets
  val test1Partitions = List(new TopicPartitionInfo(1, brokerList(0), List(brokerList(0), brokerList(1), brokerList(2)).asJava,
    List(brokerList(0), brokerList(1)).asJava),
    new TopicPartitionInfo(2, brokerList(1), List(brokerList(0), brokerList(1), brokerList(2)).asJava,
      List(brokerList(0), brokerList(1), brokerList(2)).asJava)).asJava
  val TestTopic2Name = "test2"
  // Topic test2 has just 1 partition, RF 3, 1 UR partition
  val test2Partitions = List(new TopicPartitionInfo(1, brokerList(3), List(brokerList(2), brokerList(3), brokerList(0)).asJava,
    List(brokerList(3), brokerList(0)).asJava)).asJava

  // Topic test3 has 3 partitions, RF3
  val TestTopic3Name = "test3"
  val test3Partitions =  List(new TopicPartitionInfo(1, brokerList(0), List(brokerList(0), brokerList(1), brokerList(2)).asJava,
    List(brokerList(0), brokerList(1)).asJava),
    new TopicPartitionInfo(2, brokerList(1), List(brokerList(0), brokerList(1), brokerList(2)).asJava,
      List(brokerList(0), brokerList(1), brokerList(2)).asJava),
    new TopicPartitionInfo(3, brokerList(1), List(brokerList(0), brokerList(1), brokerList(2)).asJava,
      List(brokerList(0), brokerList(1), brokerList(2)).asJava)).asJava

  val TestInvalidTopicName = "test4"

  /**
   * Set up the standardized test cluster.
   */
  def setUpTestCluster() : Admin = {
    // Note the non-sequential, non-zero-based IDs.
    val mockAdmin = new MockAdminClient(brokerList.asJava, brokerList(2))

    mockAdmin.addTopic(false, TestTopic1Name, test1Partitions, Collections.emptyMap())
    mockAdmin.addTopic(false, TestTopic2Name, test2Partitions, Collections.emptyMap())
    mockAdmin.addTopic(false, TestTopic3Name, test3Partitions, Collections.emptyMap())

    mockAdmin
  }

  @Test
  def testGetBrokerList() : Unit = {
    val mockAdmin = setUpTestCluster()
    val serviceClient = new AdminClientReassignCommandService(mockAdmin)

    val brokerResults = serviceClient.getBrokerIdsInCluster
    assertEquals(brokerIds, brokerResults)
  }

  @Test
  def testGetBrokerMetadatasRackAwareDisabledRackedBrokersWithValidRacks() : Unit = {
    // Don't use the standard Test Cluster but one that's all got rack data
    val brokerIds = List(0, 1, 2, 3)
    val portBase = 9000
    val brokerList = brokerIds.map { id => new Node(id, s"broker-$id", portBase, s"rack-$id"); }
    val mockAdmin = new MockAdminClient(brokerList.asJava, brokerList(2))

    val serviceClient = new AdminClientReassignCommandService(mockAdmin)

    // Test the flavors of rack-aware with valid rack info
    val brokerMetadataRackDisabled = serviceClient.getBrokerMetadatas(kafka.admin.RackAwareMode.Disabled, None)
    assertEquals(brokerIds.map(id => new BrokerMetadata(id, None)), brokerMetadataRackDisabled)
  }

  @Test
  def testGetBrokerMetadatasRackAwareEnforcedRackedBrokersWithValidRacks() : Unit = {
    // Don't use the standard Test Cluster but one that's all got rack data
    val brokerIds = List(0, 1, 2, 3)
    val portBase = 9000
    val brokerList = brokerIds.map { id => new Node(id, s"broker-$id", portBase, s"rack-$id"); }
    val mockAdmin = new MockAdminClient(brokerList.asJava, brokerList(2))

    val serviceClient = new AdminClientReassignCommandService(mockAdmin)

    // Test the flavors of rack-aware with valid rack info
    val brokerMetadataRackAware = serviceClient.getBrokerMetadatas(kafka.admin.RackAwareMode.Enforced, None)
    assertEquals(brokerIds.map(id => new BrokerMetadata(id, Some(s"rack-$id"))), brokerMetadataRackAware)
  }

  @Test
  def testGetBrokerMetadatasRackAwareSafeRackedBrokersWithValidRacks() : Unit = {
    // Don't use the standard Test Cluster but one that's all got rack data
    val brokerIds = List(0, 1, 2, 3)
    val portBase = 9000
    val brokerList = brokerIds.map { id => new Node(id, s"broker-$id", portBase, s"rack-$id"); }
    val mockAdmin = new MockAdminClient(brokerList.asJava, brokerList(2))

    val serviceClient = new AdminClientReassignCommandService(mockAdmin)

    // RackAwareMode Safe should return rack info for this example
    val brokerMetadataRackSafe = serviceClient.getBrokerMetadatas(kafka.admin.RackAwareMode.Safe, None)
    assertEquals(brokerIds.map(id => new BrokerMetadata(id, Some(s"rack-$id"))), brokerMetadataRackSafe)
  }

  @Test
  def testGetBrokerMetadatasMixedRackDataRackAwareDisabled() : Unit = {
    val mockAdmin = setUpTestCluster()
    val serviceClient = new AdminClientReassignCommandService(mockAdmin)

    // Test the flavors of rack-aware with not-all-valid rack info
    val brokerMetadataRackDisabled = serviceClient.getBrokerMetadatas(kafka.admin.RackAwareMode.Disabled, None)
    assertEquals(brokerIds.map(id => new BrokerMetadata(id, None)), brokerMetadataRackDisabled)
  }

  @Test
  def testGetBrokerMetadatasMixedRackDataRackAwareEnforced() : Unit = {
    val mockAdmin = setUpTestCluster()
    val serviceClient = new AdminClientReassignCommandService(mockAdmin)

    // Enforced rack mode should throw here
    val badRackResult = Try(serviceClient.getBrokerMetadatas(kafka.admin.RackAwareMode.Enforced, None))
    assert(badRackResult.isFailure)
    assert(badRackResult match {
      // Shouldn't succeed
      case Success(v) => false
      // Expect an AdminOperationException
      case Failure(e: kafka.admin.AdminOperationException) => true
      case Failure(e) => false
    })
  }

  @Test
  def testGetBrokerMetadatasMixedRackDataRackAwareSafe() : Unit = {
    val mockAdmin = setUpTestCluster()
    val serviceClient = new AdminClientReassignCommandService(mockAdmin)

    // RackAwareMode Safe should return *no* rack info for this example.
    val brokerMetadataRackSafe = serviceClient.getBrokerMetadatas(kafka.admin.RackAwareMode.Safe, None)
    assertEquals(brokerIds.map(id => new BrokerMetadata(id, None)), brokerMetadataRackSafe)
  }

  @Test
  def testGetBrokerMetadatasLimitedSetAll() : Unit = {
    val mockAdmin = setUpTestCluster()
    val serviceClient = new AdminClientReassignCommandService(mockAdmin)
    val interestingBrokers = Array(4, 6)

    // Test limiting
    val brokerMetadataLimited = serviceClient.getBrokerMetadatas(kafka.admin.RackAwareMode.Disabled, Some(interestingBrokers))
    assert(brokerMetadataLimited.forall { n => interestingBrokers.contains(n.id) })
  }


  @Test
  def testGetBrokerMetadatasLimitedSetEmptyList() : Unit = {
    val mockAdmin = setUpTestCluster()
    val serviceClient = new AdminClientReassignCommandService(mockAdmin)

    val brokerMetadataFullList = serviceClient.getBrokerMetadatas(kafka.admin.RackAwareMode.Disabled, Some(List.empty[Int]))
    assertEquals(brokerIds, brokerMetadataFullList.collect({ case m: BrokerMetadata => m.id }))
  }


  @Test
  def testGetBrokerMetadatasLimitedSetNone() : Unit = {
    val mockAdmin = setUpTestCluster()
    val serviceClient = new AdminClientReassignCommandService(mockAdmin)

    val brokerMetadataFullNone =  serviceClient.getBrokerMetadatas(kafka.admin.RackAwareMode.Disabled, None)
    assertEquals(brokerIds, brokerMetadataFullNone.collect ({ case m: BrokerMetadata => m.id}))
  }


  def testUpdateBrokerConfigs() : Unit = ???

  def testUpdateTopicConfigs() : Unit = ???

  @Test
  def testGetPartitionsForTopicsAll() : Unit = {
    val mockAdmin = setUpTestCluster()
    val serviceClient = new AdminClientReassignCommandService(mockAdmin)

    // Test 1: Fetch all known topics
    val topicQuery1 = Set(TestTopic1Name, TestTopic2Name, TestTopic3Name)
    val topicResult1 = serviceClient.getPartitionsForTopics(topicQuery1)
    assertEquals(topicResult1(TestTopic1Name), Seq(1, 2))
    assertEquals(topicResult1(TestTopic2Name), Seq(1))
    assertEquals(topicResult1(TestTopic3Name), Seq(1, 2, 3))
  }

  @Test
  def testGetPartitionsForTopicsLimitedTopics() : Unit = {
    val mockAdmin = setUpTestCluster()
    val serviceClient = new AdminClientReassignCommandService(mockAdmin)

    // Test 2: Fetch not all topics
    val topicQuery2 = Set(TestTopic2Name, TestTopic3Name)
    val topicResult2 = serviceClient.getPartitionsForTopics(topicQuery2)
    assertEquals(topicResult2(TestTopic2Name), Seq(1))
    assertEquals(topicResult2(TestTopic3Name), Seq(1, 2, 3))
    assertFalse(topicResult2.contains(TestTopic1Name))
  }


  @Test
  def testGetPartitionsForTopicsNonexistentTopic() : Unit = {
    val mockAdmin = setUpTestCluster()
    val serviceClient = new AdminClientReassignCommandService(mockAdmin)

    // Test 3: Attempt to fetch an invalid topic
    val topicQuery3 = Set(TestTopic1Name, TestInvalidTopicName)
    val topicResult3 = Try(serviceClient.getPartitionsForTopics(topicQuery3))
    assert(topicResult3.isFailure)
    assert(topicResult3 match {
      case Failure(e: ExecutionException) => e.getCause match {
        case t : UnknownTopicOrPartitionException => true
        case _ => throw e.getCause
      }
      case _ => false
    })
  }

  @Test
  def testGetReplicaAssignmentForTopicsAllTopics() : Unit = {
    val mockAdmin = setUpTestCluster()
    val serviceClient = new AdminClientReassignCommandService(mockAdmin)

    // Test 1: Fetch partitions for all known topics
    val topicQuery1 = Set(TestTopic1Name, TestTopic2Name, TestTopic3Name)
    val topicResult1 = serviceClient.getReplicaAssignmentForTopics(topicQuery1)
    assertEquals(topicResult1(new TopicPartition(TestTopic1Name, 1)), Seq(1, 4, 6))
    assertEquals(topicResult1(new TopicPartition(TestTopic1Name, 2)), Seq(1, 4, 6))
    assertEquals(topicResult1(new TopicPartition(TestTopic2Name, 1)), Seq(6, 7, 1))
    assertEquals(topicResult1(new TopicPartition(TestTopic3Name, 1)), Seq(1, 4, 6))
    assertEquals(topicResult1(new TopicPartition(TestTopic3Name, 2)), Seq(1, 4, 6))
    assertEquals(topicResult1(new TopicPartition(TestTopic3Name, 3)), Seq(1, 4, 6))
  }

  @Test
  def testGetReplicaAssignmentForTopicsLimitedSet() : Unit = {
    val mockAdmin = setUpTestCluster()
    val serviceClient = new AdminClientReassignCommandService(mockAdmin)

    // Test 2: Fetch not all topics
    val topicQuery2 = Set(TestTopic2Name, TestTopic3Name)
    val topicResult2 = serviceClient.getReplicaAssignmentForTopics(topicQuery2)
    assertEquals(topicResult2(new TopicPartition(TestTopic2Name, 1)), Seq(6, 7, 1))
    assertEquals(topicResult2(new TopicPartition(TestTopic3Name, 1)), Seq(1, 4, 6))
    assertEquals(topicResult2(new TopicPartition(TestTopic3Name, 2)), Seq(1, 4, 6))
    assertEquals(topicResult2(new TopicPartition(TestTopic3Name, 3)), Seq(1, 4, 6))
    assertFalse(topicResult2.contains(new TopicPartition(TestTopic1Name, 1)))
  }

  @Test
  def testGetReplicaAssignmentForTopicsNonexistentTopic() : Unit = {
    val mockAdmin = setUpTestCluster()
    val serviceClient = new AdminClientReassignCommandService(mockAdmin)

    // Test 3: Attempt to fetch an invalid topic
    val topicQuery3 = Set(TestTopic1Name, TestInvalidTopicName)
    val topicResult3 = Try(serviceClient.getReplicaAssignmentForTopics(topicQuery3))
    assert(topicResult3.isFailure)
    assert(topicResult3 match {
      case Failure(e: ExecutionException) => e.getCause match {
        case t : UnknownTopicOrPartitionException => true
        case _ => throw e.getCause
      }
      case _ => false
    })

  }

  def testGetReplicaLogDirsForTopics() : Unit = ???

  def testAlterPartitionAssignment() : Unit = ???

  def testAlterPartitionLogDirs() : Unit = ???

  def testGetReassignmentInProgress() : Unit = ???

  def testGetOngoingReassignments() : Unit = ???

}
