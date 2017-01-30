/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package kafka.api

import java.nio.ByteBuffer
import java.util
import java.util.concurrent.ExecutionException
import java.util.regex.Pattern
import java.util.{ArrayList, Collections, Properties}

import kafka.common
import kafka.common.TopicAndPartition
import kafka.security.auth._
import kafka.server.{BaseRequestTest, KafkaConfig}
import kafka.utils.TestUtils
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener
import org.apache.kafka.clients.consumer._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.errors._
import org.apache.kafka.common.protocol.{ApiKeys, Errors, SecurityProtocol}
import org.apache.kafka.common.requests._
import CreateTopicsRequest.TopicDetails
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.{Node, TopicPartition, requests}
import org.junit.Assert._
import org.junit.{After, Assert, Before, Test}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.Buffer
import org.apache.kafka.common.KafkaException
import kafka.admin.AdminUtils
import kafka.network.SocketServer
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.record.MemoryRecords

class AuthorizerIntegrationTest extends BaseRequestTest {

  override def numBrokers: Int = 1
  val brokerId: Integer = 0

  val topic = "topic"
  val topicPattern = "topic.*"
  val createTopic = "topic-new"
  val deleteTopic = "topic-delete"
  val part = 0
  val correlationId = 0
  val clientId = "client-Id"
  val tp = new TopicPartition(topic, part)
  val topicAndPartition = TopicAndPartition(topic, part)
  val group = "my-group"
  val topicResource = new Resource(Topic, topic)
  val groupResource = new Resource(Group, group)
  val deleteTopicResource = new Resource(Topic, deleteTopic)

  val GroupReadAcl = Map(groupResource -> Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Read)))
  val ClusterAcl = Map(Resource.ClusterResource -> Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, ClusterAction)))
  val ClusterCreateAcl = Map(Resource.ClusterResource -> Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Create)))
  val TopicReadAcl = Map(topicResource -> Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Read)))
  val TopicWriteAcl = Map(topicResource -> Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Write)))
  val TopicDescribeAcl = Map(topicResource -> Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Describe)))
  val TopicDeleteAcl = Map(deleteTopicResource -> Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Delete)))

  val consumers = Buffer[KafkaConsumer[Array[Byte], Array[Byte]]]()
  val producers = Buffer[KafkaProducer[Array[Byte], Array[Byte]]]()

  val producerCount = 1
  val consumerCount = 2
  val producerConfig = new Properties
  val numRecords = 1

  override def propertyOverrides(properties: Properties): Unit = {
    properties.put(KafkaConfig.AuthorizerClassNameProp, classOf[SimpleAclAuthorizer].getName)
    properties.put(KafkaConfig.BrokerIdProp, brokerId.toString)
    properties.put(KafkaConfig.OffsetsTopicPartitionsProp, "1")
  }

  val RequestKeyToResponseDeserializer: Map[ApiKeys, Class[_ <: Any]] =
    Map(ApiKeys.METADATA -> classOf[requests.MetadataResponse],
      ApiKeys.PRODUCE -> classOf[requests.ProduceResponse],
      ApiKeys.FETCH -> classOf[requests.FetchResponse],
      ApiKeys.LIST_OFFSETS -> classOf[requests.ListOffsetResponse],
      ApiKeys.OFFSET_COMMIT -> classOf[requests.OffsetCommitResponse],
      ApiKeys.OFFSET_FETCH -> classOf[requests.OffsetFetchResponse],
      ApiKeys.GROUP_COORDINATOR -> classOf[requests.GroupCoordinatorResponse],
      ApiKeys.UPDATE_METADATA_KEY -> classOf[requests.UpdateMetadataResponse],
      ApiKeys.JOIN_GROUP -> classOf[JoinGroupResponse],
      ApiKeys.SYNC_GROUP -> classOf[SyncGroupResponse],
      ApiKeys.HEARTBEAT -> classOf[HeartbeatResponse],
      ApiKeys.LEAVE_GROUP -> classOf[LeaveGroupResponse],
      ApiKeys.LEADER_AND_ISR -> classOf[requests.LeaderAndIsrResponse],
      ApiKeys.STOP_REPLICA -> classOf[requests.StopReplicaResponse],
      ApiKeys.CONTROLLED_SHUTDOWN_KEY -> classOf[requests.ControlledShutdownResponse],
      ApiKeys.CREATE_TOPICS -> classOf[CreateTopicsResponse],
      ApiKeys.DELETE_TOPICS -> classOf[requests.DeleteTopicsResponse]
  )

  val RequestKeyToErrorCode = Map[ApiKeys, (Nothing) => Short](
    ApiKeys.METADATA -> ((resp: requests.MetadataResponse) => resp.errors().asScala.find(_._1 == topic).getOrElse(("test", Errors.NONE))._2.code),
    ApiKeys.PRODUCE -> ((resp: requests.ProduceResponse) => resp.responses().asScala.find(_._1 == tp).get._2.error.code),
    ApiKeys.FETCH -> ((resp: requests.FetchResponse) => resp.responseData().asScala.find(_._1 == tp).get._2.errorCode),
    ApiKeys.LIST_OFFSETS -> ((resp: requests.ListOffsetResponse) => resp.responseData().asScala.find(_._1 == tp).get._2.errorCode),
    ApiKeys.OFFSET_COMMIT -> ((resp: requests.OffsetCommitResponse) => resp.responseData().asScala.find(_._1 == tp).get._2),
    ApiKeys.OFFSET_FETCH -> ((resp: requests.OffsetFetchResponse) => resp.error.code),
    ApiKeys.GROUP_COORDINATOR -> ((resp: requests.GroupCoordinatorResponse) => resp.errorCode()),
    ApiKeys.UPDATE_METADATA_KEY -> ((resp: requests.UpdateMetadataResponse) => resp.errorCode()),
    ApiKeys.JOIN_GROUP -> ((resp: JoinGroupResponse) => resp.errorCode()),
    ApiKeys.SYNC_GROUP -> ((resp: SyncGroupResponse) => resp.errorCode()),
    ApiKeys.HEARTBEAT -> ((resp: HeartbeatResponse) => resp.errorCode()),
    ApiKeys.LEAVE_GROUP -> ((resp: LeaveGroupResponse) => resp.errorCode()),
    ApiKeys.LEADER_AND_ISR -> ((resp: requests.LeaderAndIsrResponse) => resp.responses().asScala.find(_._1 == tp).get._2),
    ApiKeys.STOP_REPLICA -> ((resp: requests.StopReplicaResponse) => resp.responses().asScala.find(_._1 == tp).get._2),
    ApiKeys.CONTROLLED_SHUTDOWN_KEY -> ((resp: requests.ControlledShutdownResponse) => resp.errorCode()),
    ApiKeys.CREATE_TOPICS -> ((resp: CreateTopicsResponse) => resp.errors().asScala.find(_._1 == createTopic).get._2.error.code),
    ApiKeys.DELETE_TOPICS -> ((resp: requests.DeleteTopicsResponse) => resp.errors().asScala.find(_._1 == deleteTopic).get._2.code)
  )

  val RequestKeysToAcls = Map[ApiKeys, Map[Resource, Set[Acl]]](
    ApiKeys.METADATA -> TopicDescribeAcl,
    ApiKeys.PRODUCE -> TopicWriteAcl,
    ApiKeys.FETCH -> TopicReadAcl,
    ApiKeys.LIST_OFFSETS -> TopicDescribeAcl,
    ApiKeys.OFFSET_COMMIT -> (TopicReadAcl ++ GroupReadAcl),
    ApiKeys.OFFSET_FETCH -> (TopicReadAcl ++ GroupReadAcl),
    ApiKeys.GROUP_COORDINATOR -> (TopicReadAcl ++ GroupReadAcl),
    ApiKeys.UPDATE_METADATA_KEY -> ClusterAcl,
    ApiKeys.JOIN_GROUP -> GroupReadAcl,
    ApiKeys.SYNC_GROUP -> GroupReadAcl,
    ApiKeys.HEARTBEAT -> GroupReadAcl,
    ApiKeys.LEAVE_GROUP -> GroupReadAcl,
    ApiKeys.LEADER_AND_ISR -> ClusterAcl,
    ApiKeys.STOP_REPLICA -> ClusterAcl,
    ApiKeys.CONTROLLED_SHUTDOWN_KEY -> ClusterAcl,
    ApiKeys.CREATE_TOPICS -> ClusterCreateAcl,
    ApiKeys.DELETE_TOPICS -> TopicDeleteAcl
  )

  @Before
  override def setUp() {
    super.setUp()

    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, ClusterAction)), Resource.ClusterResource)

    for (_ <- 0 until producerCount)
      producers += TestUtils.createNewProducer(TestUtils.getBrokerListStrFromServers(servers),
        maxBlockMs = 3000,
        acks = 1)
    for (_ <- 0 until consumerCount)
      consumers += TestUtils.createNewConsumer(TestUtils.getBrokerListStrFromServers(servers), groupId = group, securityProtocol = SecurityProtocol.PLAINTEXT)

    // create the consumer offset topic
    TestUtils.createTopic(zkUtils, common.Topic.GroupMetadataTopicName,
      1,
      1,
      servers,
      servers.head.groupCoordinator.offsetsTopicConfigs)
    // create the test topic with all the brokers as replicas
    TestUtils.createTopic(zkUtils, topic, 1, 1, this.servers)
    TestUtils.createTopic(zkUtils, deleteTopic, 1, 1, this.servers)
  }

  @After
  override def tearDown() = {
    producers.foreach(_.close())
    consumers.foreach(_.wakeup())
    consumers.foreach(_.close())
    removeAllAcls()
    super.tearDown()
  }

  private def createMetadataRequest = {
    new requests.MetadataRequest.Builder(List(topic).asJava).build()
  }

  private def createProduceRequest = {
    new requests.ProduceRequest.Builder(1, 5000,
      collection.mutable.Map(tp -> MemoryRecords.readableRecords(ByteBuffer.wrap("test".getBytes))).asJava).
      build()
  }

  private def createFetchRequest = {
    val partitionMap = new util.LinkedHashMap[TopicPartition, requests.FetchRequest.PartitionData]
    partitionMap.put(tp, new requests.FetchRequest.PartitionData(0, 100))
    new requests.FetchRequest.Builder(100, Int.MaxValue, partitionMap).setReplicaId(5000).build()
  }

  private def createListOffsetsRequest = {
    new requests.ListOffsetRequest.Builder().setTargetTimes(
      Map(tp -> (0L: java.lang.Long)).asJava).
      build()
  }

  private def createOffsetFetchRequest = {
    new requests.OffsetFetchRequest.Builder(group, List(tp).asJava).build()
  }

  private def createGroupCoordinatorRequest = {
    new requests.GroupCoordinatorRequest.Builder(group).build()
  }

  private def createUpdateMetadataRequest = {
    val partitionState = Map(tp -> new PartitionState(Int.MaxValue, brokerId, Int.MaxValue, List(brokerId).asJava, 2, Set(brokerId).asJava)).asJava
    val securityProtocol = SecurityProtocol.PLAINTEXT
    val brokers = Set(new requests.UpdateMetadataRequest.Broker(brokerId,
      Seq(new requests.UpdateMetadataRequest.EndPoint("localhost", 0, securityProtocol,
        ListenerName.forSecurityProtocol(securityProtocol))).asJava, null)).asJava
    new requests.UpdateMetadataRequest.Builder(brokerId, Int.MaxValue, partitionState, brokers).build()
  }

  private def createJoinGroupRequest = {
    new JoinGroupRequest.Builder(group, 10000, "", "consumer",
      List( new JoinGroupRequest.ProtocolMetadata("consumer-range",ByteBuffer.wrap("test".getBytes()))).asJava)
      .setRebalanceTimeout(60000).build()
  }

  private def createSyncGroupRequest = {
    new SyncGroupRequest.Builder(group, 1, "", Map[String, ByteBuffer]().asJava).build()
  }

  private def createOffsetCommitRequest = {
    new requests.OffsetCommitRequest.Builder(
      group, Map(tp -> new requests.OffsetCommitRequest.PartitionData(0, "metadata")).asJava).
      setMemberId("").setGenerationId(1).setRetentionTime(1000).
      build()
  }

  private def createHeartbeatRequest = {
    new HeartbeatRequest.Builder(group, 1, "").build()
  }

  private def createLeaveGroupRequest = {
    new LeaveGroupRequest.Builder(group, "").build()
  }

  private def createLeaderAndIsrRequest = {
    new requests.LeaderAndIsrRequest.Builder(brokerId, Int.MaxValue,
      Map(tp -> new PartitionState(Int.MaxValue, brokerId, Int.MaxValue, List(brokerId).asJava, 2, Set(brokerId).asJava)).asJava,
      Set(new Node(brokerId, "localhost", 0)).asJava).build()
  }

  private def createStopReplicaRequest = {
    new requests.StopReplicaRequest.Builder(brokerId, Int.MaxValue, true, Set(tp).asJava).build()
  }

  private def createControlledShutdownRequest = {
    new requests.ControlledShutdownRequest.Builder(brokerId).build()
  }

  private def createTopicsRequest = {
    new CreateTopicsRequest.Builder(Map(createTopic -> new TopicDetails(1, 1.toShort)).asJava, 0).build()
  }

  private def deleteTopicsRequest = {
    new DeleteTopicsRequest.Builder(Set(deleteTopic).asJava, 5000).build()
  }

  @Test
  def testAuthorizationWithTopicExisting() {
    val requestKeyToRequest = mutable.LinkedHashMap[ApiKeys, AbstractRequest](
      ApiKeys.METADATA -> createMetadataRequest,
      ApiKeys.PRODUCE -> createProduceRequest,
      ApiKeys.FETCH -> createFetchRequest,
      ApiKeys.LIST_OFFSETS -> createListOffsetsRequest,
      ApiKeys.OFFSET_FETCH -> createOffsetFetchRequest,
      ApiKeys.GROUP_COORDINATOR -> createGroupCoordinatorRequest,
      ApiKeys.UPDATE_METADATA_KEY -> createUpdateMetadataRequest,
      ApiKeys.JOIN_GROUP -> createJoinGroupRequest,
      ApiKeys.SYNC_GROUP -> createSyncGroupRequest,
      ApiKeys.OFFSET_COMMIT -> createOffsetCommitRequest,
      ApiKeys.HEARTBEAT -> createHeartbeatRequest,
      ApiKeys.LEAVE_GROUP -> createLeaveGroupRequest,
      ApiKeys.LEADER_AND_ISR -> createLeaderAndIsrRequest,
      ApiKeys.STOP_REPLICA -> createStopReplicaRequest,
      ApiKeys.CONTROLLED_SHUTDOWN_KEY -> createControlledShutdownRequest,
      ApiKeys.CREATE_TOPICS -> createTopicsRequest,
      ApiKeys.DELETE_TOPICS -> deleteTopicsRequest
    )

    for ((key, request) <- requestKeyToRequest) {
      removeAllAcls
      val resources = RequestKeysToAcls(key).map(_._1.resourceType).toSet
      sendRequestAndVerifyResponseErrorCode(key, request, resources, isAuthorized = false, isAuthorizedTopicDescribe = false)

      val resourceToAcls = RequestKeysToAcls(key)
      resourceToAcls.get(topicResource).map { acls =>
        val describeAcls = TopicDescribeAcl(topicResource)
        val isAuthorized =  describeAcls == acls
        addAndVerifyAcls(describeAcls, topicResource)
        sendRequestAndVerifyResponseErrorCode(key, request, resources, isAuthorized = isAuthorized, isAuthorizedTopicDescribe = true)
        removeAllAcls
      }

      for ((resource, acls) <- resourceToAcls)
        addAndVerifyAcls(acls, resource)
      sendRequestAndVerifyResponseErrorCode(key, request, resources, isAuthorized = true, isAuthorizedTopicDescribe = false)
    }
  }

  /*
   * even if the topic doesn't exist, request APIs should not leak the topic name
   */
  @Test
  def testAuthorizationWithTopicNotExisting() {
    AdminUtils.deleteTopic(zkUtils, topic)
    TestUtils.verifyTopicDeletion(zkUtils, topic, 1, servers)
    AdminUtils.deleteTopic(zkUtils, deleteTopic)
    TestUtils.verifyTopicDeletion(zkUtils, deleteTopic, 1, servers)

    val requestKeyToRequest = mutable.LinkedHashMap[ApiKeys, AbstractRequest](
      ApiKeys.PRODUCE -> createProduceRequest,
      ApiKeys.FETCH -> createFetchRequest,
      ApiKeys.LIST_OFFSETS -> createListOffsetsRequest,
      ApiKeys.OFFSET_COMMIT -> createOffsetCommitRequest,
      ApiKeys.OFFSET_FETCH -> createOffsetFetchRequest,
      ApiKeys.DELETE_TOPICS -> deleteTopicsRequest
    )

    for ((key, request) <- requestKeyToRequest) {
      removeAllAcls
      val resources = RequestKeysToAcls(key).map(_._1.resourceType).toSet
      sendRequestAndVerifyResponseErrorCode(key, request, resources, isAuthorized = false, isAuthorizedTopicDescribe = false, topicExists = false)

      val resourceToAcls = RequestKeysToAcls(key)
      resourceToAcls.get(topicResource).map { acls =>
        val describeAcls = TopicDescribeAcl(topicResource)
        val isAuthorized =  describeAcls == acls
        addAndVerifyAcls(describeAcls, topicResource)
        sendRequestAndVerifyResponseErrorCode(key, request, resources, isAuthorized = isAuthorized, isAuthorizedTopicDescribe = true, topicExists = false)
        removeAllAcls
      }

      for ((resource, acls) <- resourceToAcls)
        addAndVerifyAcls(acls, resource)
      sendRequestAndVerifyResponseErrorCode(key, request, resources, isAuthorized = true, isAuthorizedTopicDescribe = false, topicExists = false)
    }
  }

  @Test
  def testProduceWithNoTopicAccess() {
    try {
      sendRecords(numRecords, tp)
      fail("should have thrown exception")
    } catch {
      case _: TimeoutException => //expected
    }
  }

  @Test
  def testProduceWithTopicDescribe() {
    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Describe)), topicResource)
    try {
      sendRecords(numRecords, tp)
      fail("should have thrown exception")
    } catch {
      case e: TopicAuthorizationException =>
        assertEquals(Collections.singleton(topic), e.unauthorizedTopics())
    }
  }

  @Test
  def testProduceWithTopicRead() {
    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Read)), topicResource)
    try {
      sendRecords(numRecords, tp)
      fail("should have thrown exception")
    } catch {
      case e: TopicAuthorizationException =>
        assertEquals(Collections.singleton(topic), e.unauthorizedTopics())
    }
  }

  @Test
  def testProduceWithTopicWrite() {
    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Write)), topicResource)
    sendRecords(numRecords, tp)
  }

  @Test
  def testCreatePermissionNeededForWritingToNonExistentTopic() {
    val newTopic = "newTopic"
    val topicPartition = new TopicPartition(newTopic, 0)
    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Write)), new Resource(Topic, newTopic))
    try {
      sendRecords(numRecords, topicPartition)
      Assert.fail("should have thrown exception")
    } catch {
      case e: TopicAuthorizationException => assertEquals(Collections.singleton(newTopic), e.unauthorizedTopics())
    }

    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Create)), Resource.ClusterResource)
    sendRecords(numRecords, topicPartition)
  }

  @Test(expected = classOf[AuthorizationException])
  def testConsumeWithNoAccess(): Unit = {
    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Write)), topicResource)
    sendRecords(1, tp)
    removeAllAcls()
    this.consumers.head.assign(List(tp).asJava)
    consumeRecords(this.consumers.head)
  }

  @Test
  def testSimpleConsumeWithOffsetLookupAndNoGroupAccess(): Unit = {
    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Write)), topicResource)
    sendRecords(1, tp)
    removeAllAcls()

    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Read)), topicResource)
    try {
      // note this still depends on group access because we haven't set offsets explicitly, which means
      // they will first be fetched from the consumer coordinator (which requires group access)
      this.consumers.head.assign(List(tp).asJava)
      consumeRecords(this.consumers.head)
      Assert.fail("should have thrown exception")
    } catch {
      case e: GroupAuthorizationException => assertEquals(group, e.groupId())
    }
  }

  @Test
  def testSimpleConsumeWithExplicitSeekAndNoGroupAccess(): Unit = {
    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Write)), topicResource)
    sendRecords(1, tp)
    removeAllAcls()

    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Read)), topicResource)

    // in this case, we do an explicit seek, so there should be no need to query the coordinator at all
    this.consumers.head.assign(List(tp).asJava)
    this.consumers.head.seekToBeginning(List(tp).asJava)
    consumeRecords(this.consumers.head)
  }

  @Test(expected = classOf[KafkaException])
  def testConsumeWithoutTopicDescribeAccess() {
    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Write)), topicResource)
    sendRecords(1, tp)
    removeAllAcls()

    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Read)), groupResource)
    this.consumers.head.assign(List(tp).asJava)

    // the consumer should raise an exception if it receives UNKNOWN_TOPIC_OR_PARTITION
    // from the ListOffsets response when looking up the initial position.
    consumeRecords(this.consumers.head)
  }

  @Test
  def testConsumeWithTopicDescribe() {
    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Write)), topicResource)
    sendRecords(1, tp)
    removeAllAcls()

    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Describe)), topicResource)
    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Read)), groupResource)
    try {
      this.consumers.head.assign(List(tp).asJava)
      consumeRecords(this.consumers.head)
      Assert.fail("should have thrown exception")
    } catch {
      case e: TopicAuthorizationException => assertEquals(Collections.singleton(topic), e.unauthorizedTopics())
    }
  }

  @Test
  def testConsumeWithTopicWrite() {
    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Write)), topicResource)
    sendRecords(1, tp)
    removeAllAcls()

    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Write)), topicResource)
    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Read)), groupResource)
    try {
      this.consumers.head.assign(List(tp).asJava)
      consumeRecords(this.consumers.head)
      Assert.fail("should have thrown exception")
    } catch {
      case e: TopicAuthorizationException =>
        assertEquals(Collections.singleton(topic), e.unauthorizedTopics())
    }
  }

  @Test
  def testConsumeWithTopicAndGroupRead() {
    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Write)), topicResource)
    sendRecords(1, tp)
    removeAllAcls()

    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Read)), topicResource)
    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Read)), groupResource)
    this.consumers.head.assign(List(tp).asJava)
    consumeRecords(this.consumers.head)
  }

  @Test
  def testPatternSubscriptionWithNoTopicAccess() {
    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Write)), topicResource)
    sendRecords(1, tp)
    removeAllAcls()

    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Read)), groupResource)
    this.consumers.head.subscribe(Pattern.compile(topicPattern), new NoOpConsumerRebalanceListener)
    this.consumers.head.poll(50)
    assertTrue(this.consumers.head.subscription.isEmpty)
  }

  @Test
  def testPatternSubscriptionWithTopicDescribeOnlyAndGroupRead() {
    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Write)), topicResource)
    sendRecords(1, tp)
    removeAllAcls()

    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Describe)), topicResource)
    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Read)), groupResource)
    val consumer = consumers.head
    consumer.subscribe(Pattern.compile(topicPattern), new NoOpConsumerRebalanceListener)
    try {
      consumeRecords(consumer)
      Assert.fail("Expected TopicAuthorizationException")
    } catch {
      case _: TopicAuthorizationException => //expected
    }
  }

  @Test
  def testPatternSubscriptionWithTopicAndGroupRead() {
    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Write)), topicResource)
    sendRecords(1, tp)

    // create an unmatched topic
    val unmatchedTopic = "unmatched"
    TestUtils.createTopic(zkUtils, unmatchedTopic, 1, 1, this.servers)
    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Write)),  new Resource(Topic, unmatchedTopic))
    sendRecords(1, new TopicPartition(unmatchedTopic, part))
    removeAllAcls()

    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Read)), topicResource)
    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Read)), groupResource)
    val consumer = consumers.head
    consumer.subscribe(Pattern.compile(topicPattern), new NoOpConsumerRebalanceListener)
    consumeRecords(consumer)

    // set the subscription pattern to an internal topic that the consumer has read permission to. Since
    // internal topics are not included, we should not be assigned any partitions from this topic
    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Read)),  new Resource(Topic, kafka.common.Topic.GroupMetadataTopicName))
    consumer.subscribe(Pattern.compile(kafka.common.Topic.GroupMetadataTopicName), new NoOpConsumerRebalanceListener)
    consumer.poll(0)
    assertTrue(consumer.subscription().isEmpty)
    assertTrue(consumer.assignment().isEmpty)
  }

  @Test
  def testPatternSubscriptionMatchingInternalTopic() {
    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Write)), topicResource)
    sendRecords(1, tp)
    removeAllAcls()

    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Read)), topicResource)
    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Read)), groupResource)

    val consumerConfig = new Properties
    consumerConfig.put(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG, "false")
    val consumer = TestUtils.createNewConsumer(TestUtils.getBrokerListStrFromServers(servers), groupId = group,
      securityProtocol = SecurityProtocol.PLAINTEXT, props = Some(consumerConfig))
    try {
      // ensure that internal topics are not included if no permission
      consumer.subscribe(Pattern.compile(".*"), new NoOpConsumerRebalanceListener)
      consumeRecords(consumer)
      assertEquals(Set(topic).asJava, consumer.subscription)

      // now authorize the user for the internal topic and verify that we can subscribe
      addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Read)), Resource(Topic, kafka.common.Topic.GroupMetadataTopicName))
      consumer.subscribe(Pattern.compile(kafka.common.Topic.GroupMetadataTopicName), new NoOpConsumerRebalanceListener)
      consumer.poll(0)
      assertEquals(Set(kafka.common.Topic.GroupMetadataTopicName), consumer.subscription.asScala)
    } finally consumer.close()
  }

  @Test
  def testPatternSubscriptionMatchingInternalTopicWithDescribeOnlyPermission() {
    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Write)), topicResource)
    sendRecords(1, tp)
    removeAllAcls()

    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Read)), topicResource)
    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Read)), groupResource)
    val internalTopicResource = new Resource(Topic, kafka.common.Topic.GroupMetadataTopicName)
    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Describe)), internalTopicResource)

    val consumerConfig = new Properties
    consumerConfig.put(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG, "false")
    val consumer = TestUtils.createNewConsumer(TestUtils.getBrokerListStrFromServers(servers), groupId = group,
      securityProtocol = SecurityProtocol.PLAINTEXT, props = Some(consumerConfig))
    try {
      consumer.subscribe(Pattern.compile(".*"), new NoOpConsumerRebalanceListener)
      consumeRecords(consumer)
      Assert.fail("Expected TopicAuthorizationException")
    } catch {
      case _: TopicAuthorizationException => //expected
    } finally consumer.close()
  }

  @Test
  def testPatternSubscriptionNotMatchingInternalTopic() {
    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Write)), topicResource)
    sendRecords(1, tp)
    removeAllAcls()

    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Read)), topicResource)
    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Read)), groupResource)

    val consumerConfig = new Properties
    consumerConfig.put(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG, "false")
    val consumer = TestUtils.createNewConsumer(TestUtils.getBrokerListStrFromServers(servers), groupId = group,
      securityProtocol = SecurityProtocol.PLAINTEXT, props = Some(consumerConfig))
    try {
      consumer.subscribe(Pattern.compile(topicPattern), new NoOpConsumerRebalanceListener)
      consumeRecords(consumer)
    } finally consumer.close()
}

  @Test
  def testCreatePermissionNeededToReadFromNonExistentTopic() {
    val newTopic = "newTopic"
    val topicPartition = new TopicPartition(newTopic, 0)
    val newTopicResource = new Resource(Topic, newTopic)
    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Read)), newTopicResource)
    addAndVerifyAcls(GroupReadAcl(groupResource), groupResource)
    addAndVerifyAcls(ClusterAcl(Resource.ClusterResource), Resource.ClusterResource)
    try {
      this.consumers.head.assign(List(topicPartition).asJava)
      consumeRecords(this.consumers.head)
      Assert.fail("should have thrown exception")
    } catch {
      case e: TopicAuthorizationException =>
        assertEquals(Collections.singleton(newTopic), e.unauthorizedTopics())
    }

    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Write)), newTopicResource)
    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Create)), Resource.ClusterResource)

    sendRecords(numRecords, topicPartition)
    consumeRecords(this.consumers.head, topic = newTopic, part = 0)
  }

  @Test(expected = classOf[AuthorizationException])
  def testCommitWithNoAccess() {
    this.consumers.head.commitSync(Map(tp -> new OffsetAndMetadata(5)).asJava)
  }

  @Test(expected = classOf[KafkaException])
  def testCommitWithNoTopicAccess() {
    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Read)), groupResource)
    this.consumers.head.commitSync(Map(tp -> new OffsetAndMetadata(5)).asJava)
  }

  @Test(expected = classOf[TopicAuthorizationException])
  def testCommitWithTopicWrite() {
    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Read)), groupResource)
    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Write)), topicResource)
    this.consumers.head.commitSync(Map(tp -> new OffsetAndMetadata(5)).asJava)
  }

  @Test(expected = classOf[TopicAuthorizationException])
  def testCommitWithTopicDescribe() {
    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Read)), groupResource)
    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Describe)), topicResource)
    this.consumers.head.commitSync(Map(tp -> new OffsetAndMetadata(5)).asJava)
  }

  @Test(expected = classOf[GroupAuthorizationException])
  def testCommitWithNoGroupAccess() {
    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Read)), topicResource)
    this.consumers.head.commitSync(Map(tp -> new OffsetAndMetadata(5)).asJava)
  }

  @Test
  def testCommitWithTopicAndGroupRead() {
    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Read)), groupResource)
    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Read)), topicResource)
    this.consumers.head.commitSync(Map(tp -> new OffsetAndMetadata(5)).asJava)
  }

  @Test(expected = classOf[AuthorizationException])
  def testOffsetFetchWithNoAccess() {
    this.consumers.head.assign(List(tp).asJava)
    this.consumers.head.position(tp)
  }

  @Test(expected = classOf[GroupAuthorizationException])
  def testOffsetFetchWithNoGroupAccess() {
    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Read)), topicResource)
    this.consumers.head.assign(List(tp).asJava)
    this.consumers.head.position(tp)
  }

  @Test(expected = classOf[KafkaException])
  def testOffsetFetchWithNoTopicAccess() {
    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Read)), groupResource)
    this.consumers.head.assign(List(tp).asJava)
    this.consumers.head.position(tp)
  }

  @Test
  def testFetchAllOffsetsTopicAuthorization() {
    val offset = 15L
    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Read)), groupResource)
    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Read)), topicResource)
    this.consumers.head.assign(List(tp).asJava)
    this.consumers.head.commitSync(Map(tp -> new OffsetAndMetadata(offset)).asJava)

    removeAllAcls()
    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Read)), groupResource)

    // send offset fetch requests directly since the consumer does not expose an API to do so
    // note there's only one broker, so no need to lookup the group coordinator

    // without describe permission on the topic, we shouldn't be able to fetch offsets
    val offsetFetchRequest = requests.OffsetFetchRequest.forAllPartitions(group)
    var offsetFetchResponse = sendOffsetFetchRequest(offsetFetchRequest, anySocketServer)
    assertEquals(Errors.NONE, offsetFetchResponse.error)
    assertTrue(offsetFetchResponse.responseData.isEmpty)

    // now add describe permission on the topic and verify that the offset can be fetched
    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Describe)), topicResource)
    offsetFetchResponse = sendOffsetFetchRequest(offsetFetchRequest, anySocketServer)
    assertEquals(Errors.NONE, offsetFetchResponse.error)
    assertTrue(offsetFetchResponse.responseData.containsKey(tp))
    assertEquals(offset, offsetFetchResponse.responseData.get(tp).offset)
  }

  @Test
  def testOffsetFetchTopicDescribe() {
    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Read)), groupResource)
    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Describe)), topicResource)
    this.consumers.head.assign(List(tp).asJava)
    this.consumers.head.position(tp)
  }

  @Test
  def testOffsetFetchWithTopicAndGroupRead() {
    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Read)), groupResource)
    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Read)), topicResource)
    this.consumers.head.assign(List(tp).asJava)
    this.consumers.head.position(tp)
  }

  @Test
  def testListOffsetsWithNoTopicAccess() {
    val partitionInfos = this.consumers.head.partitionsFor(topic)
    assertNull(partitionInfos)
  }

  @Test
  def testListOffsetsWithTopicDescribe() {
    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Describe)), topicResource)
    this.consumers.head.partitionsFor(topic)
  }

  @Test
  def testUnauthorizedDeleteWithoutDescribe() {
    val response = send(deleteTopicsRequest, ApiKeys.DELETE_TOPICS)
    val deleteResponse = DeleteTopicsResponse.parse(response)

    assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION, deleteResponse.errors.asScala.head._2)
  }

  @Test
  def testUnauthorizedDeleteWithDescribe() {
    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Describe)), deleteTopicResource)
    val response = send(deleteTopicsRequest, ApiKeys.DELETE_TOPICS)
    val deleteResponse = DeleteTopicsResponse.parse(response)

    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED, deleteResponse.errors.asScala.head._2)
  }

  @Test
  def testDeleteWithWildCardAuth() {
    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Delete)), new Resource(Topic, "*"))
    val response = send(deleteTopicsRequest, ApiKeys.DELETE_TOPICS)
    val deleteResponse = DeleteTopicsResponse.parse(response)

    assertEquals(Errors.NONE, deleteResponse.errors.asScala.head._2)
  }

  def removeAllAcls() = {
    servers.head.apis.authorizer.get.getAcls().keys.foreach { resource =>
      servers.head.apis.authorizer.get.removeAcls(resource)
      TestUtils.waitAndVerifyAcls(Set.empty[Acl], servers.head.apis.authorizer.get, resource)
    }
  }

  def sendRequestAndVerifyResponseErrorCode(apiKey: ApiKeys,
                                            request: AbstractRequest,
                                            resources: Set[ResourceType],
                                            isAuthorized: Boolean,
                                            isAuthorizedTopicDescribe: Boolean,
                                            topicExists: Boolean = true): AbstractResponse = {
    val resp = send(request, apiKey)
    val response = RequestKeyToResponseDeserializer(apiKey).getMethod("parse", classOf[ByteBuffer]).invoke(null, resp).asInstanceOf[AbstractResponse]
    val error = Errors.forCode(RequestKeyToErrorCode(apiKey).asInstanceOf[(AbstractResponse) => Short](response))

    val authorizationErrorCodes = resources.flatMap { resourceType =>
      if (resourceType == Topic) {
        if (isAuthorized)
          Set(Errors.UNKNOWN_TOPIC_OR_PARTITION, Errors.forCode(Topic.errorCode))
        else if (!isAuthorizedTopicDescribe)
          Set(Errors.UNKNOWN_TOPIC_OR_PARTITION)
        else
          Set(Errors.forCode(Topic.errorCode))
      } else {
        Set(Errors.forCode(resourceType.errorCode))
      }
    }

    if (topicExists)
      if (isAuthorized)
        assertFalse(s"$apiKey should be allowed. Found unexpected authorization error $error", authorizationErrorCodes.contains(error))
      else
        assertTrue(s"$apiKey should be forbidden. Found error $error but expected one of $authorizationErrorCodes", authorizationErrorCodes.contains(error))
    else if (resources == Set(Topic))
      assertEquals(s"$apiKey had an unexpected error", Errors.UNKNOWN_TOPIC_OR_PARTITION, error)
    else
      assertNotEquals(s"$apiKey had an unexpected error", Errors.TOPIC_AUTHORIZATION_FAILED, error)

    response
  }

  private def sendRecords(numRecords: Int, tp: TopicPartition) {
    val futures = (0 until numRecords).map { i =>
      this.producers.head.send(new ProducerRecord(tp.topic(), tp.partition(), i.toString.getBytes, i.toString.getBytes))
    }
    try {
      futures.foreach(_.get)
    } catch {
      case e: ExecutionException => throw e.getCause
    }
  }

  private def addAndVerifyAcls(acls: Set[Acl], resource: Resource) = {
    servers.head.apis.authorizer.get.addAcls(acls, resource)
    TestUtils.waitAndVerifyAcls(servers.head.apis.authorizer.get.getAcls(resource) ++ acls, servers.head.apis.authorizer.get, resource)
  }

  private def consumeRecords(consumer: Consumer[Array[Byte], Array[Byte]],
                             numRecords: Int = 1,
                             startingOffset: Int = 0,
                             topic: String = topic,
                             part: Int = part) {
    val records = new ArrayList[ConsumerRecord[Array[Byte], Array[Byte]]]()

    TestUtils.waitUntilTrue(() => {
      for (record <- consumer.poll(50).asScala)
        records.add(record)
      records.size == numRecords
    }, "Failed to receive all expected records from the consumer")

    for (i <- 0 until numRecords) {
      val record = records.get(i)
      val offset = startingOffset + i
      assertEquals(topic, record.topic())
      assertEquals(part, record.partition())
      assertEquals(offset.toLong, record.offset())
    }
  }

  private def sendOffsetFetchRequest(request: requests.OffsetFetchRequest,
                                     socketServer: SocketServer): requests.OffsetFetchResponse = {
    val response = send(request, ApiKeys.OFFSET_FETCH, socketServer)
    requests.OffsetFetchResponse.parse(response, request.version)
  }

}
