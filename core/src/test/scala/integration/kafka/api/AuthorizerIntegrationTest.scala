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

import java.io.{DataInputStream, DataOutputStream}
import java.net.Socket
import java.nio.ByteBuffer
import java.util.concurrent.ExecutionException
import java.util.{ArrayList, Collections, Properties}

import kafka.cluster.EndPoint
import kafka.common.TopicAndPartition
import kafka.coordinator.GroupCoordinator
import kafka.integration.KafkaServerTestHarness
import kafka.security.auth._
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.apache.kafka.clients.consumer.{Consumer, ConsumerRecord, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.errors._
import org.apache.kafka.common.protocol.{ApiKeys, Errors, SecurityProtocol}
import org.apache.kafka.common.requests._
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.{Node, TopicPartition, requests}
import org.junit.Assert._
import org.junit.{After, Assert, Before, Test}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.Buffer
import org.apache.kafka.common.internals.TopicConstants

class AuthorizerIntegrationTest extends KafkaServerTestHarness {
  val topic = "topic"
  val part = 0
  val brokerId: Integer = 0
  val correlationId = 0
  val clientId = "client-Id"
  val tp = new TopicPartition(topic, part)
  val topicAndPartition = new TopicAndPartition(topic, part)
  val group = "my-group"
  val topicResource = new Resource(Topic, topic)
  val groupResource = new Resource(Group, group)

  val GroupReadAcl = Map(groupResource -> Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Read)))
  val ClusterAcl = Map(Resource.ClusterResource -> Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, ClusterAction)))
  val TopicReadAcl = Map(topicResource -> Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Read)))
  val TopicWriteAcl = Map(topicResource -> Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Write)))
  val TopicDescribeAcl = Map(topicResource -> Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Describe)))

  val consumers = Buffer[KafkaConsumer[Array[Byte], Array[Byte]]]()
  val producers = Buffer[KafkaProducer[Array[Byte], Array[Byte]]]()

  val numServers = 1
  val producerCount = 1
  val consumerCount = 2
  val producerConfig = new Properties
  val numRecords = 1

  val overridingProps = new Properties()
  overridingProps.put(KafkaConfig.AuthorizerClassNameProp, classOf[SimpleAclAuthorizer].getName)
  overridingProps.put(KafkaConfig.BrokerIdProp, brokerId.toString)
  overridingProps.put(KafkaConfig.OffsetsTopicPartitionsProp, "1")

  val endPoint = new EndPoint("localhost", 0, SecurityProtocol.PLAINTEXT)

  val RequestKeyToResponseDeserializer: Map[Short, Class[_ <: Any]] =
    Map(ApiKeys.METADATA.id -> classOf[requests.MetadataResponse],
      ApiKeys.PRODUCE.id -> classOf[requests.ProduceResponse],
      ApiKeys.FETCH.id -> classOf[requests.FetchResponse],
      ApiKeys.LIST_OFFSETS.id -> classOf[requests.ListOffsetResponse],
      ApiKeys.OFFSET_COMMIT.id -> classOf[requests.OffsetCommitResponse],
      ApiKeys.OFFSET_FETCH.id -> classOf[requests.OffsetFetchResponse],
      ApiKeys.GROUP_COORDINATOR.id -> classOf[requests.GroupCoordinatorResponse],
      ApiKeys.UPDATE_METADATA_KEY.id -> classOf[requests.UpdateMetadataResponse],
      ApiKeys.JOIN_GROUP.id -> classOf[JoinGroupResponse],
      ApiKeys.SYNC_GROUP.id -> classOf[SyncGroupResponse],
      ApiKeys.HEARTBEAT.id -> classOf[HeartbeatResponse],
      ApiKeys.LEAVE_GROUP.id -> classOf[LeaveGroupResponse],
      ApiKeys.LEADER_AND_ISR.id -> classOf[requests.LeaderAndIsrResponse],
      ApiKeys.STOP_REPLICA.id -> classOf[requests.StopReplicaResponse],
      ApiKeys.CONTROLLED_SHUTDOWN_KEY.id -> classOf[requests.ControlledShutdownResponse]
    )

  val RequestKeyToErrorCode = Map[Short, (Nothing) => Short](
    ApiKeys.METADATA.id -> ((resp: requests.MetadataResponse) => resp.errors().asScala.find(_._1 == topic).getOrElse(("test", Errors.NONE))._2.code()),
    ApiKeys.PRODUCE.id -> ((resp: requests.ProduceResponse) => resp.responses().asScala.find(_._1 == tp).get._2.errorCode),
    ApiKeys.FETCH.id -> ((resp: requests.FetchResponse) => resp.responseData().asScala.find(_._1 == tp).get._2.errorCode),
    ApiKeys.LIST_OFFSETS.id -> ((resp: requests.ListOffsetResponse) => resp.responseData().asScala.find(_._1 == tp).get._2.errorCode),
    ApiKeys.OFFSET_COMMIT.id -> ((resp: requests.OffsetCommitResponse) => resp.responseData().asScala.find(_._1 == tp).get._2),
    ApiKeys.OFFSET_FETCH.id -> ((resp: requests.OffsetFetchResponse) => resp.responseData().asScala.find(_._1 == tp).get._2.errorCode),
    ApiKeys.GROUP_COORDINATOR.id -> ((resp: requests.GroupCoordinatorResponse) => resp.errorCode()),
    ApiKeys.UPDATE_METADATA_KEY.id -> ((resp: requests.UpdateMetadataResponse) => resp.errorCode()),
    ApiKeys.JOIN_GROUP.id -> ((resp: JoinGroupResponse) => resp.errorCode()),
    ApiKeys.SYNC_GROUP.id -> ((resp: SyncGroupResponse) => resp.errorCode()),
    ApiKeys.HEARTBEAT.id -> ((resp: HeartbeatResponse) => resp.errorCode()),
    ApiKeys.LEAVE_GROUP.id -> ((resp: LeaveGroupResponse) => resp.errorCode()),
    ApiKeys.LEADER_AND_ISR.id -> ((resp: requests.LeaderAndIsrResponse) => resp.responses().asScala.find(_._1 == tp).get._2),
    ApiKeys.STOP_REPLICA.id -> ((resp: requests.StopReplicaResponse) => resp.responses().asScala.find(_._1 == tp).get._2),
    ApiKeys.CONTROLLED_SHUTDOWN_KEY.id -> ((resp: requests.ControlledShutdownResponse) => resp.errorCode())
  )

  val RequestKeysToAcls = Map[Short, Map[Resource, Set[Acl]]](
    ApiKeys.METADATA.id -> TopicDescribeAcl,
    ApiKeys.PRODUCE.id -> TopicWriteAcl,
    ApiKeys.FETCH.id -> TopicReadAcl,
    ApiKeys.LIST_OFFSETS.id -> TopicDescribeAcl,
    ApiKeys.OFFSET_COMMIT.id -> (TopicReadAcl ++ GroupReadAcl),
    ApiKeys.OFFSET_FETCH.id -> (TopicReadAcl ++ GroupReadAcl),
    ApiKeys.GROUP_COORDINATOR.id -> (TopicReadAcl ++ GroupReadAcl),
    ApiKeys.UPDATE_METADATA_KEY.id -> ClusterAcl,
    ApiKeys.JOIN_GROUP.id -> GroupReadAcl,
    ApiKeys.SYNC_GROUP.id -> GroupReadAcl,
    ApiKeys.HEARTBEAT.id -> GroupReadAcl,
    ApiKeys.LEAVE_GROUP.id -> GroupReadAcl,
    ApiKeys.LEADER_AND_ISR.id -> ClusterAcl,
    ApiKeys.STOP_REPLICA.id -> ClusterAcl,
    ApiKeys.CONTROLLED_SHUTDOWN_KEY.id -> ClusterAcl
  )

  // configure the servers and clients
  override def generateConfigs() = TestUtils.createBrokerConfigs(1, zkConnect, enableControlledShutdown = false).map(KafkaConfig.fromProps(_, overridingProps))

  @Before
  override def setUp() {
    super.setUp()

    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, ClusterAction)), Resource.ClusterResource)

    for (i <- 0 until producerCount)
      producers += TestUtils.createNewProducer(TestUtils.getBrokerListStrFromServers(servers),
        acks = 1)
    for (i <- 0 until consumerCount)
      consumers += TestUtils.createNewConsumer(TestUtils.getBrokerListStrFromServers(servers), groupId = group, securityProtocol = SecurityProtocol.PLAINTEXT)

    // create the consumer offset topic
    TestUtils.createTopic(zkUtils, TopicConstants.GROUP_METADATA_TOPIC_NAME,
      1,
      1,
      servers,
      servers.head.consumerCoordinator.offsetsTopicConfigs)
    // create the test topic with all the brokers as replicas
    TestUtils.createTopic(zkUtils, topic, 1, 1, this.servers)
  }

  @After
  override def tearDown() = {
    producers.foreach(_.close())
    consumers.foreach(_.close())
    removeAllAcls
    super.tearDown()
  }

  private def createMetadataRequest = {
    new requests.MetadataRequest(List(topic).asJava)
  }

  private def createProduceRequest = {
    new requests.ProduceRequest(1, 5000, collection.mutable.Map(tp -> ByteBuffer.wrap("test".getBytes)).asJava)
  }

  private def createFetchRequest = {
    new requests.FetchRequest(5000, 100, Map(tp -> new requests.FetchRequest.PartitionData(0, 100)).asJava)
  }

  private def createListOffsetsRequest = {
    new requests.ListOffsetRequest(Map(tp -> new ListOffsetRequest.PartitionData(0, 100)).asJava)
  }

  private def createOffsetFetchRequest = {
    new requests.OffsetFetchRequest(group, List(tp).asJava)
  }

  private def createGroupCoordinatorRequest = {
    new requests.GroupCoordinatorRequest(group)
  }

  private def createUpdateMetadataRequest = {
    val partitionState = Map(tp -> new requests.UpdateMetadataRequest.PartitionState(Int.MaxValue, brokerId, Int.MaxValue, List(brokerId).asJava, 2, Set(brokerId).asJava)).asJava
    val brokers = Set(new requests.UpdateMetadataRequest.Broker(brokerId,
      Map(SecurityProtocol.PLAINTEXT -> new requests.UpdateMetadataRequest.EndPoint("localhost", 0)).asJava, null)).asJava
    new requests.UpdateMetadataRequest(brokerId, Int.MaxValue, partitionState, brokers)
  }

  private def createJoinGroupRequest = {
    new JoinGroupRequest(group, 30000, "", "consumer",
      List( new JoinGroupRequest.ProtocolMetadata("consumer-range",ByteBuffer.wrap("test".getBytes()))).asJava)
  }

  private def createSyncGroupRequest = {
    new SyncGroupRequest(group, 1, "", Map[String, ByteBuffer]().asJava)
  }

  private def createOffsetCommitRequest = {
    new requests.OffsetCommitRequest(group, 1, "", 1000, Map(tp -> new requests.OffsetCommitRequest.PartitionData(0, "metadata")).asJava)
  }

  private def createHeartbeatRequest = {
    new HeartbeatRequest(group, 1, "")
  }

  private def createLeaveGroupRequest = {
    new LeaveGroupRequest(group, "")
  }

  private def createLeaderAndIsrRequest = {
    new requests.LeaderAndIsrRequest(brokerId, Int.MaxValue,
      Map(tp -> new requests.LeaderAndIsrRequest.PartitionState(Int.MaxValue, brokerId, Int.MaxValue, List(brokerId).asJava, 2, Set(brokerId).asJava)).asJava,
      Set(new Node(brokerId, "localhost", 0)).asJava)
  }

  private def createStopReplicaRequest = {
    new requests.StopReplicaRequest(brokerId, Int.MaxValue, true, Set(tp).asJava)
  }

  private def createControlledShutdownRequest = {
    new requests.ControlledShutdownRequest(brokerId)
  }

  @Test
  def testAuthorization() {
    val requestKeyToRequest = mutable.LinkedHashMap[Short, AbstractRequest](
      ApiKeys.METADATA.id -> createMetadataRequest,
      ApiKeys.PRODUCE.id -> createProduceRequest,
      ApiKeys.FETCH.id -> createFetchRequest,
      ApiKeys.LIST_OFFSETS.id -> createListOffsetsRequest,
      ApiKeys.OFFSET_FETCH.id -> createOffsetFetchRequest,
      ApiKeys.GROUP_COORDINATOR.id -> createGroupCoordinatorRequest,
      ApiKeys.UPDATE_METADATA_KEY.id -> createUpdateMetadataRequest,
      ApiKeys.JOIN_GROUP.id -> createJoinGroupRequest,
      ApiKeys.SYNC_GROUP.id -> createSyncGroupRequest,
      ApiKeys.OFFSET_COMMIT.id -> createOffsetCommitRequest,
      ApiKeys.HEARTBEAT.id -> createHeartbeatRequest,
      ApiKeys.LEAVE_GROUP.id -> createLeaveGroupRequest,
      ApiKeys.LEADER_AND_ISR.id -> createLeaderAndIsrRequest,
      ApiKeys.STOP_REPLICA.id -> createStopReplicaRequest,
      ApiKeys.CONTROLLED_SHUTDOWN_KEY.id -> createControlledShutdownRequest
    )

    val socket = new Socket("localhost", servers.head.boundPort())

    for ((key, request) <- requestKeyToRequest) {
      removeAllAcls
      val resources = RequestKeysToAcls(key).map(_._1.resourceType).toSet
      sendRequestAndVerifyResponseErrorCode(socket, key, request, resources, isAuthorized = false)
      for ((resource, acls) <- RequestKeysToAcls(key))
        addAndVerifyAcls(acls, resource)
      sendRequestAndVerifyResponseErrorCode(socket, key, request, resources, isAuthorized = true)
    }
  }

  @Test
  def testProduceWithNoTopicAccess() {
    try {
      sendRecords(numRecords, tp)
      fail("sendRecords should have thrown")
    } catch {
      case e: TopicAuthorizationException =>
        assertEquals(Collections.singleton(topic), e.unauthorizedTopics())
    }
  }

  @Test
  def testProduceWithTopicDescribe() {
    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Describe)), topicResource)
    try {
      sendRecords(numRecords, tp)
      fail("sendRecords should have thrown")
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
      fail("sendRecords should have thrown")
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
  def testConsumeWithNoGroupAccess(): Unit = {
    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Write)), topicResource)
    sendRecords(1, tp)
    removeAllAcls()

    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Read)), topicResource)
    try {
      this.consumers.head.assign(List(tp).asJava)
      consumeRecords(this.consumers.head)
      Assert.fail("should have thrown exception")
    } catch {
      case e: GroupAuthorizationException => assertEquals(group, e.groupId())
    }
  }

  @Test
  def testConsumeWithNoTopicAccess() {
    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Write)), topicResource)
    sendRecords(1, tp)
    removeAllAcls()

    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Read)), groupResource)
    try {
      this.consumers.head.assign(List(tp).asJava)
      consumeRecords(this.consumers.head)
      Assert.fail("should have thrown exception")
    } catch {
      case e: TopicAuthorizationException => assertEquals(Collections.singleton(topic), e.unauthorizedTopics());
    }
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
      case e: TopicAuthorizationException => assertEquals(Collections.singleton(topic), e.unauthorizedTopics());
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
        assertEquals(Collections.singleton(topic), e.unauthorizedTopics());
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
  def testCreatePermissionNeededToReadFromNonExistentTopic() {
    val newTopic = "newTopic"
    val topicPartition = new TopicPartition(newTopic, 0)
    val newTopicResource = new Resource(Topic, newTopic)
    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Read)), newTopicResource)
    addAndVerifyAcls(GroupReadAcl(groupResource), groupResource)
    addAndVerifyAcls(ClusterAcl(Resource.ClusterResource), Resource.ClusterResource)
    try {
      this.consumers(0).assign(List(topicPartition).asJava)
      consumeRecords(this.consumers(0))
      Assert.fail("should have thrown exception")
    } catch {
      case e: TopicAuthorizationException =>
        assertEquals(Collections.singleton(newTopic), e.unauthorizedTopics());
    }

    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Write)), newTopicResource)
    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Create)), Resource.ClusterResource)

    sendRecords(numRecords, topicPartition)
    consumeRecords(this.consumers(0), topic = newTopic, part = 0)
  }

  @Test(expected = classOf[AuthorizationException])
  def testCommitWithNoAccess() {
    this.consumers.head.commitSync(Map(tp -> new OffsetAndMetadata(5)).asJava)
  }

  @Test(expected = classOf[TopicAuthorizationException])
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

  @Test(expected = classOf[TopicAuthorizationException])
  def testOffsetFetchWithNoTopicAccess() {
    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Read)), groupResource)
    this.consumers.head.assign(List(tp).asJava)
    this.consumers.head.position(tp)
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
    val e = intercept[TopicAuthorizationException] {
      this.consumers.head.partitionsFor(topic);
    }
    assertEquals(Set(topic), e.unauthorizedTopics().asScala)
  }

  @Test
  def testListOfsetsWithTopicDescribe() {
    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Describe)), topicResource)
    this.consumers.head.partitionsFor(topic);
  }

  def removeAllAcls() = {
    servers.head.apis.authorizer.get.getAcls().keys.foreach { resource =>
      servers.head.apis.authorizer.get.removeAcls(resource)
      TestUtils.waitAndVerifyAcls(Set.empty[Acl], servers.head.apis.authorizer.get, resource)
    }
  }

  def sendRequestAndVerifyResponseErrorCode(socket: Socket,
                                            key: Short,
                                            request: AbstractRequest,
                                            resources: Set[ResourceType],
                                            isAuthorized: Boolean): AbstractRequestResponse = {
    val header = new RequestHeader(key, "client", 1)
    val body = request.toStruct

    val buffer = ByteBuffer.allocate(header.sizeOf() + body.sizeOf())
    header.writeTo(buffer)
    body.writeTo(buffer)
    buffer.rewind()
    val requestBytes = buffer.array()

    sendRequest(socket, key, requestBytes)
    val resp = receiveResponse(socket)
    ResponseHeader.parse(resp)

    val response = RequestKeyToResponseDeserializer(key).getMethod("parse", classOf[ByteBuffer]).invoke(null, resp).asInstanceOf[AbstractRequestResponse]
    val errorCode = RequestKeyToErrorCode(key).asInstanceOf[(AbstractRequestResponse) => Short](response)

    val possibleErrorCodes = resources.map(_.errorCode)
    if (isAuthorized)
      assertFalse(s"${ApiKeys.forId(key)} should be allowed", possibleErrorCodes.contains(errorCode))
    else
      assertTrue(s"${ApiKeys.forId(key)} should be forbidden", possibleErrorCodes.contains(errorCode))

    response
  }

  private def sendRequest(socket: Socket, id: Short, request: Array[Byte]) {
    val outgoing = new DataOutputStream(socket.getOutputStream)
    outgoing.writeInt(request.length)
    outgoing.write(request)
    outgoing.flush()
  }

  private def receiveResponse(socket: Socket): ByteBuffer = {
    val incoming = new DataInputStream(socket.getInputStream)
    val len = incoming.readInt()
    val response = new Array[Byte](len)
    incoming.readFully(response)
    ByteBuffer.wrap(response)
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

  private def removeAndVerifyAcls(acls: Set[Acl], resource: Resource) = {
    servers.head.apis.authorizer.get.removeAcls(acls, resource)
    TestUtils.waitAndVerifyAcls(servers.head.apis.authorizer.get.getAcls(resource) -- acls, servers.head.apis.authorizer.get, resource)
  }


  private def consumeRecords(consumer: Consumer[Array[Byte], Array[Byte]],
                             numRecords: Int = 1,
                             startingOffset: Int = 0,
                             topic: String = topic,
                             part: Int = part) {
    val records = new ArrayList[ConsumerRecord[Array[Byte], Array[Byte]]]()
    val maxIters = numRecords * 50
    var iters = 0
    while (records.size < numRecords) {
      for (record <- consumer.poll(50).asScala) {
        records.add(record)
      }
      if (iters > maxIters)
        throw new IllegalStateException("Failed to consume the expected records after " + iters + " iterations.")
      iters += 1
    }
    for (i <- 0 until numRecords) {
      val record = records.get(i)
      val offset = startingOffset + i
      assertEquals(topic, record.topic())
      assertEquals(part, record.partition())
      assertEquals(offset.toLong, record.offset())
    }
  }
}
