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
import java.time.Duration

import kafka.admin.AdminClient
import kafka.admin.ConsumerGroupCommand.{ConsumerGroupCommandOptions, KafkaConsumerGroupService}
import kafka.common.TopicAndPartition
import kafka.log.LogConfig
import kafka.network.SocketServer
import kafka.security.auth._
import kafka.server.{BaseRequestTest, KafkaConfig}
import kafka.utils.TestUtils
import org.apache.kafka.clients.admin.NewPartitions
import org.apache.kafka.clients.consumer._
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.acl.{AccessControlEntry, AccessControlEntryFilter, AclBinding, AclBindingFilter, AclOperation, AclPermissionType}
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.errors._
import org.apache.kafka.common.internals.Topic.GROUP_METADATA_TOPIC_NAME
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record.{CompressionType, MemoryRecords, Records, SimpleRecord}
import org.apache.kafka.common.requests.CreateAclsRequest.AclCreation
import org.apache.kafka.common.requests.CreateTopicsRequest.TopicDetails
import org.apache.kafka.common.requests._
import org.apache.kafka.common.resource.ResourceNameType.LITERAL
import org.apache.kafka.common.resource.{ResourcePattern, ResourcePatternFilter, ResourceType => AdminResourceType}
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.apache.kafka.common.{KafkaException, Node, TopicPartition, requests}
import org.junit.Assert._
import org.junit.{After, Assert, Before, Test}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.Buffer

class AuthorizerIntegrationTest extends BaseRequestTest {

  override def numBrokers: Int = 1
  val brokerId: Integer = 0
  def userPrincipal = KafkaPrincipal.ANONYMOUS

  val topic = "topic"
  val topicPattern = "topic.*"
  val createTopic = "topic-new"
  val deleteTopic = "topic-delete"
  val transactionalId = "transactional.id"
  val producerId = 83392L
  val part = 0
  val correlationId = 0
  val clientId = "client-Id"
  val tp = new TopicPartition(topic, part)
  val logDir = "logDir"
  val deleteRecordsPartition = new TopicPartition(deleteTopic, part)
  val topicAndPartition = TopicAndPartition(topic, part)
  val group = "my-group"
  val topicResource = Resource(Topic, topic, LITERAL)
  val groupResource = Resource(Group, group, LITERAL)
  val deleteTopicResource = Resource(Topic, deleteTopic, LITERAL)
  val transactionalIdResource = Resource(TransactionalId, transactionalId, LITERAL)
  val createTopicResource = Resource(Topic, createTopic, LITERAL)

  val groupReadAcl = Map(groupResource -> Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Read)))
  val groupDescribeAcl = Map(groupResource -> Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Describe)))
  val groupDeleteAcl = Map(groupResource -> Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Delete)))
  val clusterAcl = Map(Resource.ClusterResource -> Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, ClusterAction)))
  val clusterCreateAcl = Map(Resource.ClusterResource -> Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Create)))
  val clusterAlterAcl = Map(Resource.ClusterResource -> Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Alter)))
  val clusterDescribeAcl = Map(Resource.ClusterResource -> Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Describe)))
  val clusterIdempotentWriteAcl = Map(Resource.ClusterResource -> Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, IdempotentWrite)))
  val topicCreateAcl = Map(createTopicResource -> Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Create)))
  val topicReadAcl = Map(topicResource -> Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Read)))
  val topicWriteAcl = Map(topicResource -> Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Write)))
  val topicDescribeAcl = Map(topicResource -> Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Describe)))
  val topicAlterAcl = Map(topicResource -> Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Alter)))
  val topicDeleteAcl = Map(deleteTopicResource -> Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Delete)))
  val topicDescribeConfigsAcl = Map(topicResource -> Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, DescribeConfigs)))
  val topicAlterConfigsAcl = Map(topicResource -> Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, AlterConfigs)))
  val transactionIdWriteAcl = Map(transactionalIdResource -> Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Write)))
  val transactionalIdDescribeAcl = Map(transactionalIdResource -> Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Describe)))


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
    properties.put(KafkaConfig.TransactionsTopicPartitionsProp, "1")
    properties.put(KafkaConfig.TransactionsTopicReplicationFactorProp, "1")
    properties.put(KafkaConfig.TransactionsTopicMinISRProp, "1")
  }

  val requestKeyToResponseDeserializer: Map[ApiKeys, Class[_ <: Any]] =
    Map(ApiKeys.METADATA -> classOf[requests.MetadataResponse],
      ApiKeys.PRODUCE -> classOf[requests.ProduceResponse],
      ApiKeys.FETCH -> classOf[requests.FetchResponse[Records]],
      ApiKeys.LIST_OFFSETS -> classOf[requests.ListOffsetResponse],
      ApiKeys.OFFSET_COMMIT -> classOf[requests.OffsetCommitResponse],
      ApiKeys.OFFSET_FETCH -> classOf[requests.OffsetFetchResponse],
      ApiKeys.FIND_COORDINATOR -> classOf[FindCoordinatorResponse],
      ApiKeys.UPDATE_METADATA -> classOf[requests.UpdateMetadataResponse],
      ApiKeys.JOIN_GROUP -> classOf[JoinGroupResponse],
      ApiKeys.SYNC_GROUP -> classOf[SyncGroupResponse],
      ApiKeys.DESCRIBE_GROUPS -> classOf[DescribeGroupsResponse],
      ApiKeys.HEARTBEAT -> classOf[HeartbeatResponse],
      ApiKeys.LEAVE_GROUP -> classOf[LeaveGroupResponse],
      ApiKeys.DELETE_GROUPS -> classOf[DeleteGroupsResponse],
      ApiKeys.LEADER_AND_ISR -> classOf[requests.LeaderAndIsrResponse],
      ApiKeys.STOP_REPLICA -> classOf[requests.StopReplicaResponse],
      ApiKeys.CONTROLLED_SHUTDOWN -> classOf[requests.ControlledShutdownResponse],
      ApiKeys.CREATE_TOPICS -> classOf[CreateTopicsResponse],
      ApiKeys.DELETE_TOPICS -> classOf[requests.DeleteTopicsResponse],
      ApiKeys.DELETE_RECORDS -> classOf[requests.DeleteRecordsResponse],
      ApiKeys.OFFSET_FOR_LEADER_EPOCH -> classOf[OffsetsForLeaderEpochResponse],
      ApiKeys.DESCRIBE_CONFIGS -> classOf[DescribeConfigsResponse],
      ApiKeys.ALTER_CONFIGS -> classOf[AlterConfigsResponse],
      ApiKeys.INIT_PRODUCER_ID -> classOf[InitProducerIdResponse],
      ApiKeys.WRITE_TXN_MARKERS -> classOf[WriteTxnMarkersResponse],
      ApiKeys.ADD_PARTITIONS_TO_TXN -> classOf[AddPartitionsToTxnResponse],
      ApiKeys.ADD_OFFSETS_TO_TXN -> classOf[AddOffsetsToTxnResponse],
      ApiKeys.END_TXN -> classOf[EndTxnResponse],
      ApiKeys.TXN_OFFSET_COMMIT -> classOf[TxnOffsetCommitResponse],
      ApiKeys.CREATE_ACLS -> classOf[CreateAclsResponse],
      ApiKeys.DELETE_ACLS -> classOf[DeleteAclsResponse],
      ApiKeys.DESCRIBE_ACLS -> classOf[DescribeAclsResponse],
      ApiKeys.ALTER_REPLICA_LOG_DIRS -> classOf[AlterReplicaLogDirsResponse],
      ApiKeys.DESCRIBE_LOG_DIRS -> classOf[DescribeLogDirsResponse],
      ApiKeys.CREATE_PARTITIONS -> classOf[CreatePartitionsResponse]
  )

  val requestKeyToError = Map[ApiKeys, Nothing => Errors](
    ApiKeys.METADATA -> ((resp: requests.MetadataResponse) => resp.errors.asScala.find(_._1 == topic).getOrElse(("test", Errors.NONE))._2),
    ApiKeys.PRODUCE -> ((resp: requests.ProduceResponse) => resp.responses.asScala.find(_._1 == tp).get._2.error),
    ApiKeys.FETCH -> ((resp: requests.FetchResponse[Records]) => resp.responseData.asScala.find(_._1 == tp).get._2.error),
    ApiKeys.LIST_OFFSETS -> ((resp: requests.ListOffsetResponse) => resp.responseData.asScala.find(_._1 == tp).get._2.error),
    ApiKeys.OFFSET_COMMIT -> ((resp: requests.OffsetCommitResponse) => resp.responseData.asScala.find(_._1 == tp).get._2),
    ApiKeys.OFFSET_FETCH -> ((resp: requests.OffsetFetchResponse) => resp.error),
    ApiKeys.FIND_COORDINATOR -> ((resp: FindCoordinatorResponse) => resp.error),
    ApiKeys.UPDATE_METADATA -> ((resp: requests.UpdateMetadataResponse) => resp.error),
    ApiKeys.JOIN_GROUP -> ((resp: JoinGroupResponse) => resp.error),
    ApiKeys.SYNC_GROUP -> ((resp: SyncGroupResponse) => resp.error),
    ApiKeys.DESCRIBE_GROUPS -> ((resp: DescribeGroupsResponse) => resp.groups.get(group).error),
    ApiKeys.HEARTBEAT -> ((resp: HeartbeatResponse) => resp.error),
    ApiKeys.LEAVE_GROUP -> ((resp: LeaveGroupResponse) => resp.error),
    ApiKeys.DELETE_GROUPS -> ((resp: DeleteGroupsResponse) => resp.get(group)),
    ApiKeys.LEADER_AND_ISR -> ((resp: requests.LeaderAndIsrResponse) => resp.responses.asScala.find(_._1 == tp).get._2),
    ApiKeys.STOP_REPLICA -> ((resp: requests.StopReplicaResponse) => resp.responses.asScala.find(_._1 == tp).get._2),
    ApiKeys.CONTROLLED_SHUTDOWN -> ((resp: requests.ControlledShutdownResponse) => resp.error),
    ApiKeys.CREATE_TOPICS -> ((resp: CreateTopicsResponse) => resp.errors.asScala.find(_._1 == createTopic).get._2.error),
    ApiKeys.DELETE_TOPICS -> ((resp: requests.DeleteTopicsResponse) => resp.errors.asScala.find(_._1 == deleteTopic).get._2),
    ApiKeys.DELETE_RECORDS -> ((resp: requests.DeleteRecordsResponse) => resp.responses.get(deleteRecordsPartition).error),
    ApiKeys.OFFSET_FOR_LEADER_EPOCH -> ((resp: OffsetsForLeaderEpochResponse) => resp.responses.get(tp).error),
    ApiKeys.DESCRIBE_CONFIGS -> ((resp: DescribeConfigsResponse) =>
      resp.configs.get(new ConfigResource(ConfigResource.Type.TOPIC, tp.topic)).error.error),
    ApiKeys.ALTER_CONFIGS -> ((resp: AlterConfigsResponse) =>
      resp.errors.get(new ConfigResource(ConfigResource.Type.TOPIC, tp.topic)).error),
    ApiKeys.INIT_PRODUCER_ID -> ((resp: InitProducerIdResponse) => resp.error),
    ApiKeys.WRITE_TXN_MARKERS -> ((resp: WriteTxnMarkersResponse) => resp.errors(producerId).get(tp)),
    ApiKeys.ADD_PARTITIONS_TO_TXN -> ((resp: AddPartitionsToTxnResponse) => resp.errors.get(tp)),
    ApiKeys.ADD_OFFSETS_TO_TXN -> ((resp: AddOffsetsToTxnResponse) => resp.error),
    ApiKeys.END_TXN -> ((resp: EndTxnResponse) => resp.error),
    ApiKeys.TXN_OFFSET_COMMIT -> ((resp: TxnOffsetCommitResponse) => resp.errors.get(tp)),
    ApiKeys.CREATE_ACLS -> ((resp: CreateAclsResponse) => resp.aclCreationResponses.asScala.head.error.error),
    ApiKeys.DESCRIBE_ACLS -> ((resp: DescribeAclsResponse) => resp.error.error),
    ApiKeys.DELETE_ACLS -> ((resp: DeleteAclsResponse) => resp.responses.asScala.head.error.error),
    ApiKeys.ALTER_REPLICA_LOG_DIRS -> ((resp: AlterReplicaLogDirsResponse) => resp.responses.get(tp)),
    ApiKeys.DESCRIBE_LOG_DIRS -> ((resp: DescribeLogDirsResponse) =>
      if (resp.logDirInfos.size() > 0) resp.logDirInfos.asScala.head._2.error else Errors.CLUSTER_AUTHORIZATION_FAILED),
    ApiKeys.CREATE_PARTITIONS -> ((resp: CreatePartitionsResponse) => resp.errors.asScala.find(_._1 == topic).get._2.error)
  )

  val requestKeysToAcls = Map[ApiKeys, Map[Resource, Set[Acl]]](
    ApiKeys.METADATA -> topicDescribeAcl,
    ApiKeys.PRODUCE -> (topicWriteAcl ++ transactionIdWriteAcl ++ clusterIdempotentWriteAcl),
    ApiKeys.FETCH -> topicReadAcl,
    ApiKeys.LIST_OFFSETS -> topicDescribeAcl,
    ApiKeys.OFFSET_COMMIT -> (topicReadAcl ++ groupReadAcl),
    ApiKeys.OFFSET_FETCH -> (topicReadAcl ++ groupDescribeAcl),
    ApiKeys.FIND_COORDINATOR -> (topicReadAcl ++ groupDescribeAcl ++ transactionalIdDescribeAcl),
    ApiKeys.UPDATE_METADATA -> clusterAcl,
    ApiKeys.JOIN_GROUP -> groupReadAcl,
    ApiKeys.SYNC_GROUP -> groupReadAcl,
    ApiKeys.DESCRIBE_GROUPS -> groupDescribeAcl,
    ApiKeys.HEARTBEAT -> groupReadAcl,
    ApiKeys.LEAVE_GROUP -> groupReadAcl,
    ApiKeys.DELETE_GROUPS -> groupDeleteAcl,
    ApiKeys.LEADER_AND_ISR -> clusterAcl,
    ApiKeys.STOP_REPLICA -> clusterAcl,
    ApiKeys.CONTROLLED_SHUTDOWN -> clusterAcl,
    ApiKeys.CREATE_TOPICS -> topicCreateAcl,
    ApiKeys.DELETE_TOPICS -> topicDeleteAcl,
    ApiKeys.DELETE_RECORDS -> topicDeleteAcl,
    ApiKeys.OFFSET_FOR_LEADER_EPOCH -> clusterAcl,
    ApiKeys.DESCRIBE_CONFIGS -> topicDescribeConfigsAcl,
    ApiKeys.ALTER_CONFIGS -> topicAlterConfigsAcl,
    ApiKeys.INIT_PRODUCER_ID -> (transactionIdWriteAcl ++ clusterIdempotentWriteAcl),
    ApiKeys.WRITE_TXN_MARKERS -> clusterAcl,
    ApiKeys.ADD_PARTITIONS_TO_TXN -> (topicWriteAcl ++ transactionIdWriteAcl),
    ApiKeys.ADD_OFFSETS_TO_TXN -> (groupReadAcl ++ transactionIdWriteAcl),
    ApiKeys.END_TXN -> transactionIdWriteAcl,
    ApiKeys.TXN_OFFSET_COMMIT -> (groupReadAcl ++ transactionIdWriteAcl),
    ApiKeys.CREATE_ACLS -> clusterAlterAcl,
    ApiKeys.DESCRIBE_ACLS -> clusterDescribeAcl,
    ApiKeys.DELETE_ACLS -> clusterAlterAcl,
    ApiKeys.ALTER_REPLICA_LOG_DIRS -> clusterAlterAcl,
    ApiKeys.DESCRIBE_LOG_DIRS -> clusterDescribeAcl,
    ApiKeys.CREATE_PARTITIONS -> topicAlterAcl

  )

  @Before
  override def setUp() {
    super.setUp()

    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, ClusterAction)), Resource.ClusterResource)

    for (_ <- 0 until producerCount)
      producers += TestUtils.createNewProducer(TestUtils.getBrokerListStrFromServers(servers),
        maxBlockMs = 3000,
        acks = 1)

    for (_ <- 0 until consumerCount)
      consumers += TestUtils.createNewConsumer(TestUtils.getBrokerListStrFromServers(servers), groupId = group, securityProtocol = SecurityProtocol.PLAINTEXT)

    // create the consumer offset topic
    createTopic(GROUP_METADATA_TOPIC_NAME, topicConfig = servers.head.groupCoordinator.offsetsTopicConfigs)
    // create the test topic with all the brokers as replicas
    createTopic(topic)
    createTopic(deleteTopic)
  }

  @After
  override def tearDown() = {
    producers.foreach(_.close())
    consumers.foreach(_.wakeup())
    consumers.foreach(_.close())
    removeAllAcls()
    super.tearDown()
  }

  private def createMetadataRequest(allowAutoTopicCreation: Boolean) = {
    new requests.MetadataRequest.Builder(List(topic).asJava, allowAutoTopicCreation).build()
  }

  private def createProduceRequest = {
    requests.ProduceRequest.Builder.forCurrentMagic(1, 5000,
      collection.mutable.Map(tp -> MemoryRecords.withRecords(CompressionType.NONE, new SimpleRecord("test".getBytes))).asJava).
      build()
  }

  private def createFetchRequest = {
    val partitionMap = new util.LinkedHashMap[TopicPartition, requests.FetchRequest.PartitionData]
    partitionMap.put(tp, new requests.FetchRequest.PartitionData(0, 0, 100))
    requests.FetchRequest.Builder.forConsumer(100, Int.MaxValue, partitionMap).build()
  }

  private def createFetchFollowerRequest = {
    val partitionMap = new util.LinkedHashMap[TopicPartition, requests.FetchRequest.PartitionData]
    partitionMap.put(tp, new requests.FetchRequest.PartitionData(0, 0, 100))
    val version = ApiKeys.FETCH.latestVersion
    requests.FetchRequest.Builder.forReplica(version, 5000, 100, Int.MaxValue, partitionMap).build()
  }

  private def createListOffsetsRequest = {
    requests.ListOffsetRequest.Builder.forConsumer(false, IsolationLevel.READ_UNCOMMITTED).setTargetTimes(
      Map(tp -> (0L: java.lang.Long)).asJava).
      build()
  }

  private def offsetsForLeaderEpochRequest = {
    new OffsetsForLeaderEpochRequest.Builder(ApiKeys.OFFSET_FOR_LEADER_EPOCH.latestVersion()).add(tp, 7).build()
  }

  private def createOffsetFetchRequest = {
    new requests.OffsetFetchRequest.Builder(group, List(tp).asJava).build()
  }

  private def createFindCoordinatorRequest = {
    new FindCoordinatorRequest.Builder(FindCoordinatorRequest.CoordinatorType.GROUP, group).build()
  }

  private def createUpdateMetadataRequest = {
    val partitionState = Map(tp -> new UpdateMetadataRequest.PartitionState(
      Int.MaxValue, brokerId, Int.MaxValue, List(brokerId).asJava, 2, Seq(brokerId).asJava, Seq.empty[Integer].asJava)).asJava
    val securityProtocol = SecurityProtocol.PLAINTEXT
    val brokers = Set(new requests.UpdateMetadataRequest.Broker(brokerId,
      Seq(new requests.UpdateMetadataRequest.EndPoint("localhost", 0, securityProtocol,
        ListenerName.forSecurityProtocol(securityProtocol))).asJava, null)).asJava
    val version = ApiKeys.UPDATE_METADATA.latestVersion
    new requests.UpdateMetadataRequest.Builder(version, brokerId, Int.MaxValue, partitionState, brokers).build()
  }

  private def createJoinGroupRequest = {
    new JoinGroupRequest.Builder(group, 10000, "", "consumer",
      List( new JoinGroupRequest.ProtocolMetadata("consumer-range",ByteBuffer.wrap("test".getBytes()))).asJava)
      .setRebalanceTimeout(60000).build()
  }

  private def createSyncGroupRequest = {
    new SyncGroupRequest.Builder(group, 1, "", Map[String, ByteBuffer]().asJava).build()
  }

  private def createDescribeGroupsRequest = {
    new DescribeGroupsRequest.Builder(List(group).asJava).build()
  }

  private def createOffsetCommitRequest = {
    new requests.OffsetCommitRequest.Builder(
      group, Map(tp -> new requests.OffsetCommitRequest.PartitionData(0, "metadata")).asJava).
      setMemberId("").setGenerationId(1).setRetentionTime(1000).
      build()
  }

  private def createPartitionsRequest = {
    new CreatePartitionsRequest.Builder(
      Map(topic -> NewPartitions.increaseTo(10)).asJava, 10000, true
    ).build()
  }

  private def heartbeatRequest = new HeartbeatRequest.Builder(group, 1, "").build()

  private def leaveGroupRequest = new LeaveGroupRequest.Builder(group, "").build()

  private def deleteGroupsRequest = new DeleteGroupsRequest.Builder(Set(group).asJava).build()

  private def leaderAndIsrRequest = {
    new requests.LeaderAndIsrRequest.Builder(ApiKeys.LEADER_AND_ISR.latestVersion, brokerId, Int.MaxValue,
      Map(tp -> new LeaderAndIsrRequest.PartitionState(Int.MaxValue, brokerId, Int.MaxValue, List(brokerId).asJava, 2, Seq(brokerId).asJava, false)).asJava,
      Set(new Node(brokerId, "localhost", 0)).asJava).build()
  }

  private def stopReplicaRequest = new StopReplicaRequest.Builder(brokerId, Int.MaxValue, true, Set(tp).asJava).build()

  private def controlledShutdownRequest = new requests.ControlledShutdownRequest.Builder(brokerId,
    ApiKeys.CONTROLLED_SHUTDOWN.latestVersion).build()

  private def createTopicsRequest =
    new CreateTopicsRequest.Builder(Map(createTopic -> new TopicDetails(1, 1.toShort)).asJava, 0).build()

  private def deleteTopicsRequest = new DeleteTopicsRequest.Builder(Set(deleteTopic).asJava, 5000).build()

  private def deleteRecordsRequest = new DeleteRecordsRequest.Builder(5000, Collections.singletonMap(deleteRecordsPartition, 0L)).build()

  private def describeConfigsRequest =
    new DescribeConfigsRequest.Builder(Collections.singleton(new ConfigResource(ConfigResource.Type.TOPIC, tp.topic))).build()

  private def alterConfigsRequest =
    new AlterConfigsRequest.Builder(
      Collections.singletonMap(new ConfigResource(ConfigResource.Type.TOPIC, tp.topic),
        new AlterConfigsRequest.Config(Collections.singleton(
          new AlterConfigsRequest.ConfigEntry(LogConfig.MaxMessageBytesProp, "1000000")
        ))), true).build()

  private def describeAclsRequest = new DescribeAclsRequest.Builder(AclBindingFilter.ANY).build()

  private def createAclsRequest = new CreateAclsRequest.Builder(
    Collections.singletonList(new AclCreation(new AclBinding(
      new ResourcePattern(AdminResourceType.TOPIC, "mytopic", LITERAL),
      new AccessControlEntry(userPrincipal.toString, "*", AclOperation.WRITE, AclPermissionType.DENY))))).build()

  private def deleteAclsRequest = new DeleteAclsRequest.Builder(
    Collections.singletonList(new AclBindingFilter(
      new ResourcePatternFilter(AdminResourceType.TOPIC, null, LITERAL),
      new AccessControlEntryFilter(userPrincipal.toString, "*", AclOperation.ANY, AclPermissionType.DENY)))).build()

  private def alterReplicaLogDirsRequest = new AlterReplicaLogDirsRequest.Builder(Collections.singletonMap(tp, logDir)).build()

  private def describeLogDirsRequest = new DescribeLogDirsRequest.Builder(Collections.singleton(tp)).build()

  private def addPartitionsToTxnRequest = new AddPartitionsToTxnRequest.Builder(transactionalId, 1, 1, Collections.singletonList(tp)).build()

  private def addOffsetsToTxnRequest = new AddOffsetsToTxnRequest.Builder(transactionalId, 1, 1, group).build()


  @Test
  def testAuthorizationWithTopicExisting() {
    val requestKeyToRequest = mutable.LinkedHashMap[ApiKeys, AbstractRequest](
      ApiKeys.METADATA -> createMetadataRequest(allowAutoTopicCreation = true),
      ApiKeys.PRODUCE -> createProduceRequest,
      ApiKeys.FETCH -> createFetchRequest,
      ApiKeys.LIST_OFFSETS -> createListOffsetsRequest,
      ApiKeys.OFFSET_FETCH -> createOffsetFetchRequest,
      ApiKeys.FIND_COORDINATOR -> createFindCoordinatorRequest,
      ApiKeys.UPDATE_METADATA -> createUpdateMetadataRequest,
      ApiKeys.JOIN_GROUP -> createJoinGroupRequest,
      ApiKeys.SYNC_GROUP -> createSyncGroupRequest,
      ApiKeys.DESCRIBE_GROUPS -> createDescribeGroupsRequest,
      ApiKeys.OFFSET_COMMIT -> createOffsetCommitRequest,
      ApiKeys.HEARTBEAT -> heartbeatRequest,
      ApiKeys.LEAVE_GROUP -> leaveGroupRequest,
      ApiKeys.LEADER_AND_ISR -> leaderAndIsrRequest,
      ApiKeys.STOP_REPLICA -> stopReplicaRequest,
      ApiKeys.CONTROLLED_SHUTDOWN -> controlledShutdownRequest,
      ApiKeys.CREATE_TOPICS -> createTopicsRequest,
      ApiKeys.DELETE_TOPICS -> deleteTopicsRequest,
      ApiKeys.DELETE_RECORDS -> deleteRecordsRequest,
      ApiKeys.OFFSET_FOR_LEADER_EPOCH -> offsetsForLeaderEpochRequest,
      ApiKeys.DESCRIBE_CONFIGS -> describeConfigsRequest,
      ApiKeys.ALTER_CONFIGS -> alterConfigsRequest,
      ApiKeys.CREATE_ACLS -> createAclsRequest,
      ApiKeys.DELETE_ACLS -> deleteAclsRequest,
      ApiKeys.DESCRIBE_ACLS -> describeAclsRequest,
      ApiKeys.ALTER_REPLICA_LOG_DIRS -> alterReplicaLogDirsRequest,
      ApiKeys.DESCRIBE_LOG_DIRS -> describeLogDirsRequest,
      ApiKeys.CREATE_PARTITIONS -> createPartitionsRequest,
      ApiKeys.ADD_PARTITIONS_TO_TXN -> addPartitionsToTxnRequest,
      ApiKeys.ADD_OFFSETS_TO_TXN -> addOffsetsToTxnRequest
    )

    for ((key, request) <- requestKeyToRequest) {
      removeAllAcls()
      val resources = requestKeysToAcls(key).map(_._1.resourceType).toSet
      sendRequestAndVerifyResponseError(key, request, resources, isAuthorized = false)

      val resourceToAcls = requestKeysToAcls(key)
      resourceToAcls.get(topicResource).foreach { acls =>
        val describeAcls = topicDescribeAcl(topicResource)
        val isAuthorized =  describeAcls == acls
        addAndVerifyAcls(describeAcls, topicResource)
        sendRequestAndVerifyResponseError(key, request, resources, isAuthorized = isAuthorized)
        removeAllAcls()
      }

      for ((resource, acls) <- resourceToAcls)
        addAndVerifyAcls(acls, resource)
      sendRequestAndVerifyResponseError(key, request, resources, isAuthorized = true)
    }
  }

  /*
   * even if the topic doesn't exist, request APIs should not leak the topic name
   */
  @Test
  def testAuthorizationWithTopicNotExisting() {
    adminZkClient.deleteTopic(topic)
    TestUtils.verifyTopicDeletion(zkClient, topic, 1, servers)
    adminZkClient.deleteTopic(deleteTopic)
    TestUtils.verifyTopicDeletion(zkClient, deleteTopic, 1, servers)

    val requestKeyToRequest = mutable.LinkedHashMap[ApiKeys, AbstractRequest](
      ApiKeys.METADATA -> createMetadataRequest(allowAutoTopicCreation = false),
      ApiKeys.PRODUCE -> createProduceRequest,
      ApiKeys.FETCH -> createFetchRequest,
      ApiKeys.LIST_OFFSETS -> createListOffsetsRequest,
      ApiKeys.OFFSET_COMMIT -> createOffsetCommitRequest,
      ApiKeys.OFFSET_FETCH -> createOffsetFetchRequest,
      ApiKeys.DELETE_TOPICS -> deleteTopicsRequest,
      ApiKeys.DELETE_RECORDS -> deleteRecordsRequest,
      ApiKeys.ADD_PARTITIONS_TO_TXN -> addPartitionsToTxnRequest,
      ApiKeys.ADD_OFFSETS_TO_TXN -> addOffsetsToTxnRequest,
      ApiKeys.CREATE_PARTITIONS -> createPartitionsRequest,
      ApiKeys.DELETE_GROUPS -> deleteGroupsRequest
    )

    for ((key, request) <- requestKeyToRequest) {
      removeAllAcls()
      val resources = requestKeysToAcls(key).map(_._1.resourceType).toSet
      sendRequestAndVerifyResponseError(key, request, resources, isAuthorized = false, topicExists = false)

      val resourceToAcls = requestKeysToAcls(key)
      resourceToAcls.get(topicResource).foreach { acls =>
        val describeAcls = topicDescribeAcl(topicResource)
        val isAuthorized = describeAcls == acls
        addAndVerifyAcls(describeAcls, topicResource)
        sendRequestAndVerifyResponseError(key, request, resources, isAuthorized = isAuthorized, topicExists = false)
        removeAllAcls()
      }

      for ((resource, acls) <- resourceToAcls)
        addAndVerifyAcls(acls, resource)
      sendRequestAndVerifyResponseError(key, request, resources, isAuthorized = true, topicExists = false)
    }
  }

  @Test
  def testCreateTopicAuthorizationWithClusterCreate() {
    removeAllAcls()
    val resources = Set[ResourceType](Topic)

    sendRequestAndVerifyResponseError(ApiKeys.CREATE_TOPICS, createTopicsRequest, resources, isAuthorized = false)

    for ((resource, acls) <- clusterCreateAcl)
      addAndVerifyAcls(acls, resource)
    sendRequestAndVerifyResponseError(ApiKeys.CREATE_TOPICS, createTopicsRequest, resources, isAuthorized = true)
  }

  @Test
  def testFetchFollowerRequest() {
    val key = ApiKeys.FETCH
    val request = createFetchFollowerRequest

    removeAllAcls()
    val resources = Set(topicResource.resourceType, Resource.ClusterResource.resourceType)
    sendRequestAndVerifyResponseError(key, request, resources, isAuthorized = false)

    val readAcls = topicReadAcl.get(topicResource).get
    addAndVerifyAcls(readAcls, topicResource)
    sendRequestAndVerifyResponseError(key, request, resources, isAuthorized = false)

    val clusterAcls = clusterAcl.get(Resource.ClusterResource).get
    addAndVerifyAcls(clusterAcls, Resource.ClusterResource)
    sendRequestAndVerifyResponseError(key, request, resources, isAuthorized = true)
  }

  @Test
  def testProduceWithNoTopicAccess() {
    try {
      sendRecords(numRecords, tp)
      fail("should have thrown exception")
    } catch {
      case _: TopicAuthorizationException => //expected
    }
  }

  @Test
  def testProduceWithTopicDescribe() {
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Describe)), topicResource)
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
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Read)), topicResource)
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
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Write)), topicResource)
    sendRecords(numRecords, tp)
  }

  @Test
  def testCreatePermissionOnTopicToWriteToNonExistentTopic() {
    testCreatePermissionNeededToWriteToNonExistentTopic(Topic)
  }

  @Test
  def testCreatePermissionOnClusterToWriteToNonExistentTopic() {
    testCreatePermissionNeededToWriteToNonExistentTopic(Cluster)
  }

  private def testCreatePermissionNeededToWriteToNonExistentTopic(resType: ResourceType) {
    val topicPartition = new TopicPartition(createTopic, 0)
    val newTopicResource = Resource(Topic, createTopic, LITERAL)
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Write)), newTopicResource)
    try {
      sendRecords(numRecords, topicPartition)
      Assert.fail("should have thrown exception")
    } catch {
      case e: TopicAuthorizationException =>
        assertEquals(Collections.singleton(createTopic), e.unauthorizedTopics())
    }

    val resource = if (resType == Topic) newTopicResource else Resource.ClusterResource
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Create)), resource)

    sendRecords(numRecords, topicPartition)
  }

  @Test(expected = classOf[TopicAuthorizationException])
  def testConsumeUsingAssignWithNoAccess(): Unit = {
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Write)), topicResource)
    sendRecords(1, tp)
    removeAllAcls()
    this.consumers.head.assign(List(tp).asJava)
    consumeRecords(this.consumers.head)
  }

  @Test
  def testSimpleConsumeWithOffsetLookupAndNoGroupAccess(): Unit = {
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Write)), topicResource)
    sendRecords(1, tp)
    removeAllAcls()

    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Read)), topicResource)
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
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Write)), topicResource)
    sendRecords(1, tp)
    removeAllAcls()

    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Read)), topicResource)

    // in this case, we do an explicit seek, so there should be no need to query the coordinator at all
    this.consumers.head.assign(List(tp).asJava)
    this.consumers.head.seekToBeginning(List(tp).asJava)
    consumeRecords(this.consumers.head)
  }

  @Test(expected = classOf[KafkaException])
  def testConsumeWithoutTopicDescribeAccess() {
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Write)), topicResource)
    sendRecords(1, tp)
    removeAllAcls()

    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Read)), groupResource)
    this.consumers.head.assign(List(tp).asJava)

    // the consumer should raise an exception if it receives UNKNOWN_TOPIC_OR_PARTITION
    // from the ListOffsets response when looking up the initial position.
    consumeRecords(this.consumers.head)
  }

  @Test
  def testConsumeWithTopicDescribe() {
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Write)), topicResource)
    sendRecords(1, tp)
    removeAllAcls()

    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Describe)), topicResource)
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Read)), groupResource)
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
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Write)), topicResource)
    sendRecords(1, tp)
    removeAllAcls()

    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Write)), topicResource)
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Read)), groupResource)
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
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Write)), topicResource)
    sendRecords(1, tp)
    removeAllAcls()

    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Read)), topicResource)
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Read)), groupResource)
    this.consumers.head.assign(List(tp).asJava)
    consumeRecords(this.consumers.head)
  }

  @Test
  def testPatternSubscriptionWithNoTopicAccess() {
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Write)), topicResource)
    sendRecords(1, tp)
    removeAllAcls()

    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Read)), groupResource)
    this.consumers.head.subscribe(Pattern.compile(topicPattern), new NoOpConsumerRebalanceListener)
    this.consumers.head.poll(50)
    assertTrue(this.consumers.head.subscription.isEmpty)
  }

  @Test
  def testPatternSubscriptionWithTopicDescribeOnlyAndGroupRead() {
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Write)), topicResource)
    sendRecords(1, tp)
    removeAllAcls()

    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Describe)), topicResource)
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Read)), groupResource)
    val consumer = consumers.head
    consumer.subscribe(Pattern.compile(topicPattern))
    try {
      consumeRecords(consumer)
      Assert.fail("Expected TopicAuthorizationException")
    } catch {
      case _: TopicAuthorizationException => //expected
    }
  }

  @Test
  def testPatternSubscriptionWithTopicAndGroupRead() {
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Write)), topicResource)
    sendRecords(1, tp)

    // create an unmatched topic
    val unmatchedTopic = "unmatched"
    createTopic(unmatchedTopic)
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Write)),  Resource(Topic, unmatchedTopic, LITERAL))
    sendRecords(1, new TopicPartition(unmatchedTopic, part))
    removeAllAcls()

    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Read)), topicResource)
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Read)), groupResource)
    val consumer = consumers.head
    consumer.subscribe(Pattern.compile(topicPattern))
    consumeRecords(consumer)

    // set the subscription pattern to an internal topic that the consumer has read permission to. Since
    // internal topics are not included, we should not be assigned any partitions from this topic
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Read)),  Resource(Topic,
      GROUP_METADATA_TOPIC_NAME, LITERAL))
    consumer.subscribe(Pattern.compile(GROUP_METADATA_TOPIC_NAME))
    consumer.poll(0)
    assertTrue(consumer.subscription().isEmpty)
    assertTrue(consumer.assignment().isEmpty)
  }

  @Test
  def testPatternSubscriptionMatchingInternalTopic() {
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Write)), topicResource)
    sendRecords(1, tp)
    removeAllAcls()

    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Read)), topicResource)
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Read)), groupResource)

    val consumerConfig = new Properties
    consumerConfig.put(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG, "false")
    val consumer = TestUtils.createNewConsumer(TestUtils.getBrokerListStrFromServers(servers), groupId = group,
      securityProtocol = SecurityProtocol.PLAINTEXT, props = Some(consumerConfig))
    try {
      // ensure that internal topics are not included if no permission
      consumer.subscribe(Pattern.compile(".*"))
      consumeRecords(consumer)
      assertEquals(Set(topic).asJava, consumer.subscription)

      // now authorize the user for the internal topic and verify that we can subscribe
      addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Read)), Resource(Topic,
        GROUP_METADATA_TOPIC_NAME, LITERAL))
      consumer.subscribe(Pattern.compile(GROUP_METADATA_TOPIC_NAME))
      consumer.poll(0)
      assertEquals(Set(GROUP_METADATA_TOPIC_NAME), consumer.subscription.asScala)
    } finally consumer.close()
  }

  @Test
  def testPatternSubscriptionMatchingInternalTopicWithDescribeOnlyPermission() {
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Write)), topicResource)
    sendRecords(1, tp)
    removeAllAcls()

    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Read)), topicResource)
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Read)), groupResource)
    val internalTopicResource = Resource(Topic, GROUP_METADATA_TOPIC_NAME, LITERAL)
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Describe)), internalTopicResource)

    val consumerConfig = new Properties
    consumerConfig.put(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG, "false")
    val consumer = TestUtils.createNewConsumer(TestUtils.getBrokerListStrFromServers(servers), groupId = group,
      securityProtocol = SecurityProtocol.PLAINTEXT, props = Some(consumerConfig))
    try {
      consumer.subscribe(Pattern.compile(".*"))
      // It is possible that the first call returns records of "topic" and the second call throws TopicAuthorizationException
      consumeRecords(consumer)
      consumeRecords(consumer)
      Assert.fail("Expected TopicAuthorizationException")
    } catch {
      case _: TopicAuthorizationException => //expected
    } finally consumer.close()
  }

  @Test
  def testPatternSubscriptionNotMatchingInternalTopic() {
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Write)), topicResource)
    sendRecords(1, tp)
    removeAllAcls()

    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Read)), topicResource)
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Read)), groupResource)

    val consumerConfig = new Properties
    consumerConfig.put(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG, "false")
    val consumer = TestUtils.createNewConsumer(TestUtils.getBrokerListStrFromServers(servers), groupId = group,
      securityProtocol = SecurityProtocol.PLAINTEXT, props = Some(consumerConfig))
    try {
      consumer.subscribe(Pattern.compile(topicPattern))
      consumeRecords(consumer)
    } finally consumer.close()
}

  @Test
  def testCreatePermissionOnTopicToReadFromNonExistentTopic() {
    testCreatePermissionNeededToReadFromNonExistentTopic("newTopic",
      Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Create)),
      Topic)
  }

  @Test
  def testCreatePermissionOnClusterToReadFromNonExistentTopic() {
    testCreatePermissionNeededToReadFromNonExistentTopic("newTopic",
      Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Create)),
      Cluster)
  }

  private def testCreatePermissionNeededToReadFromNonExistentTopic(newTopic: String, acls: Set[Acl], resType: ResourceType) {
    val topicPartition = new TopicPartition(newTopic, 0)
    val newTopicResource = Resource(Topic, newTopic, LITERAL)
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Read)), newTopicResource)
    addAndVerifyAcls(groupReadAcl(groupResource), groupResource)
    this.consumers.head.assign(List(topicPartition).asJava)
    val unauthorizedTopics = intercept[TopicAuthorizationException] {
      (0 until 10).foreach(_ => consumers.head.poll(Duration.ofMillis(50L)))
    }.unauthorizedTopics
    assertEquals(Collections.singleton(newTopic), unauthorizedTopics)

    val resource = if (resType == Topic) newTopicResource else Resource.ClusterResource
    addAndVerifyAcls(acls, resource)

    TestUtils.waitUntilTrue(() => {
      this.consumers.head.poll(Duration.ofMillis(50L))
      this.zkClient.topicExists(newTopic)
    }, "Expected topic was not created")
  }

  @Test(expected = classOf[AuthorizationException])
  def testCommitWithNoAccess() {
    this.consumers.head.commitSync(Map(tp -> new OffsetAndMetadata(5)).asJava)
  }

  @Test(expected = classOf[KafkaException])
  def testCommitWithNoTopicAccess() {
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Read)), groupResource)
    this.consumers.head.commitSync(Map(tp -> new OffsetAndMetadata(5)).asJava)
  }

  @Test(expected = classOf[TopicAuthorizationException])
  def testCommitWithTopicWrite() {
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Read)), groupResource)
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Write)), topicResource)
    this.consumers.head.commitSync(Map(tp -> new OffsetAndMetadata(5)).asJava)
  }

  @Test(expected = classOf[TopicAuthorizationException])
  def testCommitWithTopicDescribe() {
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Read)), groupResource)
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Describe)), topicResource)
    this.consumers.head.commitSync(Map(tp -> new OffsetAndMetadata(5)).asJava)
  }

  @Test(expected = classOf[GroupAuthorizationException])
  def testCommitWithNoGroupAccess() {
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Read)), topicResource)
    this.consumers.head.commitSync(Map(tp -> new OffsetAndMetadata(5)).asJava)
  }

  @Test
  def testCommitWithTopicAndGroupRead() {
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Read)), groupResource)
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Read)), topicResource)
    this.consumers.head.commitSync(Map(tp -> new OffsetAndMetadata(5)).asJava)
  }

  @Test(expected = classOf[AuthorizationException])
  def testOffsetFetchWithNoAccess() {
    this.consumers.head.assign(List(tp).asJava)
    this.consumers.head.position(tp)
  }

  @Test(expected = classOf[GroupAuthorizationException])
  def testOffsetFetchWithNoGroupAccess() {
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Read)), topicResource)
    this.consumers.head.assign(List(tp).asJava)
    this.consumers.head.position(tp)
  }

  @Test(expected = classOf[KafkaException])
  def testOffsetFetchWithNoTopicAccess() {
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Read)), groupResource)
    this.consumers.head.assign(List(tp).asJava)
    this.consumers.head.position(tp)
  }

  @Test
  def testFetchAllOffsetsTopicAuthorization() {
    val offset = 15L
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Read)), groupResource)
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Read)), topicResource)
    this.consumers.head.assign(List(tp).asJava)
    this.consumers.head.commitSync(Map(tp -> new OffsetAndMetadata(offset)).asJava)

    removeAllAcls()
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Read)), groupResource)

    // send offset fetch requests directly since the consumer does not expose an API to do so
    // note there's only one broker, so no need to lookup the group coordinator

    // without describe permission on the topic, we shouldn't be able to fetch offsets
    val offsetFetchRequest = requests.OffsetFetchRequest.forAllPartitions(group)
    var offsetFetchResponse = sendOffsetFetchRequest(offsetFetchRequest, anySocketServer)
    assertEquals(Errors.NONE, offsetFetchResponse.error)
    assertTrue(offsetFetchResponse.responseData.isEmpty)

    // now add describe permission on the topic and verify that the offset can be fetched
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Describe)), topicResource)
    offsetFetchResponse = sendOffsetFetchRequest(offsetFetchRequest, anySocketServer)
    assertEquals(Errors.NONE, offsetFetchResponse.error)
    assertTrue(offsetFetchResponse.responseData.containsKey(tp))
    assertEquals(offset, offsetFetchResponse.responseData.get(tp).offset)
  }

  @Test
  def testOffsetFetchTopicDescribe() {
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Describe)), groupResource)
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Describe)), topicResource)
    this.consumers.head.assign(List(tp).asJava)
    this.consumers.head.position(tp)
  }

  @Test
  def testOffsetFetchWithTopicAndGroupRead() {
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Read)), groupResource)
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Read)), topicResource)
    this.consumers.head.assign(List(tp).asJava)
    this.consumers.head.position(tp)
  }

  @Test(expected = classOf[TopicAuthorizationException])
  def testListOffsetsWithNoTopicAccess() {
    this.consumers.head.partitionsFor(topic)
  }

  @Test
  def testListOffsetsWithTopicDescribe() {
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Describe)), topicResource)
    this.consumers.head.partitionsFor(topic)
  }

  @Test(expected = classOf[GroupAuthorizationException])
  def testDescribeGroupApiWithNoGroupAcl() {
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Describe)), topicResource)
    AdminClient.createSimplePlaintext(brokerList).describeConsumerGroup(group)
  }

  @Test
  def testDescribeGroupApiWithGroupDescribe() {
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Describe)), groupResource)
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Describe)), topicResource)
    AdminClient.createSimplePlaintext(brokerList).describeConsumerGroup(group)
  }

  @Test
  def testDescribeGroupCliWithGroupDescribe() {
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Describe)), groupResource)
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Describe)), topicResource)

    val cgcArgs = Array("--bootstrap-server", brokerList, "--describe", "--group", group)
    val opts = new ConsumerGroupCommandOptions(cgcArgs)
    val consumerGroupService = new KafkaConsumerGroupService(opts)
    consumerGroupService.describeGroup()
    consumerGroupService.close()
  }

  @Test
  def testDeleteGroupApiWithDeleteGroupAcl() {
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Read)), groupResource)
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Read)), topicResource)
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Delete)), groupResource)
    this.consumers.head.assign(List(tp).asJava)
    this.consumers.head.commitSync(Map(tp -> new OffsetAndMetadata(5, "")).asJava)
    val result = AdminClient.createSimplePlaintext(brokerList).deleteConsumerGroups(List(group))
    assert(result.size == 1 && result.keySet.contains(group) && result.get(group).contains(Errors.NONE))
  }

  @Test
  def testDeleteGroupApiWithNoDeleteGroupAcl() {
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Read)), groupResource)
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Read)), topicResource)
    this.consumers.head.assign(List(tp).asJava)
    this.consumers.head.commitSync(Map(tp -> new OffsetAndMetadata(5, "")).asJava)
    val result = AdminClient.createSimplePlaintext(brokerList).deleteConsumerGroups(List(group))
    assert(result.size == 1 && result.keySet.contains(group) && result.get(group).contains(Errors.GROUP_AUTHORIZATION_FAILED))
  }

  @Test
  def testDeleteGroupApiWithNoDeleteGroupAcl2() {
    val result = AdminClient.createSimplePlaintext(brokerList).deleteConsumerGroups(List(group))
    assert(result.size == 1 && result.keySet.contains(group) && result.get(group).contains(Errors.GROUP_AUTHORIZATION_FAILED))
  }

  @Test
  def testUnauthorizedDeleteTopicsWithoutDescribe() {
    val response = connectAndSend(deleteTopicsRequest, ApiKeys.DELETE_TOPICS)
    val version = ApiKeys.DELETE_TOPICS.latestVersion
    val deleteResponse = DeleteTopicsResponse.parse(response, version)
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED, deleteResponse.errors.asScala.head._2)
  }

  @Test
  def testUnauthorizedDeleteTopicsWithDescribe() {
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Describe)), deleteTopicResource)
    val response = connectAndSend(deleteTopicsRequest, ApiKeys.DELETE_TOPICS)
    val version = ApiKeys.DELETE_TOPICS.latestVersion
    val deleteResponse = DeleteTopicsResponse.parse(response, version)

    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED, deleteResponse.errors.asScala.head._2)
  }

  @Test
  def testDeleteTopicsWithWildCardAuth() {
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Delete)), Resource(Topic, "*", LITERAL))
    val response = connectAndSend(deleteTopicsRequest, ApiKeys.DELETE_TOPICS)
    val version = ApiKeys.DELETE_TOPICS.latestVersion
    val deleteResponse = DeleteTopicsResponse.parse(response, version)

    assertEquals(Errors.NONE, deleteResponse.errors.asScala.head._2)
  }

  @Test
  def testUnauthorizedDeleteRecordsWithoutDescribe() {
    val response = connectAndSend(deleteRecordsRequest, ApiKeys.DELETE_RECORDS)
    val version = ApiKeys.DELETE_RECORDS.latestVersion
    val deleteRecordsResponse = DeleteRecordsResponse.parse(response, version)
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED, deleteRecordsResponse.responses.asScala.head._2.error)
  }

  @Test
  def testUnauthorizedDeleteRecordsWithDescribe() {
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Describe)), deleteTopicResource)
    val response = connectAndSend(deleteRecordsRequest, ApiKeys.DELETE_RECORDS)
    val version = ApiKeys.DELETE_RECORDS.latestVersion
    val deleteRecordsResponse = DeleteRecordsResponse.parse(response, version)
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED, deleteRecordsResponse.responses.asScala.head._2.error)
  }

  @Test
  def testDeleteRecordsWithWildCardAuth() {
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Delete)), Resource(Topic, "*", LITERAL))
    val response = connectAndSend(deleteRecordsRequest, ApiKeys.DELETE_RECORDS)
    val version = ApiKeys.DELETE_RECORDS.latestVersion
    val deleteRecordsResponse = DeleteRecordsResponse.parse(response, version)

    assertEquals(Errors.NONE, deleteRecordsResponse.responses.asScala.head._2.error)
  }

  @Test
  def testUnauthorizedCreatePartitions() {
    val response = connectAndSend(createPartitionsRequest, ApiKeys.CREATE_PARTITIONS)
    val version = ApiKeys.CREATE_PARTITIONS.latestVersion
    val createPartitionsResponse = CreatePartitionsResponse.parse(response, version)
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED, createPartitionsResponse.errors.asScala.head._2.error)
  }

  @Test
  def testCreatePartitionsWithWildCardAuth() {
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Alter)), Resource(Topic, "*", LITERAL))
    val response = connectAndSend(createPartitionsRequest, ApiKeys.CREATE_PARTITIONS)
    val version = ApiKeys.CREATE_PARTITIONS.latestVersion
    val createPartitionsResponse = CreatePartitionsResponse.parse(response, version)
    assertEquals(Errors.NONE, createPartitionsResponse.errors.asScala.head._2.error)
  }

  @Test(expected = classOf[TransactionalIdAuthorizationException])
  def testTransactionalProducerInitTransactionsNoWriteTransactionalIdAcl(): Unit = {
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Describe)), transactionalIdResource)
    val producer = buildTransactionalProducer()
    producer.initTransactions()
  }

  @Test(expected = classOf[TransactionalIdAuthorizationException])
  def testTransactionalProducerInitTransactionsNoDescribeTransactionalIdAcl(): Unit = {
    val producer = buildTransactionalProducer()
    producer.initTransactions()
  }

  @Test
  def testSendOffsetsWithNoConsumerGroupDescribeAccess(): Unit = {
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, ClusterAction)), Resource.ClusterResource)
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Write)), topicResource)
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Write)), transactionalIdResource)
    val producer = buildTransactionalProducer()
    producer.initTransactions()
    producer.beginTransaction()
    try {
      producer.sendOffsetsToTransaction(Map(new TopicPartition(topic, 0) -> new OffsetAndMetadata(0L)).asJava, group)
      fail("Should have raised GroupAuthorizationException")
    } catch {
      case e: GroupAuthorizationException =>
    }
  }

  @Test
  def testSendOffsetsWithNoConsumerGroupWriteAccess(): Unit = {
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Write)), transactionalIdResource)
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Describe)), groupResource)
    val producer = buildTransactionalProducer()
    producer.initTransactions()
    producer.beginTransaction()
    try {
      producer.sendOffsetsToTransaction(Map(new TopicPartition(topic, 0) -> new OffsetAndMetadata(0L)).asJava, group)
      fail("Should have raised GroupAuthorizationException")
    } catch {
      case e: GroupAuthorizationException =>
    }
  }

  @Test
  def testIdempotentProducerNoIdempotentWriteAclInInitProducerId(): Unit = {
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Write)), topicResource)
    val producer = buildIdempotentProducer()
    try {
      // the InitProducerId is sent asynchronously, so we expect the error either in the callback
      // or raised from send itself
      producer.send(new ProducerRecord[Array[Byte], Array[Byte]](topic, "hi".getBytes)).get()
      fail("Should have raised ClusterAuthorizationException")
    } catch {
      case e: ExecutionException =>
        assertTrue(e.getCause.isInstanceOf[ClusterAuthorizationException])
    }
    try {
      // the second time, the call to send itself should fail (the producer becomes unusable
      // if no producerId can be obtained)
      producer.send(new ProducerRecord[Array[Byte], Array[Byte]](topic, "hi".getBytes)).get()
      fail("Should have raised ClusterAuthorizationException")
    } catch {
      case e: ExecutionException =>
        assertTrue(e.getCause.isInstanceOf[ClusterAuthorizationException])
    }
  }

  @Test
  def testIdempotentProducerNoIdempotentWriteAclInProduce(): Unit = {
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Write)), topicResource)
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, IdempotentWrite)), Resource.ClusterResource)

    val producer = buildIdempotentProducer()

    // first send should be fine since we have permission to get a ProducerId
    producer.send(new ProducerRecord[Array[Byte], Array[Byte]](topic, "hi".getBytes)).get()

    // revoke the IdempotentWrite permission
    removeAllAcls()
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Write)), topicResource)

    try {
      // the send should now fail with a cluster auth error
      producer.send(new ProducerRecord[Array[Byte], Array[Byte]](topic, "hi".getBytes)).get()
      fail("Should have raised ClusterAuthorizationException")
    } catch {
      case e: ExecutionException =>
        assertTrue(e.getCause.isInstanceOf[ClusterAuthorizationException])
    }
    try {
      // the second time, the call to send itself should fail (the producer becomes unusable
      // if no producerId can be obtained)
      producer.send(new ProducerRecord[Array[Byte], Array[Byte]](topic, "hi".getBytes)).get()
      fail("Should have raised ClusterAuthorizationException")
    } catch {
      case e: ExecutionException =>
        assertTrue(e.getCause.isInstanceOf[ClusterAuthorizationException])
    }
  }

  @Test
  def shouldInitTransactionsWhenAclSet(): Unit = {
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Write)), transactionalIdResource)
    val producer = buildTransactionalProducer()
    producer.initTransactions()
  }

  @Test
  def testTransactionalProducerTopicAuthorizationExceptionInSendCallback(): Unit = {
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Write)), transactionalIdResource)
    // add describe access so that we can fetch metadata
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Describe)), topicResource)
    val producer = buildTransactionalProducer()
    producer.initTransactions()
    producer.beginTransaction()
    try {
      producer.send(new ProducerRecord(tp.topic, tp.partition, "1".getBytes, "1".getBytes)).get
      Assert.fail("expected TopicAuthorizationException")
    } catch {
      case e: ExecutionException =>
        e.getCause match {
          case cause: TopicAuthorizationException =>
            assertEquals(Set(topic), cause.unauthorizedTopics().asScala)
          case other =>
            fail("Unexpected failure cause in send callback")
        }
    }
  }

  @Test
  def testTransactionalProducerTopicAuthorizationExceptionInCommit(): Unit = {
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Write)), transactionalIdResource)
    // add describe access so that we can fetch metadata
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Describe)), topicResource)
    val producer = buildTransactionalProducer()
    producer.initTransactions()
    producer.beginTransaction()
    try {
      producer.send(new ProducerRecord(tp.topic, tp.partition, "1".getBytes, "1".getBytes))
      producer.commitTransaction()
      Assert.fail("expected TopicAuthorizationException")
    } catch {
      case e: TopicAuthorizationException =>
        assertEquals(Set(topic), e.unauthorizedTopics().asScala)
    }
  }

  @Test
  def shouldThrowTransactionalIdAuthorizationExceptionWhenNoTransactionAccessDuringSend(): Unit = {
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Write)), transactionalIdResource)
    val producer = buildTransactionalProducer()
    producer.initTransactions()
    removeAllAcls()
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Write)), topicResource)
    try {
      producer.beginTransaction()
      producer.send(new ProducerRecord(tp.topic, tp.partition, "1".getBytes, "1".getBytes)).get
      Assert.fail("expected TransactionalIdAuthorizationException")
    } catch {
      case e: ExecutionException => assertTrue(s"expected TransactionalIdAuthorizationException, but got ${e.getCause}",
        e.getCause.isInstanceOf[TransactionalIdAuthorizationException])
    }
  }

  @Test
  def shouldThrowTransactionalIdAuthorizationExceptionWhenNoTransactionAccessOnEndTransaction(): Unit = {
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Write)), transactionalIdResource)
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Write)), topicResource)
    val producer = buildTransactionalProducer()
    producer.initTransactions()
    producer.beginTransaction()
    producer.send(new ProducerRecord(tp.topic, tp.partition, "1".getBytes, "1".getBytes)).get
    removeAllAcls()
    try {
      producer.commitTransaction()
      Assert.fail("expected TransactionalIdAuthorizationException")
    } catch {
      case _: TransactionalIdAuthorizationException => // ok
    }
  }

  @Test
  def shouldSuccessfullyAbortTransactionAfterTopicAuthorizationException(): Unit = {
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Write)), transactionalIdResource)
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Write)), topicResource)
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Describe)), Resource(Topic, deleteTopic, LITERAL))
    val producer = buildTransactionalProducer()
    producer.initTransactions()
    producer.beginTransaction()
    producer.send(new ProducerRecord(tp.topic, tp.partition, "1".getBytes, "1".getBytes)).get
    // try and add a partition resulting in TopicAuthorizationException
    try {
      producer.send(new ProducerRecord(deleteTopic, 0, "1".getBytes, "1".getBytes)).get
    } catch {
      case e: ExecutionException =>
        assertTrue(e.getCause.isInstanceOf[TopicAuthorizationException])
    }
    // now rollback
    producer.abortTransaction()
  }

  @Test
  def shouldThrowTransactionalIdAuthorizationExceptionWhenNoTransactionAccessOnSendOffsetsToTxn(): Unit = {
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Write)), transactionalIdResource)
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Write)), groupResource)
    val producer = buildTransactionalProducer()
    producer.initTransactions()
    producer.beginTransaction()
    removeAllAcls()
    try {
      val offsets: util.Map[TopicPartition, OffsetAndMetadata] = Map(new TopicPartition(tp.topic, tp.partition) -> new OffsetAndMetadata(1L)).asJava
      producer.sendOffsetsToTransaction(offsets, group)
      Assert.fail("expected TransactionalIdAuthorizationException")
    } catch {
      case _: TransactionalIdAuthorizationException => // ok
    }
  }

  @Test
  def shouldSendSuccessfullyWhenIdempotentAndHasCorrectACL(): Unit = {
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, IdempotentWrite)), Resource.ClusterResource)
    addAndVerifyAcls(Set(new Acl(userPrincipal, Allow, Acl.WildCardHost, Write)), topicResource)
    val producer = buildIdempotentProducer()
    producer.send(new ProducerRecord(tp.topic, tp.partition, "1".getBytes, "1".getBytes)).get
  }

  def removeAllAcls() = {
    servers.head.apis.authorizer.get.getAcls().keys.foreach { resource =>
      servers.head.apis.authorizer.get.removeAcls(resource)
      TestUtils.waitAndVerifyAcls(Set.empty[Acl], servers.head.apis.authorizer.get, resource)
    }
  }

  def sendRequestAndVerifyResponseError(apiKey: ApiKeys,
                                        request: AbstractRequest,
                                        resources: Set[ResourceType],
                                        isAuthorized: Boolean,
                                        topicExists: Boolean = true): AbstractResponse = {
    val resp = connectAndSend(request, apiKey)
    val response = requestKeyToResponseDeserializer(apiKey).getMethod("parse", classOf[ByteBuffer], classOf[Short]).invoke(
      null, resp, request.version: java.lang.Short).asInstanceOf[AbstractResponse]
    val error = requestKeyToError(apiKey).asInstanceOf[(AbstractResponse) => Errors](response)

    val authorizationErrors = resources.flatMap { resourceType =>
      if (resourceType == Topic) {
        if (isAuthorized)
          Set(Errors.UNKNOWN_TOPIC_OR_PARTITION, Topic.error)
        else
          Set(Topic.error)
      } else {
        Set(resourceType.error)
      }
    }

    if (topicExists)
      if (isAuthorized)
        assertFalse(s"$apiKey should be allowed. Found unexpected authorization error $error", authorizationErrors.contains(error))
      else
        assertTrue(s"$apiKey should be forbidden. Found error $error but expected one of $authorizationErrors", authorizationErrors.contains(error))
    else if (resources == Set(Topic))
      if (isAuthorized)
        assertEquals(s"$apiKey had an unexpected error", Errors.UNKNOWN_TOPIC_OR_PARTITION, error)
      else
        assertEquals(s"$apiKey had an unexpected error", Errors.TOPIC_AUTHORIZATION_FAILED, error)

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
    val response = connectAndSend(request, ApiKeys.OFFSET_FETCH, socketServer)
    requests.OffsetFetchResponse.parse(response, request.version)
  }

  private def buildTransactionalProducer(): KafkaProducer[Array[Byte], Array[Byte]] = {
    val transactionalProperties = new Properties()
    transactionalProperties.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId)
    val producer = TestUtils.createNewProducer(TestUtils.getBrokerListStrFromServers(servers),
      retries = 3,
      props = Some(transactionalProperties))
    producers += producer
    producer
  }

  private def buildIdempotentProducer(): KafkaProducer[Array[Byte], Array[Byte]] = {
    val idempotentProperties = new Properties()
    idempotentProperties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    val producer = TestUtils.createNewProducer(TestUtils.getBrokerListStrFromServers(servers),
      retries = 3,
      props = Some(idempotentProperties))
    producers += producer
    producer
  }

}
