/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.server

import kafka.network.SocketServer
import kafka.utils.TestUtils
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.{TopicCollection, TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.common.message.DeleteGroupsResponseData.{DeletableGroupResult, DeletableGroupResultCollection}
import org.apache.kafka.common.message.LeaveGroupRequestData.MemberIdentity
import org.apache.kafka.common.message.LeaveGroupResponseData.MemberResponse
import org.apache.kafka.common.message.SyncGroupRequestData.SyncGroupRequestAssignment
import org.apache.kafka.common.message.WriteTxnMarkersRequestData.{WritableTxnMarker, WritableTxnMarkerTopic}
import org.apache.kafka.common.message.{AddOffsetsToTxnRequestData, AddOffsetsToTxnResponseData, ConsumerGroupDescribeRequestData, ConsumerGroupDescribeResponseData, ConsumerGroupHeartbeatRequestData, ConsumerGroupHeartbeatResponseData, DeleteGroupsRequestData, DeleteGroupsResponseData, DescribeGroupsRequestData, DescribeGroupsResponseData, EndTxnRequestData, HeartbeatRequestData, HeartbeatResponseData, InitProducerIdRequestData, JoinGroupRequestData, JoinGroupResponseData, LeaveGroupResponseData, ListGroupsRequestData, ListGroupsResponseData, OffsetCommitRequestData, OffsetCommitResponseData, OffsetDeleteRequestData, OffsetDeleteResponseData, OffsetFetchRequestData, OffsetFetchResponseData, ShareGroupDescribeRequestData, ShareGroupDescribeResponseData, ShareGroupHeartbeatRequestData, ShareGroupHeartbeatResponseData, StreamsGroupDescribeRequestData, StreamsGroupDescribeResponseData, StreamsGroupHeartbeatRequestData, StreamsGroupHeartbeatResponseData, SyncGroupRequestData, SyncGroupResponseData, TxnOffsetCommitRequestData, TxnOffsetCommitResponseData, WriteTxnMarkersRequestData}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{AbstractRequest, AbstractResponse, AddOffsetsToTxnRequest, AddOffsetsToTxnResponse, ConsumerGroupDescribeRequest, ConsumerGroupDescribeResponse, ConsumerGroupHeartbeatRequest, ConsumerGroupHeartbeatResponse, DeleteGroupsRequest, DeleteGroupsResponse, DescribeGroupsRequest, DescribeGroupsResponse, EndTxnRequest, EndTxnResponse, HeartbeatRequest, HeartbeatResponse, InitProducerIdRequest, InitProducerIdResponse, JoinGroupRequest, JoinGroupResponse, LeaveGroupRequest, LeaveGroupResponse, ListGroupsRequest, ListGroupsResponse, OffsetCommitRequest, OffsetCommitResponse, OffsetDeleteRequest, OffsetDeleteResponse, OffsetFetchRequest, OffsetFetchResponse, ShareGroupDescribeRequest, ShareGroupDescribeResponse, ShareGroupHeartbeatRequest, ShareGroupHeartbeatResponse, StreamsGroupDescribeRequest, StreamsGroupDescribeResponse, StreamsGroupHeartbeatRequest, StreamsGroupHeartbeatResponse, SyncGroupRequest, SyncGroupResponse, TxnOffsetCommitRequest, TxnOffsetCommitResponse, WriteTxnMarkersRequest, WriteTxnMarkersResponse}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.common.test.ClusterInstance
import org.apache.kafka.common.utils.ProducerIdAndEpoch
import org.apache.kafka.controller.ControllerRequestContextUtil.ANONYMOUS_CONTEXT
import org.apache.kafka.server.IntegrationTestUtils
import org.junit.jupiter.api.Assertions.{assertEquals, fail}

import java.net.Socket
import java.util
import java.util.{Comparator, Properties}
import java.util.stream.Collectors
import scala.collection.Seq
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._


class GroupCoordinatorBaseRequestTest(cluster: ClusterInstance) {
  private def brokers(): Seq[KafkaBroker] = cluster.brokers.values().stream().collect(Collectors.toList[KafkaBroker]).asScala.toSeq

  private def controllerServers(): Seq[ControllerServer] = cluster.controllers().values().asScala.toSeq

  protected var producer: KafkaProducer[String, String] = _

  protected var openSockets: ListBuffer[Socket] = ListBuffer[Socket]()

  protected def createOffsetsTopic(): Unit = {
    val admin = cluster.admin()
    try {
      TestUtils.createOffsetsTopicWithAdmin(
        admin = admin,
        brokers = brokers(),
        controllers = controllerServers()
      )
    } finally {
      admin.close()
    }
  }

  protected def createTransactionStateTopic(): Unit = {
    val admin = cluster.admin()
    try {
      TestUtils.createTransactionStateTopicWithAdmin(
        admin = admin,
        brokers = brokers(),
        controllers = controllerServers()
      )
    } finally {
      admin.close()
    }
  }

  protected def createTopic(
    topic: String,
    numPartitions: Int
  ): Uuid = {
    val admin = cluster.admin()
    try {
      TestUtils.createTopicWithAdmin(
        admin = admin,
        brokers = brokers(),
        controllers = controllerServers(),
        topic = topic,
        numPartitions = numPartitions
      )
      admin
        .describeTopics(TopicCollection.ofTopicNames(List(topic).asJava))
        .allTopicNames()
        .get()
        .get(topic)
        .topicId()
    } finally {
      admin.close()
    }
  }

  protected def deleteTopic(
    topic: String
  ): Unit = {
    val admin = cluster.admin()
    try {
      admin
        .deleteTopics(TopicCollection.ofTopicNames(List(topic).asJava))
        .all()
        .get()
    } finally {
      admin.close()
    }
  }

  protected def createTopicAndReturnLeaders(
    topic: String,
    numPartitions: Int = 1,
    replicationFactor: Int = 1,
    topicConfig: Properties = new Properties
  ): Map[TopicIdPartition, Int] = {
    val admin = cluster.admin()
    try {
      val partitionToLeader = TestUtils.createTopicWithAdmin(
        admin = admin,
        topic = topic,
        brokers = brokers(),
        controllers = controllerServers(),
        numPartitions = numPartitions,
        replicationFactor = replicationFactor,
        topicConfig = topicConfig
      )
      partitionToLeader.map { case (partition, leader) => new TopicIdPartition(getTopicIds.get(topic), new TopicPartition(topic, partition)) -> leader }
    } finally {
      admin.close()
    }
  }

  protected def isUnstableApiEnabled: Boolean = {
    cluster.brokers.values.stream.allMatch(b => b.config.unstableApiVersionsEnabled)
  }

  protected def getTopicIds: util.Map[String, Uuid] = {
    cluster.controllers().get(cluster.controllerIds().iterator().next()).controller.findAllTopicIds(ANONYMOUS_CONTEXT).get()
  }

  protected def getBrokers: Seq[KafkaBroker] = {
    cluster.brokers.values().stream().collect(Collectors.toList[KafkaBroker]).asScala.toSeq
  }

  protected def bootstrapServers(): String = {
    TestUtils.plaintextBootstrapServers(getBrokers)
  }

  protected def initProducer(): Unit = {
    producer = TestUtils.createProducer(cluster.bootstrapServers(),
      keySerializer = new StringSerializer, valueSerializer = new StringSerializer)
  }

  protected def closeSockets(): Unit = {
    while (openSockets.nonEmpty) {
      val socket = openSockets.head
      socket.close()
      openSockets.remove(0)
    }
  }

  protected def closeProducer(): Unit = {
    if(producer != null)
      producer.close()
  }

  protected def produceData(
    topicIdPartition: TopicIdPartition, 
    numMessages: Int
  ): Seq[RecordMetadata] = {
    val records = for {
      messageIndex <- 0 until numMessages
    } yield {
      val suffix = s"$topicIdPartition-$messageIndex"
      new ProducerRecord(topicIdPartition.topic, topicIdPartition.partition, s"key $suffix", s"value $suffix")
    }
    records.map(producer.send(_).get)
  }

  protected def produceData(
    topicIdPartition: TopicIdPartition, 
    key: String, 
    value: String
  ): RecordMetadata = {
    producer.send(new ProducerRecord(topicIdPartition.topic, topicIdPartition.partition,
      key, value)).get
  }

  protected def commitOffset(
    groupId: String,
    memberId: String,
    memberEpoch: Int,
    topic: String,
    topicId: Uuid,
    partition: Int,
    offset: Long,
    expectedError: Errors,
    version: Short = ApiKeys.OFFSET_COMMIT.latestVersion(isUnstableApiEnabled)
  ): Unit = {
    if (version >= 10 && topicId == Uuid.ZERO_UUID) {
      throw new IllegalArgumentException(s"Cannot call OffsetCommit API version $version without a topic id")
    }

    val request = OffsetCommitRequest.Builder.forTopicIdsOrNames(
      new OffsetCommitRequestData()
        .setGroupId(groupId)
        .setMemberId(memberId)
        .setGenerationIdOrMemberEpoch(memberEpoch)
        .setTopics(List(
          new OffsetCommitRequestData.OffsetCommitRequestTopic()
            .setTopicId(topicId)
            .setName(topic)
            .setPartitions(List(
              new OffsetCommitRequestData.OffsetCommitRequestPartition()
                .setPartitionIndex(partition)
                .setCommittedOffset(offset)
            ).asJava)
        ).asJava),
      isUnstableApiEnabled
    ).build(version)

    val expectedResponse = new OffsetCommitResponseData()
      .setTopics(List(
        new OffsetCommitResponseData.OffsetCommitResponseTopic()
          .setTopicId(if (version >= 10) topicId else Uuid.ZERO_UUID)
          .setName(if (version < 10) topic else "")
          .setPartitions(List(
            new OffsetCommitResponseData.OffsetCommitResponsePartition()
              .setPartitionIndex(partition)
              .setErrorCode(expectedError.code)
          ).asJava)
      ).asJava)

    val response = connectAndReceive[OffsetCommitResponse](request)
    assertEquals(expectedResponse, response.data)
  }

  protected def commitTxnOffset(
     groupId: String,
     memberId: String,
     generationId: Int,
     producerId: Long,
     producerEpoch: Short,
     transactionalId: String,
     topic: String,
     partition: Int,
     offset: Long,
     expectedError: Errors,
     version: Short = ApiKeys.TXN_OFFSET_COMMIT.latestVersion(isUnstableApiEnabled)
  ): Unit = {
    val request = new TxnOffsetCommitRequest.Builder(
      new TxnOffsetCommitRequestData()
        .setGroupId(groupId)
        .setMemberId(memberId)
        .setGenerationId(generationId)
        .setProducerId(producerId)
        .setProducerEpoch(producerEpoch)
        .setTransactionalId(transactionalId)
        .setTopics(List(
          new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic()
            .setName(topic)
            .setPartitions(List(
              new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
                .setPartitionIndex(partition)
                .setCommittedOffset(offset)
            ).asJava)
        ).asJava)
    ).build(version)

    val expectedResponse = new TxnOffsetCommitResponseData()
      .setTopics(List(
        new TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic()
          .setName(topic)
          .setPartitions(List(
            new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
              .setPartitionIndex(partition)
              .setErrorCode(expectedError.code)
            ).asJava)
        ).asJava)

    val response = connectAndReceive[TxnOffsetCommitResponse](request)
    assertEquals(expectedResponse, response.data)
  }

  protected def addOffsetsToTxn(
    groupId: String,
    producerId: Long,
    producerEpoch: Short,
    transactionalId: String,
    version: Short = ApiKeys.ADD_OFFSETS_TO_TXN.latestVersion(isUnstableApiEnabled)
  ): Unit = {
    val request = new AddOffsetsToTxnRequest.Builder(
      new AddOffsetsToTxnRequestData()
        .setTransactionalId(transactionalId)
        .setProducerId(producerId)
        .setProducerEpoch(producerEpoch)
        .setGroupId(groupId)
    ).build(version)

    val response = connectAndReceive[AddOffsetsToTxnResponse](request)
    assertEquals(new AddOffsetsToTxnResponseData(), response.data)
  }

  protected def initProducerId(
    transactionalId: String,
    transactionTimeoutMs: Int = 60000,
    producerIdAndEpoch: ProducerIdAndEpoch,
    expectedError: Errors,
    version: Short = ApiKeys.INIT_PRODUCER_ID.latestVersion(isUnstableApiEnabled)
  ): ProducerIdAndEpoch = {
    val request = new InitProducerIdRequest.Builder(
      new InitProducerIdRequestData()
        .setTransactionalId(transactionalId)
        .setTransactionTimeoutMs(transactionTimeoutMs)
        .setProducerId(producerIdAndEpoch.producerId)
        .setProducerEpoch(producerIdAndEpoch.epoch))
      .build(version)

    val response = connectAndReceive[InitProducerIdResponse](request).data
    assertEquals(expectedError.code, response.errorCode)
    new ProducerIdAndEpoch(response.producerId, response.producerEpoch)
  }

  protected def endTxn(
    producerId: Long,
    producerEpoch: Short,
    transactionalId: String,
    isTransactionV2Enabled: Boolean,
    committed: Boolean,
    expectedError: Errors,
    version: Short = ApiKeys.END_TXN.latestVersion(isUnstableApiEnabled)
  ): Unit = {
    val request = new EndTxnRequest.Builder(
      new EndTxnRequestData()
        .setProducerId(producerId)
        .setProducerEpoch(producerEpoch)
        .setTransactionalId(transactionalId)
        .setCommitted(committed),
      isUnstableApiEnabled,
      isTransactionV2Enabled
    ).build(version)

    assertEquals(expectedError, connectAndReceive[EndTxnResponse](request).error)
  }

  protected def writeTxnMarkers(
    producerId: Long,
    producerEpoch: Short,
    committed: Boolean,
    expectedError: Errors = Errors.NONE,
    version: Short = ApiKeys.WRITE_TXN_MARKERS.latestVersion(isUnstableApiEnabled)
  ): Unit = {
    val request = new WriteTxnMarkersRequest.Builder(
      new WriteTxnMarkersRequestData()
        .setMarkers(List(
          new WritableTxnMarker()
            .setProducerId(producerId)
            .setProducerEpoch(producerEpoch)
            .setTransactionResult(committed)
            .setTopics(List(
              new WritableTxnMarkerTopic()
                .setName(Topic.GROUP_METADATA_TOPIC_NAME)
                .setPartitionIndexes(List[Integer](0).asJava)
            ).asJava)
            .setCoordinatorEpoch(0)
        ).asJava)
    ).build(version)

    assertEquals(
      expectedError.code,
      connectAndReceive[WriteTxnMarkersResponse](request).data.markers.get(0).topics.get(0).partitions.get(0).errorCode
    )
  }

  protected def fetchOffsets(
    groups: List[OffsetFetchRequestData.OffsetFetchRequestGroup],
    requireStable: Boolean,
    version: Short
  ): List[OffsetFetchResponseData.OffsetFetchResponseGroup] = {
    if (version < 8) {
      fail(s"OffsetFetch API version $version cannot fetch multiple groups.")
    }

    val request = OffsetFetchRequest.Builder.forTopicIdsOrNames(
      new OffsetFetchRequestData()
        .setRequireStable(requireStable)
        .setGroups(groups.asJava),
      false,
      true
    ).build(version)

    val response = connectAndReceive[OffsetFetchResponse](request)

    // Sort topics and partitions within the response as their order is not guaranteed.
    response.data.groups.asScala.foreach(sortTopicPartitions)

    response.data.groups.asScala.toList
  }

  protected def fetchOffsets(
    group: OffsetFetchRequestData.OffsetFetchRequestGroup,
    requireStable: Boolean,
    version: Short
  ): OffsetFetchResponseData.OffsetFetchResponseGroup = {
    val request = OffsetFetchRequest.Builder.forTopicIdsOrNames(
      new OffsetFetchRequestData()
        .setRequireStable(requireStable)
        .setGroups(List(group).asJava),
      false,
      true
    ).build(version)

    val response = connectAndReceive[OffsetFetchResponse](request)

    // Normalize the response based on the version to present the
    // same format to the caller.
    val groupResponse = if (version >= 8) {
      assertEquals(1, response.data.groups.size)
      assertEquals(group.groupId, response.data.groups.get(0).groupId)
      response.data.groups.asScala.head
    } else {
      new OffsetFetchResponseData.OffsetFetchResponseGroup()
        .setGroupId(group.groupId)
        .setErrorCode(response.data.errorCode)
        .setTopics(response.data.topics.asScala.map { topic =>
          new OffsetFetchResponseData.OffsetFetchResponseTopics()
            .setName(topic.name)
            .setPartitions(topic.partitions.asScala.map { partition =>
              new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                .setPartitionIndex(partition.partitionIndex)
                .setErrorCode(partition.errorCode)
                .setCommittedOffset(partition.committedOffset)
                .setCommittedLeaderEpoch(partition.committedLeaderEpoch)
                .setMetadata(partition.metadata)
            }.asJava)
        }.asJava)
    }

    // Sort topics and partitions within the response as their order is not guaranteed.
    sortTopicPartitions(groupResponse)

    groupResponse
  }

  protected def fetchOffset(
    groupId: String,
    topic: String,
    partition: Int
  ): Long = {
    val groupIdRecord = fetchOffsets(
      group = new OffsetFetchRequestData.OffsetFetchRequestGroup()
        .setGroupId(groupId)
        .setTopics(List(
          new OffsetFetchRequestData.OffsetFetchRequestTopics()
            .setName(topic)
            .setPartitionIndexes(List[Integer](partition).asJava)
        ).asJava),
      requireStable = true,
      version = 9
    )
    val topicRecord = groupIdRecord.topics.asScala.find(_.name == topic).head
    val partitionRecord = topicRecord.partitions.asScala.find(_.partitionIndex == partition).head
    partitionRecord.committedOffset
  }

  protected def deleteOffset(
    groupId: String,
    topic: String,
    partition: Int,
    expectedResponseError: Errors = Errors.NONE,
    expectedPartitionError: Errors = Errors.NONE,
    version: Short
  ): Unit = {
    if (expectedResponseError != Errors.NONE && expectedPartitionError != Errors.NONE) {
      fail("deleteOffset: neither expectedResponseError nor expectedTopicError is Errors.NONE.")
    }

    val request = new OffsetDeleteRequest.Builder(
      new OffsetDeleteRequestData()
        .setGroupId(groupId)
        .setTopics(new OffsetDeleteRequestData.OffsetDeleteRequestTopicCollection(List(
          new OffsetDeleteRequestData.OffsetDeleteRequestTopic()
            .setName(topic)
            .setPartitions(List(
              new OffsetDeleteRequestData.OffsetDeleteRequestPartition()
                .setPartitionIndex(partition)
            ).asJava)
        ).asJava.iterator))
    ).build(version)

    val expectedResponse = new OffsetDeleteResponseData()
    if (expectedResponseError == Errors.NONE) {
      expectedResponse
        .setTopics(new OffsetDeleteResponseData.OffsetDeleteResponseTopicCollection(List(
          new OffsetDeleteResponseData.OffsetDeleteResponseTopic()
            .setName(topic)
            .setPartitions(new OffsetDeleteResponseData.OffsetDeleteResponsePartitionCollection(List(
              new OffsetDeleteResponseData.OffsetDeleteResponsePartition()
                .setPartitionIndex(partition)
                .setErrorCode(expectedPartitionError.code)
            ).asJava.iterator))
        ).asJava.iterator))
    } else {
      expectedResponse.setErrorCode(expectedResponseError.code)
    }

    val response = connectAndReceive[OffsetDeleteResponse](request)
    assertEquals(expectedResponse, response.data)
  }

  private def sortTopicPartitions(
    group: OffsetFetchResponseData.OffsetFetchResponseGroup
  ): Unit = {
    group.topics.sort((t1, t2) => t1.name.compareTo(t2.name))
    group.topics.asScala.foreach { topic =>
      topic.partitions.sort(Comparator.comparingInt[OffsetFetchResponseData.OffsetFetchResponsePartitions](_.partitionIndex))
    }
  }

  protected def verifySyncGroupWithOldProtocol(
    groupId: String,
    memberId: String,
    generationId: Int,
    protocolType: String = "consumer",
    protocolName: String = "consumer-range",
    assignments: List[SyncGroupRequestData.SyncGroupRequestAssignment] = List.empty,
    expectedProtocolType: String = "consumer",
    expectedProtocolName: String = "consumer-range",
    expectedAssignment: Array[Byte] = Array.empty,
    expectedError: Errors = Errors.NONE,
    version: Short = ApiKeys.SYNC_GROUP.latestVersion(isUnstableApiEnabled)
  ): SyncGroupResponseData = {
    val syncGroupResponseData = syncGroupWithOldProtocol(
      groupId = groupId,
      memberId = memberId,
      generationId = generationId,
      protocolType = protocolType,
      protocolName = protocolName,
      assignments = assignments,
      version = version
    )

    assertEquals(
      new SyncGroupResponseData()
        .setErrorCode(expectedError.code)
        .setProtocolType(if (version >= 5) expectedProtocolType else null)
        .setProtocolName(if (version >= 5) expectedProtocolName else null)
        .setAssignment(expectedAssignment),
      syncGroupResponseData
    )

    syncGroupResponseData
  }

  protected def syncGroupWithOldProtocol(
    groupId: String,
    memberId: String,
    generationId: Int,
    protocolType: String = "consumer",
    protocolName: String = "consumer-range",
    assignments: List[SyncGroupRequestData.SyncGroupRequestAssignment] = List.empty,
    version: Short = ApiKeys.SYNC_GROUP.latestVersion(isUnstableApiEnabled)
  ): SyncGroupResponseData = {
    val syncGroupRequestData = new SyncGroupRequestData()
      .setGroupId(groupId)
      .setMemberId(memberId)
      .setGenerationId(generationId)
      .setProtocolType(protocolType)
      .setProtocolName(protocolName)
      .setAssignments(assignments.asJava)

    val syncGroupRequest = new SyncGroupRequest.Builder(syncGroupRequestData).build(version)
    val syncGroupResponse = connectAndReceive[SyncGroupResponse](syncGroupRequest)
    syncGroupResponse.data
  }

  protected def sendJoinRequest(
    groupId: String,
    memberId: String = "",
    groupInstanceId: String = null,
    protocolType: String = "consumer",
    protocolName: String = "consumer-range",
    metadata: Array[Byte] = Array.empty,
    version: Short = ApiKeys.JOIN_GROUP.latestVersion(isUnstableApiEnabled)
  ): JoinGroupResponseData = {
    val joinGroupRequestData = new JoinGroupRequestData()
      .setGroupId(groupId)
      .setMemberId(memberId)
      .setGroupInstanceId(groupInstanceId)
      .setRebalanceTimeoutMs(5 * 50 * 1000)
      .setSessionTimeoutMs(600000)
      .setProtocolType(protocolType)
      .setProtocols(new JoinGroupRequestData.JoinGroupRequestProtocolCollection(
        List(
          new JoinGroupRequestData.JoinGroupRequestProtocol()
            .setName(protocolName)
            .setMetadata(metadata)
        ).asJava.iterator
      ))

    // Send the request until receiving a successful response. There is a delay
    // here because the group coordinator is loaded in the background.
    val joinGroupRequest = new JoinGroupRequest.Builder(joinGroupRequestData).build(version)
    var joinGroupResponse: JoinGroupResponse = null
    TestUtils.waitUntilTrue(() => {
      joinGroupResponse = connectAndReceive[JoinGroupResponse](joinGroupRequest)
      joinGroupResponse != null
    }, msg = s"Could not join the group successfully. Last response $joinGroupResponse.")

    joinGroupResponse.data
  }

  protected def joinDynamicConsumerGroupWithOldProtocol(
    groupId: String,
    metadata: Array[Byte] = Array.empty,
    assignment: Array[Byte] = Array.empty,
    completeRebalance: Boolean = true
  ): (String, Int) = {
    val joinGroupResponseData = sendJoinRequest(
      groupId = groupId,
      metadata = metadata
    )
    assertEquals(Errors.MEMBER_ID_REQUIRED.code, joinGroupResponseData.errorCode)

    // Rejoin the group with the member id.
    val rejoinGroupResponseData = sendJoinRequest(
      groupId = groupId,
      memberId = joinGroupResponseData.memberId,
      metadata = metadata
    )
    assertEquals(Errors.NONE.code, rejoinGroupResponseData.errorCode)

    if (completeRebalance) {
      // Send the sync group request to complete the rebalance.
      verifySyncGroupWithOldProtocol(
        groupId = groupId,
        memberId = rejoinGroupResponseData.memberId,
        generationId = rejoinGroupResponseData.generationId,
        assignments = List(new SyncGroupRequestAssignment()
          .setMemberId(rejoinGroupResponseData.memberId)
          .setAssignment(assignment)),
        expectedAssignment = assignment
      )
    }

    (rejoinGroupResponseData.memberId, rejoinGroupResponseData.generationId)
  }

  protected def joinStaticConsumerGroupWithOldProtocol(
    groupId: String,
    groupInstanceId: String,
    metadata: Array[Byte] = Array.empty,
    assignment: Array[Byte] = Array.empty,
    completeRebalance: Boolean = true
  ): (String, Int) = {
    val joinGroupResponseData = sendJoinRequest(
      groupId = groupId,
      groupInstanceId = groupInstanceId,
      metadata = metadata
    )

    if (completeRebalance) {
      // Send the sync group request to complete the rebalance.
      verifySyncGroupWithOldProtocol(
        groupId = groupId,
        memberId = joinGroupResponseData.memberId,
        generationId = joinGroupResponseData.generationId,
        assignments = List(new SyncGroupRequestAssignment()
          .setMemberId(joinGroupResponseData.memberId)
          .setAssignment(assignment)),
        expectedAssignment = assignment
      )
    }

    (joinGroupResponseData.memberId, joinGroupResponseData.generationId)
  }

  protected def joinConsumerGroupWithNewProtocol(groupId: String, memberId: String = ""): (String, Int) = {
    val consumerGroupHeartbeatResponseData = consumerGroupHeartbeat(
      groupId = groupId,
      memberId = memberId,
      rebalanceTimeoutMs = 5 * 60 * 1000,
      subscribedTopicNames = List("foo"),
      topicPartitions = List.empty
    )
    (consumerGroupHeartbeatResponseData.memberId, consumerGroupHeartbeatResponseData.memberEpoch)
  }

  protected def joinConsumerGroup(groupId: String, useNewProtocol: Boolean): (String, Int) = {
    if (useNewProtocol) {
      // Note that we heartbeat only once to join the group and assume
      // that the test will complete within the session timeout.
      joinConsumerGroupWithNewProtocol(groupId, Uuid.randomUuid().toString)
    } else {
      // Note that we don't heartbeat and assume that the test will
      // complete within the session timeout.
      joinDynamicConsumerGroupWithOldProtocol(groupId = groupId)
    }
  }

  protected def listGroups(
    statesFilter: List[String],
    typesFilter: List[String],
    version: Short = ApiKeys.LIST_GROUPS.latestVersion(isUnstableApiEnabled)
  ): List[ListGroupsResponseData.ListedGroup] = {
    val request = new ListGroupsRequest.Builder(
      new ListGroupsRequestData()
        .setStatesFilter(statesFilter.asJava)
        .setTypesFilter(typesFilter.asJava)
    ).build(version)

    val response = connectAndReceive[ListGroupsResponse](request)

    response.data.groups.asScala.toList
  }

  protected def describeGroups(
    groupIds: List[String],
    version: Short = ApiKeys.DESCRIBE_GROUPS.latestVersion(isUnstableApiEnabled)
  ): List[DescribeGroupsResponseData.DescribedGroup] = {
    val describeGroupsRequest = new DescribeGroupsRequest.Builder(
      new DescribeGroupsRequestData().setGroups(groupIds.asJava)
    ).build(version)

    val describeGroupsResponse = connectAndReceive[DescribeGroupsResponse](describeGroupsRequest)

    describeGroupsResponse.data.groups.asScala.toList
  }

  protected def consumerGroupDescribe(
    groupIds: List[String],
    includeAuthorizedOperations: Boolean = false,
    version: Short = ApiKeys.CONSUMER_GROUP_DESCRIBE.latestVersion(isUnstableApiEnabled)
  ): List[ConsumerGroupDescribeResponseData.DescribedGroup] = {
    val consumerGroupDescribeRequest = new ConsumerGroupDescribeRequest.Builder(
      new ConsumerGroupDescribeRequestData()
        .setGroupIds(groupIds.asJava)
        .setIncludeAuthorizedOperations(includeAuthorizedOperations)
    ).build(version)

    val consumerGroupDescribeResponse = connectAndReceive[ConsumerGroupDescribeResponse](consumerGroupDescribeRequest)
    consumerGroupDescribeResponse.data.groups.asScala.toList
  }

  protected def shareGroupDescribe(
     groupIds: List[String],
     includeAuthorizedOperations: Boolean,
     version: Short = ApiKeys.SHARE_GROUP_DESCRIBE.latestVersion(isUnstableApiEnabled)
   ): List[ShareGroupDescribeResponseData.DescribedGroup] = {
    val shareGroupDescribeRequest = new ShareGroupDescribeRequest.Builder(
      new ShareGroupDescribeRequestData()
        .setGroupIds(groupIds.asJava)
        .setIncludeAuthorizedOperations(includeAuthorizedOperations)
    ).build(version)

    val shareGroupDescribeResponse = connectAndReceive[ShareGroupDescribeResponse](shareGroupDescribeRequest)
    shareGroupDescribeResponse.data.groups.asScala.toList
  }

  protected def streamsGroupDescribe(
    groupIds: List[String],
    includeAuthorizedOperations: Boolean = false,
    version: Short = ApiKeys.STREAMS_GROUP_DESCRIBE.latestVersion(isUnstableApiEnabled)
  ): List[StreamsGroupDescribeResponseData.DescribedGroup] = {
    val streamsGroupDescribeRequest = new StreamsGroupDescribeRequest.Builder(
      new StreamsGroupDescribeRequestData()
        .setGroupIds(groupIds.asJava)
        .setIncludeAuthorizedOperations(includeAuthorizedOperations)
    ).build(version)

    val streamsGroupDescribeResponse = connectAndReceive[StreamsGroupDescribeResponse](streamsGroupDescribeRequest)
    streamsGroupDescribeResponse.data.groups.asScala.toList
  }

  protected def heartbeat(
    groupId: String,
    generationId: Int,
    memberId: String,
    groupInstanceId: String = null,
    expectedError: Errors = Errors.NONE,
    version: Short = ApiKeys.HEARTBEAT.latestVersion(isUnstableApiEnabled)
  ): HeartbeatResponseData = {
    val heartbeatRequest = new HeartbeatRequest.Builder(
      new HeartbeatRequestData()
        .setGroupId(groupId)
        .setGenerationId(generationId)
        .setMemberId(memberId)
        .setGroupInstanceId(groupInstanceId)
    ).build(version)

    val heartbeatResponse = connectAndReceive[HeartbeatResponse](heartbeatRequest)
    assertEquals(expectedError.code, heartbeatResponse.data.errorCode)

    heartbeatResponse.data
  }

  protected def consumerGroupHeartbeat(
    groupId: String,
    memberId: String = "",
    memberEpoch: Int = 0,
    instanceId: String = null,
    rackId: String = null,
    rebalanceTimeoutMs: Int = -1,
    serverAssignor: String = null,
    subscribedTopicNames: List[String] = null,
    topicPartitions: List[ConsumerGroupHeartbeatRequestData.TopicPartitions] = null,
    expectedError: Errors = Errors.NONE,
    version: Short = ApiKeys.CONSUMER_GROUP_HEARTBEAT.latestVersion(isUnstableApiEnabled)
  ): ConsumerGroupHeartbeatResponseData = {
    val consumerGroupHeartbeatRequest = new ConsumerGroupHeartbeatRequest.Builder(
      new ConsumerGroupHeartbeatRequestData()
        .setGroupId(groupId)
        .setMemberId(memberId)
        .setMemberEpoch(memberEpoch)
        .setInstanceId(instanceId)
        .setRackId(rackId)
        .setRebalanceTimeoutMs(rebalanceTimeoutMs)
        .setSubscribedTopicNames(subscribedTopicNames.asJava)
        .setServerAssignor(serverAssignor)
        .setTopicPartitions(topicPartitions.asJava)
    ).build(version)

    // Send the request until receiving a successful response. There is a delay
    // here because the group coordinator is loaded in the background.
    var consumerGroupHeartbeatResponse: ConsumerGroupHeartbeatResponse = null
    TestUtils.waitUntilTrue(() => {
      consumerGroupHeartbeatResponse = connectAndReceive[ConsumerGroupHeartbeatResponse](consumerGroupHeartbeatRequest)
      consumerGroupHeartbeatResponse.data.errorCode == expectedError.code
    }, msg = s"Could not heartbeat successfully. Last response $consumerGroupHeartbeatResponse.")

    consumerGroupHeartbeatResponse.data
  }

  protected def shareGroupHeartbeat(
    groupId: String,
    memberId: String = Uuid.randomUuid.toString,
    memberEpoch: Int = 0,
    rackId: String = null,
    subscribedTopicNames: List[String] = null,
    expectedError: Errors = Errors.NONE
  ): ShareGroupHeartbeatResponseData = {
    val shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequest.Builder(
      new ShareGroupHeartbeatRequestData()
        .setGroupId(groupId)
        .setMemberId(memberId)
        .setMemberEpoch(memberEpoch)
        .setRackId(rackId)
        .setSubscribedTopicNames(subscribedTopicNames.asJava)
    ).build()

    // Send the request until receiving a successful response. There is a delay
    // here because the group coordinator is loaded in the background.
    var shareGroupHeartbeatResponse: ShareGroupHeartbeatResponse = null
    TestUtils.waitUntilTrue(() => {
      shareGroupHeartbeatResponse = connectAndReceive[ShareGroupHeartbeatResponse](shareGroupHeartbeatRequest)
      shareGroupHeartbeatResponse.data.errorCode == expectedError.code
    }, msg = s"Could not heartbeat successfully. Last response $shareGroupHeartbeatResponse.")

    shareGroupHeartbeatResponse.data
  }

  protected def streamsGroupHeartbeat(
    groupId: String,
    memberId: String = "",
    memberEpoch: Int = 0,
    rebalanceTimeoutMs: Int = -1,
    activeTasks: List[StreamsGroupHeartbeatRequestData.TaskIds] = null,
    standbyTasks: List[StreamsGroupHeartbeatRequestData.TaskIds] = null,
    warmupTasks: List[StreamsGroupHeartbeatRequestData.TaskIds] = null,
    topology: StreamsGroupHeartbeatRequestData.Topology = null,
    expectedError: Errors = Errors.NONE,
    processId: String = null,
    version: Short = ApiKeys.STREAMS_GROUP_HEARTBEAT.latestVersion(isUnstableApiEnabled)
  ): StreamsGroupHeartbeatResponseData = {
    val streamsGroupHeartbeatRequest = new StreamsGroupHeartbeatRequest.Builder(
      new StreamsGroupHeartbeatRequestData()
        .setGroupId(groupId)
        .setMemberId(memberId)
        .setMemberEpoch(memberEpoch)
        .setRebalanceTimeoutMs(rebalanceTimeoutMs)
        .setActiveTasks(activeTasks.asJava)
        .setStandbyTasks(standbyTasks.asJava)
        .setWarmupTasks(warmupTasks.asJava)
        .setTopology(topology)
        .setProcessId(processId)
    ).build(version)

    // Send the request until receiving a successful response. There is a delay
    // here because the group coordinator is loaded in the background.
    var streamsGroupHeartbeatResponse: StreamsGroupHeartbeatResponse = null
    TestUtils.waitUntilTrue(() => {
      streamsGroupHeartbeatResponse = connectAndReceive[StreamsGroupHeartbeatResponse](streamsGroupHeartbeatRequest)
      streamsGroupHeartbeatResponse.data.errorCode == expectedError.code
    }, msg = s"Could not heartbeat successfully. Last response $streamsGroupHeartbeatResponse.")

    streamsGroupHeartbeatResponse.data
  }

  protected def leaveGroupWithNewProtocol(
    groupId: String,
    memberId: String
  ): ConsumerGroupHeartbeatResponseData = {
    consumerGroupHeartbeat(
      groupId = groupId,
      memberId = memberId,
      memberEpoch = ConsumerGroupHeartbeatRequest.LEAVE_GROUP_MEMBER_EPOCH
    )
  }

  protected def classicLeaveGroup(
    groupId: String,
    memberIds: List[String],
    groupInstanceIds: List[String] = null,
    version: Short = ApiKeys.LEAVE_GROUP.latestVersion(isUnstableApiEnabled)
  ): LeaveGroupResponseData = {
    val leaveGroupRequest = new LeaveGroupRequest.Builder(
      groupId,
      List.tabulate(memberIds.length) { i =>
        new MemberIdentity()
          .setMemberId(memberIds(i))
          .setGroupInstanceId(if (groupInstanceIds == null) null else groupInstanceIds(i))
      }.asJava
    ).build(version)

    connectAndReceive[LeaveGroupResponse](leaveGroupRequest).data
  }

  protected def leaveGroupWithOldProtocol(
    groupId: String,
    memberIds: List[String],
    groupInstanceIds: List[String] = null,
    expectedLeaveGroupError: Errors,
    expectedMemberErrors: List[Errors],
    version: Short = ApiKeys.LEAVE_GROUP.latestVersion(isUnstableApiEnabled)
  ): Unit = {
    val leaveGroupResponse = classicLeaveGroup(
      groupId,
      memberIds,
      groupInstanceIds,
      version
    )

    val expectedResponseData = new LeaveGroupResponseData()
    if (expectedLeaveGroupError != Errors.NONE) {
      expectedResponseData.setErrorCode(expectedLeaveGroupError.code)
    } else {
      expectedResponseData
        .setMembers(List.tabulate(expectedMemberErrors.length) { i =>
          new MemberResponse()
            .setMemberId(memberIds(i))
            .setGroupInstanceId(if (groupInstanceIds == null) null else groupInstanceIds(i))
            .setErrorCode(expectedMemberErrors(i).code)
        }.asJava)
    }

    assertEquals(expectedResponseData, leaveGroupResponse)
  }

  protected def leaveGroup(
    groupId: String,
    memberId: String,
    useNewProtocol: Boolean,
    version: Short
  ): Unit = {
    if (useNewProtocol) {
      leaveGroupWithNewProtocol(groupId, memberId)
    } else {
      leaveGroupWithOldProtocol(groupId, List(memberId), null, Errors.NONE, List(Errors.NONE), version)
    }
  }

  protected def deleteGroups(
    groupIds: List[String],
    expectedErrors: List[Errors],
    version: Short
  ): Unit = {
    if (groupIds.size != expectedErrors.size) {
      fail("deleteGroups: groupIds and expectedErrors have unmatched sizes.")
    }

    val deleteGroupsRequest = new DeleteGroupsRequest.Builder(
      new DeleteGroupsRequestData()
        .setGroupsNames(groupIds.asJava)
    ).build(version)

    val expectedResponseData = new DeleteGroupsResponseData()
      .setResults(new DeletableGroupResultCollection(List.tabulate(groupIds.length) { i =>
        new DeletableGroupResult()
          .setGroupId(groupIds(i))
          .setErrorCode(expectedErrors(i).code)
      }.asJava.iterator))

    val deleteGroupsResponse = connectAndReceive[DeleteGroupsResponse](deleteGroupsRequest)
    assertEquals(expectedResponseData.results.asScala.toSet, deleteGroupsResponse.data.results.asScala.toSet)
  }

  protected def connectAny(): Socket = {
    val socket: Socket = IntegrationTestUtils.connect(cluster.brokerBoundPorts().get(0))
    openSockets += socket
    socket
  }

  protected def connect(destination: Int): Socket = {
    val socket = IntegrationTestUtils.connect(brokerSocketServer(destination).boundPort(cluster.clientListener()))
    openSockets += socket
    socket
  }

  protected def connectAndReceive[T <: AbstractResponse](
    request: AbstractRequest
  ): T = {
    IntegrationTestUtils.connectAndReceive[T](request, cluster.brokerBoundPorts().get(0))
  }

  protected def connectAndReceive[T <: AbstractResponse](
    request: AbstractRequest,
    destination: Int
  ): T = {
    val socketServer = brokerSocketServer(destination)
    val listenerName = cluster.clientListener()
    IntegrationTestUtils.connectAndReceive[T](request, socketServer.boundPort(listenerName))
  }

  private def brokerSocketServer(brokerId: Int): SocketServer = {
    getBrokers.find { broker =>
      broker.config.brokerId == brokerId
    }.map(_.socketServer).getOrElse(throw new IllegalStateException(s"Could not find broker with id $brokerId"))
  }
}
