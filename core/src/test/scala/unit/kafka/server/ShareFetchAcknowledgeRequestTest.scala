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

import kafka.utils.TestUtils
import org.apache.kafka.clients.admin.DescribeShareGroupsOptions
import org.apache.kafka.clients.consumer.ShareAcquireMode
import org.apache.kafka.common.test.api.{ClusterConfigProperty, ClusterFeature, ClusterTest, ClusterTestDefaults, ClusterTests, Type}
import org.apache.kafka.common.message.ShareFetchResponseData.AcquiredRecords
import org.apache.kafka.common.message.{FindCoordinatorRequestData, ShareAcknowledgeRequestData, ShareAcknowledgeResponseData, ShareFetchRequestData, ShareFetchResponseData, ShareGroupHeartbeatRequestData}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.{TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.common.requests.{FindCoordinatorRequest, FindCoordinatorResponse, ShareAcknowledgeRequest, ShareAcknowledgeResponse, ShareFetchRequest, ShareFetchResponse, ShareGroupHeartbeatRequest, ShareGroupHeartbeatResponse, ShareRequestMetadata}
import org.apache.kafka.common.test.ClusterInstance
import org.apache.kafka.server.common.Feature
import org.apache.kafka.server.IntegrationTestUtils
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.{AfterEach, Timeout}

import java.net.Socket
import java.util

@Timeout(1200)
@ClusterTestDefaults(types = Array(Type.KRAFT), brokers = 1, serverProperties = Array(
  new ClusterConfigProperty(key = "group.share.persister.class.name", value = "")
))
class ShareFetchAcknowledgeRequestTest(cluster: ClusterInstance) extends GroupCoordinatorBaseRequestTest(cluster) {

  private final val MAX_WAIT_MS = 5000
  private final val GROUP_ID = "group"
  private final val TOPIC = "topic"
  private final val PARTITION = 0
  private final val MEMBER_ID = Uuid.randomUuid()

  @AfterEach
  def tearDown(): Unit = {
    closeProducer()
    closeSockets()
  }

  @ClusterTest(
    features = Array(
      new ClusterFeature(feature = Feature.SHARE_VERSION, version = 0)
    )
  )
  def testShareFetchRequestIsInAccessibleWhenConfigsDisabled(): Unit = {
    val metadata: ShareRequestMetadata = new ShareRequestMetadata(Uuid.randomUuid(), ShareRequestMetadata.INITIAL_EPOCH)
    val send = util.List.of(
      new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("topic1", 0)),
      new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("topic1", 1))
    )

    val socket: Socket = connectAny()

    val shareFetchRequest = createShareFetchRequest(GROUP_ID, metadata, send, util.List.of, util.Map.of)
    val shareFetchResponse = IntegrationTestUtils.sendAndReceive[ShareFetchResponse](shareFetchRequest, socket)

    assertEquals(Errors.UNSUPPORTED_VERSION.code, shareFetchResponse.data.errorCode)
    assertEquals(0, shareFetchResponse.data.acquisitionLockTimeoutMs)
  }

  @ClusterTest(
    features = Array(
      new ClusterFeature(feature = Feature.SHARE_VERSION, version = 0)
    )
  )
  def testShareAcknowledgeRequestIsInAccessibleWhenConfigsDisabled(): Unit = {
    val metadata: ShareRequestMetadata = new ShareRequestMetadata(Uuid.randomUuid(), ShareRequestMetadata.INITIAL_EPOCH)

    val socket: Socket = connectAny()

    val shareAcknowledgeRequest = createShareAcknowledgeRequest(GROUP_ID, metadata, util.Map.of)
    val shareAcknowledgeResponse = IntegrationTestUtils.sendAndReceive[ShareAcknowledgeResponse](shareAcknowledgeRequest, socket)

    assertEquals(Errors.UNSUPPORTED_VERSION.code, shareAcknowledgeResponse.data.errorCode)
    assertEquals(0, shareAcknowledgeResponse.data.acquisitionLockTimeoutMs)
  }

  @ClusterTests(
    Array(
      new ClusterTest(
        serverProperties = Array(
          new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
          new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1")
        ),
        brokers = 2
      ),
      new ClusterTest(
        serverProperties = Array(
          new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
          new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1"),
          new ClusterConfigProperty(key = "group.share.persister.class.name", value = "org.apache.kafka.server.share.persister.DefaultStatePersister"),
          new ClusterConfigProperty(key = "share.coordinator.state.topic.replication.factor", value = "1"),
          new ClusterConfigProperty(key = "share.coordinator.state.topic.num.partitions", value = "1")
        ),
        brokers = 2
      ),
    )
  )
  def testShareFetchRequestToNonLeaderReplica(): Unit = {
    val metadata: ShareRequestMetadata = new ShareRequestMetadata(MEMBER_ID, ShareRequestMetadata.INITIAL_EPOCH)

    // Create a single-partition topic and find a broker which is not the leader
    val partitionToLeader = createTopicAndReturnLeaders(TOPIC)
    val topicIds = getTopicIds
    val topicId = topicIds.get(TOPIC)
    val topicIdPartition = new TopicIdPartition(topicId, new TopicPartition(TOPIC, PARTITION))
    val topicNames = {
      val map = new java.util.LinkedHashMap[Uuid, String]()
      topicIds.forEach((k, v) => map.put(v, k)) // swap key and value
      map
    }
    val leader = partitionToLeader(topicIdPartition)
    val nonReplicaOpt = getBrokers.find(_.config.brokerId != leader)
    assertTrue(nonReplicaOpt.isDefined)
    val nonReplicaId = nonReplicaOpt.get.config.brokerId

    val send = util.List.of(topicIdPartition)

    val socket: Socket = connect(nonReplicaId)

    createOffsetsTopic()
    shareHeartbeat(MEMBER_ID, GROUP_ID, util.Map.of[String, Int](TOPIC, 1))

    // Send the share fetch request to the non-replica and verify the error code
    val shareFetchRequest = createShareFetchRequest(GROUP_ID, metadata, send, util.List.of, util.Map.of)
    val shareFetchResponse = IntegrationTestUtils.sendAndReceive[ShareFetchResponse](shareFetchRequest, socket)
    assertEquals(30000, shareFetchResponse.data.acquisitionLockTimeoutMs)
    val partitionData = shareFetchResponse.responseData(topicNames).get(topicIdPartition)
    assertEquals(Errors.NOT_LEADER_OR_FOLLOWER.code, partitionData.errorCode)
    assertEquals(leader, partitionData.currentLeader().leaderId())
  }

  @ClusterTests(
    Array(
      new ClusterTest(
        serverProperties = Array(
          new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
          new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1")
        )
      ),
      new ClusterTest(
        serverProperties = Array(
          new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
          new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1"),
          new ClusterConfigProperty(key = "group.share.persister.class.name", value = "org.apache.kafka.server.share.persister.DefaultStatePersister"),
          new ClusterConfigProperty(key = "share.coordinator.state.topic.replication.factor", value = "1"),
          new ClusterConfigProperty(key = "share.coordinator.state.topic.num.partitions", value = "1")
        )
      )
    )
  )
  def testShareFetchRequestSuccess(): Unit = {
    createTopicAndReturnLeaders(TOPIC, numPartitions = 3)
    val topicIds = getTopicIds
    val topicId = topicIds.get(TOPIC)
    val topicIdPartition = new TopicIdPartition(topicId, new TopicPartition(TOPIC, PARTITION))

    val send = util.List.of(topicIdPartition)

    val socket: Socket = connectAny()

    createOffsetsTopic()
    shareHeartbeat(MEMBER_ID, GROUP_ID, util.Map.of[String, Int](TOPIC, 3))

    // Send the first share fetch request to initialize the share partition
    sendFirstShareFetchRequest(MEMBER_ID, GROUP_ID, send, socket)

    initProducer()
    // Producing 10 records to the topic created above
    produceData(topicIdPartition, 10)

    // Send the second share fetch request to fetch the records produced above
    val metadata = new ShareRequestMetadata(MEMBER_ID, ShareRequestMetadata.nextEpoch(ShareRequestMetadata.INITIAL_EPOCH))
    val acknowledgementsMap = util.Map.of[TopicIdPartition, util.List[ShareFetchRequestData.AcknowledgementBatch]]
    val shareFetchRequest = createShareFetchRequest(GROUP_ID, metadata, send, util.List.of, acknowledgementsMap)
    val shareFetchResponse = IntegrationTestUtils.sendAndReceive[ShareFetchResponse](shareFetchRequest, socket)

    val shareFetchResponseData = shareFetchResponse.data()
    assertEquals(Errors.NONE.code, shareFetchResponseData.errorCode)
    assertEquals(30000, shareFetchResponseData.acquisitionLockTimeoutMs)
    assertEquals(1, shareFetchResponseData.responses().size())
    assertEquals(topicId, shareFetchResponseData.responses().stream().findFirst().get().topicId())
    assertEquals(1, shareFetchResponseData.responses().stream().findFirst().get().partitions().size())

    val expectedPartitionData = new ShareFetchResponseData.PartitionData()
      .setPartitionIndex(PARTITION)
      .setErrorCode(Errors.NONE.code())
      .setAcknowledgeErrorCode(Errors.NONE.code())
      .setAcquiredRecords(expectedAcquiredRecords(util.List.of(0), util.List.of(9), util.List.of(1)))

    val partitionData = shareFetchResponseData.responses().stream().findFirst().get().partitions().get(0)
    compareFetchResponsePartitions(expectedPartitionData, partitionData)
  }

  @ClusterTests(
    Array(
      new ClusterTest(
        serverProperties = Array(
          new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
          new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1")
        )
      ),
      new ClusterTest(
        serverProperties = Array(
          new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
          new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1"),
          new ClusterConfigProperty(key = "group.share.persister.class.name", value = "org.apache.kafka.server.share.persister.DefaultStatePersister"),
          new ClusterConfigProperty(key = "share.coordinator.state.topic.replication.factor", value = "1"),
          new ClusterConfigProperty(key = "share.coordinator.state.topic.num.partitions", value = "1")
        )
      )
    )
  )
  def testShareFetchRequestSuccessMultiplePartitions(): Unit = {
    createTopicAndReturnLeaders(TOPIC, numPartitions = 3)
    val topicIds = getTopicIds
    val topicId = topicIds.get(TOPIC)
    val topicIdPartition1 = new TopicIdPartition(topicId, new TopicPartition(TOPIC, 0))
    val topicIdPartition2 = new TopicIdPartition(topicId, new TopicPartition(TOPIC, 1))
    val topicIdPartition3 = new TopicIdPartition(topicId, new TopicPartition(TOPIC, 2))

    val send = util.List.of(topicIdPartition1, topicIdPartition2, topicIdPartition3)

    val socket: Socket = connectAny()

    createOffsetsTopic()
    shareHeartbeat(MEMBER_ID, GROUP_ID, util.Map.of[String, Int](TOPIC, 3))

    // Send the first share fetch request to initialize the share partitions
    sendFirstShareFetchRequest(MEMBER_ID, GROUP_ID, send, socket)

    initProducer()
    // Producing 10 records to the topic partitions created above
    produceData(topicIdPartition1, 10)
    produceData(topicIdPartition2, 10)
    produceData(topicIdPartition3, 10)

    // Send the second share fetch request to fetch the records produced above
    val metadata = new ShareRequestMetadata(MEMBER_ID, ShareRequestMetadata.nextEpoch(ShareRequestMetadata.INITIAL_EPOCH))
    val acknowledgementsMap = util.Map.of[TopicIdPartition, util.List[ShareFetchRequestData.AcknowledgementBatch]]
    val shareFetchRequest = createShareFetchRequest(GROUP_ID, metadata, send, util.List.of, acknowledgementsMap)

    // For the multi partition fetch request, the response may not be available in the first attempt
    // as the share partitions might not be initialized yet. So, we retry until we get the response.
    var responses = Seq[ShareFetchResponseData.PartitionData]()
    TestUtils.waitUntilTrue(() => {
      val shareFetchResponse = IntegrationTestUtils.sendAndReceive[ShareFetchResponse](shareFetchRequest, socket)
      val shareFetchResponseData = shareFetchResponse.data()
      assertEquals(Errors.NONE.code, shareFetchResponseData.errorCode)
      assertEquals(30000, shareFetchResponseData.acquisitionLockTimeoutMs)
      assertEquals(1, shareFetchResponseData.responses().size())
      val partitionsCount = shareFetchResponseData.responses().stream().findFirst().get().partitions().size()
      if (partitionsCount > 0) {
        assertEquals(topicId, shareFetchResponseData.responses().stream().findFirst().get().topicId())
        shareFetchResponseData.responses().stream().findFirst().get().partitions().forEach(partitionData => {
          if (!partitionData.acquiredRecords().isEmpty) {
            responses = responses :+ partitionData
          }
        })
      }
      responses.size == 3
    }, "Share fetch request failed", 5000)

    val expectedPartitionData1 = new ShareFetchResponseData.PartitionData()
      .setPartitionIndex(0)
      .setErrorCode(Errors.NONE.code())
      .setAcknowledgeErrorCode(Errors.NONE.code())
      .setAcquiredRecords(expectedAcquiredRecords(util.List.of(0), util.List.of(9), util.List.of(1)))

    val expectedPartitionData2 = new ShareFetchResponseData.PartitionData()
      .setPartitionIndex(1)
      .setErrorCode(Errors.NONE.code())
      .setAcknowledgeErrorCode(Errors.NONE.code())
      .setAcquiredRecords(expectedAcquiredRecords(util.List.of(0), util.List.of(9), util.List.of(1)))

    val expectedPartitionData3 = new ShareFetchResponseData.PartitionData()
      .setPartitionIndex(2)
      .setErrorCode(Errors.NONE.code())
      .setAcknowledgeErrorCode(Errors.NONE.code())
      .setAcquiredRecords(expectedAcquiredRecords(util.List.of(0), util.List.of(9), util.List.of(1)))

    responses.foreach(partitionData => {
      partitionData.partitionIndex() match {
        case 0 => compareFetchResponsePartitions(expectedPartitionData1, partitionData)
        case 1 => compareFetchResponsePartitions(expectedPartitionData2, partitionData)
        case 2 => compareFetchResponsePartitions(expectedPartitionData3, partitionData)
      }
    })
  }

  @ClusterTests(
    Array(
      new ClusterTest(
        serverProperties = Array(
          new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
          new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1")
        ),
        brokers = 3
      ),
      new ClusterTest(
        serverProperties = Array(
          new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
          new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1"),
          new ClusterConfigProperty(key = "group.share.persister.class.name", value = "org.apache.kafka.server.share.persister.DefaultStatePersister"),
          new ClusterConfigProperty(key = "share.coordinator.state.topic.replication.factor", value = "1"),
          new ClusterConfigProperty(key = "share.coordinator.state.topic.num.partitions", value = "1")
        ),
        brokers = 3
      ),
    )
  )
  def testShareFetchRequestSuccessMultiplePartitionsMultipleBrokers(): Unit = {
    val partitionToLeaders = createTopicAndReturnLeaders(TOPIC, numPartitions = 3)
    val topicIds = getTopicIds
    val topicId = topicIds.get(TOPIC)
    val topicIdPartition1 = new TopicIdPartition(topicId, new TopicPartition(TOPIC, 0))
    val topicIdPartition2 = new TopicIdPartition(topicId, new TopicPartition(TOPIC, 1))
    val topicIdPartition3 = new TopicIdPartition(topicId, new TopicPartition(TOPIC, 2))

    val leader1 = partitionToLeaders(topicIdPartition1)
    val leader2 = partitionToLeaders(topicIdPartition2)
    val leader3 = partitionToLeaders(topicIdPartition3)

    val send1 = util.List.of(topicIdPartition1)
    val send2 = util.List.of(topicIdPartition2)
    val send3 = util.List.of(topicIdPartition3)

    val metadata: ShareRequestMetadata = new ShareRequestMetadata(MEMBER_ID, ShareRequestMetadata.INITIAL_EPOCH)
    val acknowledgementsMap = util.Map.of[TopicIdPartition, util.List[ShareFetchRequestData.AcknowledgementBatch]]

    val socket1: Socket = connect(leader1)
    val socket2: Socket = connect(leader2)
    val socket3: Socket = connect(leader3)

    createOffsetsTopic()
    shareHeartbeat(MEMBER_ID, GROUP_ID, util.Map.of[String, Int](TOPIC, 3))

    // Send the first share fetch request to initialize the share partitions
    // Create different share fetch requests for different partitions as they may have leaders on separate brokers
    var shareFetchRequest1 = createShareFetchRequest(GROUP_ID, metadata, send1, util.List.of, acknowledgementsMap)
    var shareFetchRequest2 = createShareFetchRequest(GROUP_ID, metadata, send2, util.List.of, acknowledgementsMap)
    var shareFetchRequest3 = createShareFetchRequest(GROUP_ID, metadata, send3, util.List.of, acknowledgementsMap)

    var shareFetchResponse1 = IntegrationTestUtils.sendAndReceive[ShareFetchResponse](shareFetchRequest1, socket1)
    var shareFetchResponse2 = IntegrationTestUtils.sendAndReceive[ShareFetchResponse](shareFetchRequest2, socket2)
    var shareFetchResponse3 = IntegrationTestUtils.sendAndReceive[ShareFetchResponse](shareFetchRequest3, socket3)

    initProducer()
    // Producing 10 records to the topic partitions created above
    produceData(topicIdPartition1, 10)
    produceData(topicIdPartition2, 10)
    produceData(topicIdPartition3, 10)

    // Send the second share fetch request to fetch the records produced above
    // Create different share fetch requests for different partitions as they may have leaders on separate brokers
    shareFetchRequest1 = createShareFetchRequest(GROUP_ID, metadata, send1, util.List.of, acknowledgementsMap)
    shareFetchRequest2 = createShareFetchRequest(GROUP_ID, metadata, send2, util.List.of, acknowledgementsMap)
    shareFetchRequest3 = createShareFetchRequest(GROUP_ID, metadata, send3, util.List.of, acknowledgementsMap)

    shareFetchResponse1 = IntegrationTestUtils.sendAndReceive[ShareFetchResponse](shareFetchRequest1, socket1)
    shareFetchResponse2 = IntegrationTestUtils.sendAndReceive[ShareFetchResponse](shareFetchRequest2, socket2)
    shareFetchResponse3 = IntegrationTestUtils.sendAndReceive[ShareFetchResponse](shareFetchRequest3, socket3)

    val shareFetchResponseData1 = shareFetchResponse1.data()
    assertEquals(Errors.NONE.code, shareFetchResponseData1.errorCode)
    assertEquals(30000, shareFetchResponseData1.acquisitionLockTimeoutMs)
    assertEquals(1, shareFetchResponseData1.responses().size())
    assertEquals(topicId, shareFetchResponseData1.responses().stream().findFirst().get().topicId())
    assertEquals(1, shareFetchResponseData1.responses().stream().findFirst().get().partitions().size())
    val partitionData1 = shareFetchResponseData1.responses().stream().findFirst().get().partitions().get(0)

    val shareFetchResponseData2 = shareFetchResponse2.data()
    assertEquals(Errors.NONE.code, shareFetchResponseData2.errorCode)
    assertEquals(30000, shareFetchResponseData2.acquisitionLockTimeoutMs)
    assertEquals(1, shareFetchResponseData2.responses().size())
    assertEquals(topicId, shareFetchResponseData2.responses().stream().findFirst().get().topicId())
    assertEquals(1, shareFetchResponseData2.responses().stream().findFirst().get().partitions().size())
    val partitionData2 = shareFetchResponseData2.responses().stream().findFirst().get().partitions().get(0)

    val shareFetchResponseData3 = shareFetchResponse3.data()
    assertEquals(Errors.NONE.code, shareFetchResponseData3.errorCode)
    assertEquals(1, shareFetchResponseData3.responses().size())
    assertEquals(topicId, shareFetchResponseData3.responses().stream().findFirst().get().topicId())
    assertEquals(1, shareFetchResponseData3.responses().stream().findFirst().get().partitions().size())
    val partitionData3 = shareFetchResponseData3.responses().stream().findFirst().get().partitions().get(0)

    val expectedPartitionData1 = new ShareFetchResponseData.PartitionData()
      .setPartitionIndex(0)
      .setErrorCode(Errors.NONE.code())
      .setAcknowledgeErrorCode(Errors.NONE.code())
      .setAcquiredRecords(expectedAcquiredRecords(util.List.of(0), util.List.of(9), util.List.of(1)))

    val expectedPartitionData2 = new ShareFetchResponseData.PartitionData()
      .setPartitionIndex(1)
      .setErrorCode(Errors.NONE.code())
      .setAcknowledgeErrorCode(Errors.NONE.code())
      .setAcquiredRecords(expectedAcquiredRecords(util.List.of(0), util.List.of(9), util.List.of(1)))

    val expectedPartitionData3 = new ShareFetchResponseData.PartitionData()
      .setPartitionIndex(2)
      .setErrorCode(Errors.NONE.code())
      .setAcknowledgeErrorCode(Errors.NONE.code())
      .setAcquiredRecords(expectedAcquiredRecords(util.List.of(0), util.List.of(9), util.List.of(1)))

    compareFetchResponsePartitions(expectedPartitionData1, partitionData1)
    compareFetchResponsePartitions(expectedPartitionData2, partitionData2)
    compareFetchResponsePartitions(expectedPartitionData3, partitionData3)
  }

  @ClusterTests(
    Array(
      new ClusterTest(
        serverProperties = Array(
          new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
          new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1")
        )
      ),
      new ClusterTest(
        serverProperties = Array(
          new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
          new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1"),
          new ClusterConfigProperty(key = "group.share.persister.class.name", value = "org.apache.kafka.server.share.persister.DefaultStatePersister"),
          new ClusterConfigProperty(key = "share.coordinator.state.topic.replication.factor", value = "1"),
          new ClusterConfigProperty(key = "share.coordinator.state.topic.num.partitions", value = "1")
        )
      ),
    )
  )
  def testShareAcknowledgeRequestSuccessAccept(): Unit = {
    createTopicAndReturnLeaders(TOPIC, numPartitions = 3)
    val topicIds = getTopicIds
    val topicId = topicIds.get(TOPIC)
    val topicIdPartition = new TopicIdPartition(topicId, new TopicPartition(TOPIC, PARTITION))

    val send = util.List.of(topicIdPartition)

    val socket: Socket = connectAny()

    createOffsetsTopic()
    shareHeartbeat(MEMBER_ID, GROUP_ID, util.Map.of[String, Int](TOPIC, 3))

    // Send the first share fetch request to initialize share partitions
    sendFirstShareFetchRequest(MEMBER_ID, GROUP_ID, send, socket)

    initProducer()
    // Producing 10 records to the topic created above
    produceData(topicIdPartition, 10)

    // Send the second share fetch request to fetch the records produced above
    var shareSessionEpoch = ShareRequestMetadata.nextEpoch(ShareRequestMetadata.INITIAL_EPOCH)
    var metadata = new ShareRequestMetadata(MEMBER_ID, shareSessionEpoch)
    val acknowledgementsMapForFetch = util.Map.of[TopicIdPartition, util.List[ShareFetchRequestData.AcknowledgementBatch]]
    var shareFetchRequest = createShareFetchRequest(GROUP_ID, metadata, send, util.List.of, acknowledgementsMapForFetch)
    var shareFetchResponse = IntegrationTestUtils.sendAndReceive[ShareFetchResponse](shareFetchRequest, socket)

    var shareFetchResponseData = shareFetchResponse.data()
    assertEquals(Errors.NONE.code, shareFetchResponseData.errorCode)
    assertEquals(30000, shareFetchResponseData.acquisitionLockTimeoutMs)
    assertEquals(1, shareFetchResponseData.responses().size())
    assertEquals(topicId, shareFetchResponseData.responses().stream().findFirst().get().topicId())
    assertEquals(1, shareFetchResponseData.responses().stream().findFirst().get().partitions().size())

    var expectedFetchPartitionData = new ShareFetchResponseData.PartitionData()
      .setPartitionIndex(PARTITION)
      .setErrorCode(Errors.NONE.code())
      .setAcknowledgeErrorCode(Errors.NONE.code())
      .setAcquiredRecords(expectedAcquiredRecords(util.List.of(0), util.List.of(9), util.List.of(1)))

    var fetchPartitionData = shareFetchResponseData.responses().stream().findFirst().get().partitions().get(0)
    compareFetchResponsePartitions(expectedFetchPartitionData, fetchPartitionData)

    // Send a Share Acknowledge request to acknowledge the fetched records
    shareSessionEpoch = ShareRequestMetadata.nextEpoch(shareSessionEpoch)
    metadata = new ShareRequestMetadata(MEMBER_ID, shareSessionEpoch)
    val acknowledgementsMapForAcknowledge: util.Map[TopicIdPartition, util.List[ShareAcknowledgeRequestData.AcknowledgementBatch]] =
      util.Map.of(topicIdPartition, util.List.of(new ShareAcknowledgeRequestData.AcknowledgementBatch()
        .setFirstOffset(0)
        .setLastOffset(9)
        .setAcknowledgeTypes(util.List.of(1.toByte)))) // Accept the records
    val shareAcknowledgeRequest = createShareAcknowledgeRequest(GROUP_ID, metadata, acknowledgementsMapForAcknowledge)
    val shareAcknowledgeResponse = IntegrationTestUtils.sendAndReceive[ShareAcknowledgeResponse](shareAcknowledgeRequest, socket)

    val shareAcknowledgeResponseData = shareAcknowledgeResponse.data()
    assertEquals(Errors.NONE.code, shareAcknowledgeResponseData.errorCode)
    assertEquals(1, shareAcknowledgeResponseData.responses().size())
    assertEquals(topicId, shareAcknowledgeResponseData.responses().stream().findFirst().get().topicId())
    assertEquals(1, shareAcknowledgeResponseData.responses().stream().findFirst().get().partitions().size())

    val expectedAcknowledgePartitionData = new ShareAcknowledgeResponseData.PartitionData()
      .setPartitionIndex(PARTITION)
      .setErrorCode(Errors.NONE.code())

    val acknowledgePartitionData = shareAcknowledgeResponseData.responses().stream().findFirst().get().partitions().get(0)
    assertEquals(30000, shareAcknowledgeResponseData.acquisitionLockTimeoutMs())
    compareAcknowledgeResponsePartitions(expectedAcknowledgePartitionData, acknowledgePartitionData)

    // Producing 10 more records to the topic
    produceData(topicIdPartition, 10)

    // Sending a third share fetch request to check if acknowledgements were done successfully
    shareSessionEpoch = ShareRequestMetadata.nextEpoch(shareSessionEpoch)
    metadata = new ShareRequestMetadata(MEMBER_ID, shareSessionEpoch)
    shareFetchRequest = createShareFetchRequest(GROUP_ID, metadata, send, util.List.of, util.Map.of)
    shareFetchResponse = IntegrationTestUtils.sendAndReceive[ShareFetchResponse](shareFetchRequest, socket)

    shareFetchResponseData = shareFetchResponse.data()
    assertEquals(Errors.NONE.code, shareFetchResponseData.errorCode)
    assertEquals(30000, shareFetchResponseData.acquisitionLockTimeoutMs)
    assertEquals(1, shareFetchResponseData.responses().size())
    assertEquals(topicId, shareFetchResponseData.responses().stream().findFirst().get().topicId())
    assertEquals(1, shareFetchResponseData.responses().stream().findFirst().get().partitions().size())

    expectedFetchPartitionData = new ShareFetchResponseData.PartitionData()
      .setPartitionIndex(PARTITION)
      .setErrorCode(Errors.NONE.code())
      .setAcknowledgeErrorCode(Errors.NONE.code())
      .setAcquiredRecords(expectedAcquiredRecords(util.List.of(10), util.List.of(19), util.List.of(1))) // Only the records from offset 10 onwards should be fetched because records at offsets 0-9 have been acknowledged

    fetchPartitionData = shareFetchResponseData.responses().stream().findFirst().get().partitions().get(0)
    compareFetchResponsePartitions(expectedFetchPartitionData, fetchPartitionData)
  }

  @ClusterTests(
    Array(
      new ClusterTest(
        serverProperties = Array(
          new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
          new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1"),
          new ClusterConfigProperty(key = "group.share.record.lock.duration.ms", value = "15000")
        )
      ),
      new ClusterTest(
        serverProperties = Array(
          new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
          new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1"),
          new ClusterConfigProperty(key = "group.share.persister.class.name", value = "org.apache.kafka.server.share.persister.DefaultStatePersister"),
          new ClusterConfigProperty(key = "share.coordinator.state.topic.replication.factor", value = "1"),
          new ClusterConfigProperty(key = "share.coordinator.state.topic.num.partitions", value = "1"),
          new ClusterConfigProperty(key = "group.share.record.lock.duration.ms", value = "15000")
        )
      ),
    )
  )
  def testShareFetchRequestPiggybackedAccept(): Unit = {
    createTopicAndReturnLeaders(TOPIC, numPartitions = 3)
    val topicIds = getTopicIds
    val topicId = topicIds.get(TOPIC)
    val topicIdPartition = new TopicIdPartition(topicId, new TopicPartition(TOPIC, PARTITION))

    val send = util.List.of(topicIdPartition)

    val socket: Socket = connectAny()

    createOffsetsTopic()
    shareHeartbeat(MEMBER_ID, GROUP_ID, util.Map.of[String, Int](TOPIC, 3))

    // Send the first share fetch request to initialize the share partition
    sendFirstShareFetchRequest(MEMBER_ID, GROUP_ID, send, socket, 15000)

    initProducer()
    // Producing 10 records to the topic created above
    produceData(topicIdPartition, 10)

    // Send the second share fetch request to fetch the records produced above
    var shareSessionEpoch: Int = ShareRequestMetadata.nextEpoch(ShareRequestMetadata.INITIAL_EPOCH)
    var metadata = new ShareRequestMetadata(MEMBER_ID, shareSessionEpoch)
    var acknowledgementsMapForFetch = util.Map.of[TopicIdPartition, util.List[ShareFetchRequestData.AcknowledgementBatch]]
    var shareFetchRequest = createShareFetchRequest(GROUP_ID, metadata, send, util.List.of, acknowledgementsMapForFetch)
    var shareFetchResponse = IntegrationTestUtils.sendAndReceive[ShareFetchResponse](shareFetchRequest, socket)

    var shareFetchResponseData = shareFetchResponse.data()
    assertEquals(Errors.NONE.code, shareFetchResponseData.errorCode)
    assertEquals(15000, shareFetchResponseData.acquisitionLockTimeoutMs)
    assertEquals(1, shareFetchResponseData.responses().size())
    assertEquals(topicId, shareFetchResponseData.responses().stream().findFirst().get().topicId())
    assertEquals(1, shareFetchResponseData.responses().stream().findFirst().get().partitions().size())

    var expectedFetchPartitionData = new ShareFetchResponseData.PartitionData()
      .setPartitionIndex(PARTITION)
      .setErrorCode(Errors.NONE.code())
      .setAcknowledgeErrorCode(Errors.NONE.code())
      .setAcquiredRecords(expectedAcquiredRecords(util.List.of(0), util.List.of(9), util.List.of(1)))

    var fetchPartitionData = shareFetchResponseData.responses().stream().findFirst().get().partitions().get(0)
    compareFetchResponsePartitions(expectedFetchPartitionData, fetchPartitionData)

    // Producing 10 more records to the topic created above
    produceData(topicIdPartition, 10)

    // Send the third Share Fetch request with piggybacked acknowledgements
    shareSessionEpoch = ShareRequestMetadata.nextEpoch(shareSessionEpoch)
    metadata = new ShareRequestMetadata(MEMBER_ID, shareSessionEpoch)
    acknowledgementsMapForFetch = util.Map.of(topicIdPartition, util.List.of(new ShareFetchRequestData.AcknowledgementBatch()
      .setFirstOffset(0)
      .setLastOffset(9)
      .setAcknowledgeTypes(util.List.of(1.toByte)))) // Accept the records
    shareFetchRequest = createShareFetchRequest(GROUP_ID, metadata, send, util.List.of, acknowledgementsMapForFetch)
    shareFetchResponse = IntegrationTestUtils.sendAndReceive[ShareFetchResponse](shareFetchRequest, socket)

    shareFetchResponseData = shareFetchResponse.data()
    assertEquals(Errors.NONE.code, shareFetchResponseData.errorCode)
    assertEquals(15000, shareFetchResponseData.acquisitionLockTimeoutMs)
    assertEquals(1, shareFetchResponseData.responses().size())
    assertEquals(topicId, shareFetchResponseData.responses().stream().findFirst().get().topicId())
    assertEquals(1, shareFetchResponseData.responses().stream().findFirst().get().partitions().size())

    expectedFetchPartitionData = new ShareFetchResponseData.PartitionData()
      .setPartitionIndex(PARTITION)
      .setErrorCode(Errors.NONE.code())
      .setAcknowledgeErrorCode(Errors.NONE.code())
      .setAcquiredRecords(expectedAcquiredRecords(util.List.of(10), util.List.of(19), util.List.of(1))) // The records at offsets 0 to 9 will not be re fetched because they have been acknowledged

    fetchPartitionData = shareFetchResponseData.responses().stream().findFirst().get().partitions().get(0)
    compareFetchResponsePartitions(expectedFetchPartitionData, fetchPartitionData)

    // Producing 10 more records to the topic
    produceData(topicIdPartition, 10)

    // Sending a fourth share fetch request to confirm if acknowledgements were done successfully
    shareSessionEpoch = ShareRequestMetadata.nextEpoch(shareSessionEpoch)
    metadata = new ShareRequestMetadata(MEMBER_ID, shareSessionEpoch)
    shareFetchRequest = createShareFetchRequest(GROUP_ID, metadata, send, util.List.of, util.Map.of)
    shareFetchResponse = IntegrationTestUtils.sendAndReceive[ShareFetchResponse](shareFetchRequest, socket)

    shareFetchResponseData = shareFetchResponse.data()
    assertEquals(Errors.NONE.code, shareFetchResponseData.errorCode)
    assertEquals(15000, shareFetchResponseData.acquisitionLockTimeoutMs)
    assertEquals(1, shareFetchResponseData.responses().size())
    assertEquals(topicId, shareFetchResponseData.responses().stream().findFirst().get().topicId())
    assertEquals(1, shareFetchResponseData.responses().stream().findFirst().get().partitions().size())

    expectedFetchPartitionData = new ShareFetchResponseData.PartitionData()
      .setPartitionIndex(PARTITION)
      .setErrorCode(Errors.NONE.code())
      .setAcknowledgeErrorCode(Errors.NONE.code())
      .setAcquiredRecords(expectedAcquiredRecords(util.List.of(20), util.List.of(29), util.List.of(1))) // Only the records from offset 20 onwards should be fetched because records at offsets 0-9 have been acknowledged before and 10 to 19 are currently acquired

    fetchPartitionData = shareFetchResponseData.responses().stream().findFirst().get().partitions().get(0)
    compareFetchResponsePartitions(expectedFetchPartitionData, fetchPartitionData)
  }

  @ClusterTests(
    Array(
      new ClusterTest(
        serverProperties = Array(
          new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
          new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1")
        )
      ),
      new ClusterTest(
        serverProperties = Array(
          new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
          new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1"),
          new ClusterConfigProperty(key = "group.share.persister.class.name", value = "org.apache.kafka.server.share.persister.DefaultStatePersister"),
          new ClusterConfigProperty(key = "share.coordinator.state.topic.replication.factor", value = "1"),
          new ClusterConfigProperty(key = "share.coordinator.state.topic.num.partitions", value = "1")
        )
      ),
    )
  )
  def testShareAcknowledgeRequestSuccessRelease(): Unit = {
    createTopicAndReturnLeaders(TOPIC, numPartitions = 3)
    val topicIds = getTopicIds
    val topicId = topicIds.get(TOPIC)
    val topicIdPartition = new TopicIdPartition(topicId, new TopicPartition(TOPIC, PARTITION))

    val send = util.List.of(topicIdPartition)

    val socket: Socket = connectAny()

    createOffsetsTopic()
    shareHeartbeat(MEMBER_ID, GROUP_ID, util.Map.of[String, Int](TOPIC, 3))

    // Send the first share fetch request to initialize the share partiion
    sendFirstShareFetchRequest(MEMBER_ID, GROUP_ID, send, socket)

    initProducer()
    // Producing 10 records to the topic created above
    produceData(topicIdPartition, 10)

    // Send the second share fetch request to fetch the records produced above
    var shareSessionEpoch = ShareRequestMetadata.nextEpoch(ShareRequestMetadata.INITIAL_EPOCH)
    var metadata = new ShareRequestMetadata(MEMBER_ID, shareSessionEpoch)
    val acknowledgementsMapForFetch = util.Map.of[TopicIdPartition, util.List[ShareFetchRequestData.AcknowledgementBatch]]
    var shareFetchRequest = createShareFetchRequest(GROUP_ID, metadata, send, util.List.of, acknowledgementsMapForFetch)
    var shareFetchResponse = IntegrationTestUtils.sendAndReceive[ShareFetchResponse](shareFetchRequest, socket)

    var shareFetchResponseData = shareFetchResponse.data()
    assertEquals(Errors.NONE.code, shareFetchResponseData.errorCode)
    assertEquals(30000, shareFetchResponseData.acquisitionLockTimeoutMs)
    assertEquals(1, shareFetchResponseData.responses().size())
    assertEquals(topicId, shareFetchResponseData.responses().stream().findFirst().get().topicId())
    assertEquals(1, shareFetchResponseData.responses().stream().findFirst().get().partitions().size())

    var expectedFetchPartitionData = new ShareFetchResponseData.PartitionData()
      .setPartitionIndex(PARTITION)
      .setErrorCode(Errors.NONE.code())
      .setAcknowledgeErrorCode(Errors.NONE.code())
      .setAcquiredRecords(expectedAcquiredRecords(util.List.of(0), util.List.of(9), util.List.of(1)))

    var fetchPartitionData = shareFetchResponseData.responses().stream().findFirst().get().partitions().get(0)
    compareFetchResponsePartitions(expectedFetchPartitionData, fetchPartitionData)

    // Send a Share Acknowledge request to acknowledge the fetched records
    shareSessionEpoch = ShareRequestMetadata.nextEpoch(shareSessionEpoch)
    metadata = new ShareRequestMetadata(MEMBER_ID, shareSessionEpoch)
    val acknowledgementsMapForAcknowledge: util.Map[TopicIdPartition, util.List[ShareAcknowledgeRequestData.AcknowledgementBatch]] =
      util.Map.of(topicIdPartition, util.List.of(new ShareAcknowledgeRequestData.AcknowledgementBatch()
        .setFirstOffset(0)
        .setLastOffset(9)
        .setAcknowledgeTypes(util.List.of(2.toByte)))) // Release the records
    val shareAcknowledgeRequest = createShareAcknowledgeRequest(GROUP_ID, metadata, acknowledgementsMapForAcknowledge)
    val shareAcknowledgeResponse = IntegrationTestUtils.sendAndReceive[ShareAcknowledgeResponse](shareAcknowledgeRequest, socket)

    val shareAcknowledgeResponseData = shareAcknowledgeResponse.data()
    assertEquals(Errors.NONE.code, shareAcknowledgeResponseData.errorCode)
    assertEquals(1, shareAcknowledgeResponseData.responses().size())
    assertEquals(topicId, shareAcknowledgeResponseData.responses().stream().findFirst().get().topicId())
    assertEquals(1, shareAcknowledgeResponseData.responses().stream().findFirst().get().partitions().size())
    assertEquals(30000, shareAcknowledgeResponseData.acquisitionLockTimeoutMs())

    val expectedAcknowledgePartitionData = new ShareAcknowledgeResponseData.PartitionData()
      .setPartitionIndex(PARTITION)
      .setErrorCode(Errors.NONE.code())

    val acknowledgePartitionData = shareAcknowledgeResponseData.responses().stream().findFirst().get().partitions().get(0)
    compareAcknowledgeResponsePartitions(expectedAcknowledgePartitionData, acknowledgePartitionData)

    // Sending a third share fetch request to check if acknowledgements were done successfully
    shareSessionEpoch = ShareRequestMetadata.nextEpoch(shareSessionEpoch)
    metadata = new ShareRequestMetadata(MEMBER_ID, shareSessionEpoch)
    shareFetchRequest = createShareFetchRequest(GROUP_ID, metadata, send, util.List.of, util.Map.of)
    shareFetchResponse = IntegrationTestUtils.sendAndReceive[ShareFetchResponse](shareFetchRequest, socket)

    shareFetchResponseData = shareFetchResponse.data()
    assertEquals(Errors.NONE.code, shareFetchResponseData.errorCode)
    assertEquals(30000, shareFetchResponseData.acquisitionLockTimeoutMs)
    assertEquals(1, shareFetchResponseData.responses().size())
    assertEquals(topicId, shareFetchResponseData.responses().stream().findFirst().get().topicId())
    assertEquals(1, shareFetchResponseData.responses().stream().findFirst().get().partitions().size())

    expectedFetchPartitionData = new ShareFetchResponseData.PartitionData()
      .setPartitionIndex(PARTITION)
      .setErrorCode(Errors.NONE.code())
      .setAcknowledgeErrorCode(Errors.NONE.code())
      .setAcquiredRecords(expectedAcquiredRecords(util.List.of(0), util.List.of(9), util.List.of(2))) // Records at offsets 0 to 9 should be fetched again because they were released with delivery count as 2

    fetchPartitionData = shareFetchResponseData.responses().stream().findFirst().get().partitions().get(0)
    compareFetchResponsePartitions(expectedFetchPartitionData, fetchPartitionData)
  }

  @ClusterTests(
    Array(
      new ClusterTest(
        serverProperties = Array(
          new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
          new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1")
        )
      ),
      new ClusterTest(
        serverProperties = Array(
          new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
          new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1"),
          new ClusterConfigProperty(key = "group.share.persister.class.name", value = "org.apache.kafka.server.share.persister.DefaultStatePersister"),
          new ClusterConfigProperty(key = "share.coordinator.state.topic.replication.factor", value = "1"),
          new ClusterConfigProperty(key = "share.coordinator.state.topic.num.partitions", value = "1")
        )
      ),
    )
  )
  def testShareFetchRequestPiggybackedRelease(): Unit = {
    createTopicAndReturnLeaders(TOPIC, numPartitions = 3)
    val topicIds = getTopicIds
    val topicId = topicIds.get(TOPIC)
    val topicIdPartition = new TopicIdPartition(topicId, new TopicPartition(TOPIC, PARTITION))

    val send = util.List.of(topicIdPartition)

    val socket: Socket = connectAny()

    createOffsetsTopic()
    shareHeartbeat(MEMBER_ID, GROUP_ID, util.Map.of[String, Int](TOPIC, 3))

    // Send the first share fetch request to initialize the share partition
    sendFirstShareFetchRequest(MEMBER_ID, GROUP_ID, send, socket)

    initProducer()
    // Producing 10 records to the topic created above
    produceData(topicIdPartition, 10)

    // Send the second share fetch request to fetch the records produced above
    var shareSessionEpoch = ShareRequestMetadata.nextEpoch(ShareRequestMetadata.INITIAL_EPOCH)
    var metadata = new ShareRequestMetadata(MEMBER_ID, shareSessionEpoch)
    var acknowledgementsMapForFetch = util.Map.of[TopicIdPartition, util.List[ShareFetchRequestData.AcknowledgementBatch]]
    var shareFetchRequest = createShareFetchRequest(GROUP_ID, metadata, send, util.List.of, acknowledgementsMapForFetch)
    var shareFetchResponse = IntegrationTestUtils.sendAndReceive[ShareFetchResponse](shareFetchRequest, socket)

    var shareFetchResponseData = shareFetchResponse.data()
    assertEquals(Errors.NONE.code, shareFetchResponseData.errorCode)
    assertEquals(30000, shareFetchResponseData.acquisitionLockTimeoutMs)
    assertEquals(1, shareFetchResponseData.responses().size())
    assertEquals(topicId, shareFetchResponseData.responses().stream().findFirst().get().topicId())
    assertEquals(1, shareFetchResponseData.responses().stream().findFirst().get().partitions().size())

    var expectedFetchPartitionData = new ShareFetchResponseData.PartitionData()
      .setPartitionIndex(PARTITION)
      .setErrorCode(Errors.NONE.code())
      .setAcknowledgeErrorCode(Errors.NONE.code())
      .setAcquiredRecords(expectedAcquiredRecords(util.List.of(0), util.List.of(9), util.List.of(1)))

    val fetchPartitionData = shareFetchResponseData.responses().stream().findFirst().get().partitions().get(0)
    compareFetchResponsePartitions(expectedFetchPartitionData, fetchPartitionData)

    // Producing 10 more records to the topic created above
    produceData(topicIdPartition, 10)

    expectedFetchPartitionData = new ShareFetchResponseData.PartitionData()
      .setPartitionIndex(PARTITION)
      .setErrorCode(Errors.NONE.code())
      .setAcknowledgeErrorCode(Errors.NONE.code())
      .setAcquiredRecords(expectedAcquiredRecords(util.List.of(0L, 10L), util.List.of(9L, 19L), util.List.of(2, 1)))

    val acquiredRecords = new util.ArrayList[AcquiredRecords]()
    var releaseAcknowledgementSent = false

    TestUtils.waitUntilTrue(() => {
      shareSessionEpoch = ShareRequestMetadata.nextEpoch(shareSessionEpoch)
      metadata = new ShareRequestMetadata(MEMBER_ID, shareSessionEpoch)
      if (releaseAcknowledgementSent) {
        // For fourth share fetch request onwards
        acknowledgementsMapForFetch = util.Map.of
      } else {
        // Send a third Share Fetch request with piggybacked acknowledgements
        acknowledgementsMapForFetch = util.Map.of(topicIdPartition, util.List.of(new ShareFetchRequestData.AcknowledgementBatch()
          .setFirstOffset(0)
          .setLastOffset(9)
          .setAcknowledgeTypes(util.List.of(2.toByte)))) // Release the records
        releaseAcknowledgementSent = true
      }
      shareFetchRequest = createShareFetchRequest(GROUP_ID, metadata, send, util.List.of, acknowledgementsMapForFetch)
      shareFetchResponse = IntegrationTestUtils.sendAndReceive[ShareFetchResponse](shareFetchRequest, socket)

      shareFetchResponseData = shareFetchResponse.data()
      assertEquals(Errors.NONE.code, shareFetchResponseData.errorCode)
      assertEquals(30000, shareFetchResponseData.acquisitionLockTimeoutMs)
      assertEquals(1, shareFetchResponseData.responses().size())
      assertEquals(topicId, shareFetchResponseData.responses().stream().findFirst().get().topicId())
      val responseSize = shareFetchResponseData.responses().stream().findFirst().get().partitions().size()
      if (responseSize > 0) {
        acquiredRecords.addAll(shareFetchResponseData.responses().stream().findFirst().get().partitions().get(0).acquiredRecords())
      }
      // There should be 2 acquired record batches finally -
      // 1. batch containing 0-9 offsets which were initially acknowledged as RELEASED.
      // 2. batch containing 10-19 offsets which were produced in the second produceData function call.
      acquiredRecords.size() == 2

    }, "Share fetch request failed", 5000)

    // All the records from offsets 0 to 19 will be fetched. Records from 0 to 9 will have delivery count as 2 because
    // they are re delivered, and records from 10 to 19 will have delivery count as 1 because they are newly acquired
    assertTrue(expectedFetchPartitionData.acquiredRecords().containsAll(acquiredRecords) &&
      acquiredRecords.containsAll(expectedFetchPartitionData.acquiredRecords()))
  }

  @ClusterTests(
    Array(
      new ClusterTest(
        serverProperties = Array(
          new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
          new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1")
        )
      ),
      new ClusterTest(
        serverProperties = Array(
          new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
          new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1"),
          new ClusterConfigProperty(key = "group.share.persister.class.name", value = "org.apache.kafka.server.share.persister.DefaultStatePersister"),
          new ClusterConfigProperty(key = "share.coordinator.state.topic.replication.factor", value = "1"),
          new ClusterConfigProperty(key = "share.coordinator.state.topic.num.partitions", value = "1")
        )
      ),
    )
  )
  def testShareAcknowledgeRequestSuccessReject(): Unit = {
    createTopicAndReturnLeaders(TOPIC, numPartitions = 3)
    val topicIds = getTopicIds
    val topicId = topicIds.get(TOPIC)
    val topicIdPartition = new TopicIdPartition(topicId, new TopicPartition(TOPIC, PARTITION))

    val send = util.List.of(topicIdPartition)

    val socket: Socket = connectAny()

    createOffsetsTopic()
    shareHeartbeat(MEMBER_ID, GROUP_ID, util.Map.of[String, Int](TOPIC, 3))

    // Send the first share fetch request to initialize the share partition
    sendFirstShareFetchRequest(MEMBER_ID, GROUP_ID, send, socket)

    initProducer()
    // Producing 10 records to the topic created above
    produceData(topicIdPartition, 10)

    // Send the second share fetch request to fetch the records produced above
    var shareSessionEpoch = ShareRequestMetadata.nextEpoch(ShareRequestMetadata.INITIAL_EPOCH)
    var metadata = new ShareRequestMetadata(MEMBER_ID, shareSessionEpoch)
    val acknowledgementsMapForFetch = util.Map.of[TopicIdPartition, util.List[ShareFetchRequestData.AcknowledgementBatch]]
    var shareFetchRequest = createShareFetchRequest(GROUP_ID, metadata, send, util.List.of, acknowledgementsMapForFetch)
    var shareFetchResponse = IntegrationTestUtils.sendAndReceive[ShareFetchResponse](shareFetchRequest, socket)

    var shareFetchResponseData = shareFetchResponse.data()
    assertEquals(Errors.NONE.code, shareFetchResponseData.errorCode)
    assertEquals(30000, shareFetchResponseData.acquisitionLockTimeoutMs)
    assertEquals(1, shareFetchResponseData.responses().size())
    assertEquals(topicId, shareFetchResponseData.responses().stream().findFirst().get().topicId())
    assertEquals(1, shareFetchResponseData.responses().stream().findFirst().get().partitions().size())

    var expectedFetchPartitionData = new ShareFetchResponseData.PartitionData()
      .setPartitionIndex(PARTITION)
      .setErrorCode(Errors.NONE.code())
      .setAcknowledgeErrorCode(Errors.NONE.code())
      .setAcquiredRecords(expectedAcquiredRecords(util.List.of(0), util.List.of(9), util.List.of(1)))

    var fetchPartitionData = shareFetchResponseData.responses().stream().findFirst().get().partitions().get(0)
    compareFetchResponsePartitions(expectedFetchPartitionData, fetchPartitionData)

    // Send a Share Acknowledge request to acknowledge the fetched records
    shareSessionEpoch = ShareRequestMetadata.nextEpoch(shareSessionEpoch)
    metadata = new ShareRequestMetadata(MEMBER_ID, shareSessionEpoch)
    val acknowledgementsMapForAcknowledge: util.Map[TopicIdPartition, util.List[ShareAcknowledgeRequestData.AcknowledgementBatch]] =
      util.Map.of(topicIdPartition, util.List.of(new ShareAcknowledgeRequestData.AcknowledgementBatch()
        .setFirstOffset(0)
        .setLastOffset(9)
        .setAcknowledgeTypes(util.List.of(3.toByte)))) // Reject the records
    val shareAcknowledgeRequest = createShareAcknowledgeRequest(GROUP_ID, metadata, acknowledgementsMapForAcknowledge)
    val shareAcknowledgeResponse = IntegrationTestUtils.sendAndReceive[ShareAcknowledgeResponse](shareAcknowledgeRequest, socket)

    val shareAcknowledgeResponseData = shareAcknowledgeResponse.data()
    assertEquals(Errors.NONE.code, shareAcknowledgeResponseData.errorCode)
    assertEquals(1, shareAcknowledgeResponseData.responses().size())
    assertEquals(topicId, shareAcknowledgeResponseData.responses().stream().findFirst().get().topicId())
    assertEquals(1, shareAcknowledgeResponseData.responses().stream().findFirst().get().partitions().size())
    assertEquals(30000, shareAcknowledgeResponseData.acquisitionLockTimeoutMs())

    val expectedAcknowledgePartitionData = new ShareAcknowledgeResponseData.PartitionData()
      .setPartitionIndex(PARTITION)
      .setErrorCode(Errors.NONE.code())

    val acknowledgePartitionData = shareAcknowledgeResponseData.responses().stream().findFirst().get().partitions().get(0)
    compareAcknowledgeResponsePartitions(expectedAcknowledgePartitionData, acknowledgePartitionData)

    // Producing 10 more records to the topic
    produceData(topicIdPartition, 10)

    // Sending a third share fetch request to check if acknowledgements were done successfully
    shareSessionEpoch = ShareRequestMetadata.nextEpoch(shareSessionEpoch)
    metadata = new ShareRequestMetadata(MEMBER_ID, shareSessionEpoch)
    shareFetchRequest = createShareFetchRequest(GROUP_ID, metadata, send, util.List.of, util.Map.of)
    shareFetchResponse = IntegrationTestUtils.sendAndReceive[ShareFetchResponse](shareFetchRequest, socket)

    shareFetchResponseData = shareFetchResponse.data()
    assertEquals(Errors.NONE.code, shareFetchResponseData.errorCode)
    assertEquals(30000, shareFetchResponseData.acquisitionLockTimeoutMs)
    assertEquals(1, shareFetchResponseData.responses().size())
    assertEquals(topicId, shareFetchResponseData.responses().stream().findFirst().get().topicId())
    assertEquals(1, shareFetchResponseData.responses().stream().findFirst().get().partitions().size())

    expectedFetchPartitionData = new ShareFetchResponseData.PartitionData()
      .setPartitionIndex(PARTITION)
      .setErrorCode(Errors.NONE.code())
      .setAcknowledgeErrorCode(Errors.NONE.code())
      .setAcquiredRecords(expectedAcquiredRecords(util.List.of(10), util.List.of(19), util.List.of(1))) // Only the records from offset 10 onwards should be fetched because records at offsets 0-9 have been rejected

    fetchPartitionData = shareFetchResponseData.responses().stream().findFirst().get().partitions().get(0)
    compareFetchResponsePartitions(expectedFetchPartitionData, fetchPartitionData)
  }

  @ClusterTests(
    Array(
      new ClusterTest(
        serverProperties = Array(
          new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
          new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1")
        )
      ),
      new ClusterTest(
        serverProperties = Array(
          new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
          new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1"),
          new ClusterConfigProperty(key = "group.share.persister.class.name", value = "org.apache.kafka.server.share.persister.DefaultStatePersister"),
          new ClusterConfigProperty(key = "share.coordinator.state.topic.replication.factor", value = "1"),
          new ClusterConfigProperty(key = "share.coordinator.state.topic.num.partitions", value = "1")
        )
      ),
    )
  )
  def testShareFetchRequestPiggybackedReject(): Unit = {
    createTopicAndReturnLeaders(TOPIC, numPartitions = 3)
    val topicIds = getTopicIds
    val topicId = topicIds.get(TOPIC)
    val topicIdPartition = new TopicIdPartition(topicId, new TopicPartition(TOPIC, PARTITION))

    val send = util.List.of(topicIdPartition)

    val socket: Socket = connectAny()

    createOffsetsTopic()
    shareHeartbeat(MEMBER_ID, GROUP_ID, util.Map.of[String, Int](TOPIC, 3))

    // Send the first share fetch request to initialize the share partition
    sendFirstShareFetchRequest(MEMBER_ID, GROUP_ID, send, socket)

    initProducer()
    // Producing 10 records to the topic created above
    produceData(topicIdPartition, 10)

    // Send the second share fetch request to fetch the records produced above
    var shareSessionEpoch = ShareRequestMetadata.nextEpoch(ShareRequestMetadata.INITIAL_EPOCH)
    var metadata = new ShareRequestMetadata(MEMBER_ID, shareSessionEpoch)
    var acknowledgementsMapForFetch = util.Map.of[TopicIdPartition, util.List[ShareFetchRequestData.AcknowledgementBatch]]
    var shareFetchRequest = createShareFetchRequest(GROUP_ID, metadata, send, util.List.of, acknowledgementsMapForFetch)
    var shareFetchResponse = IntegrationTestUtils.sendAndReceive[ShareFetchResponse](shareFetchRequest, socket)

    var shareFetchResponseData = shareFetchResponse.data()
    assertEquals(Errors.NONE.code, shareFetchResponseData.errorCode)
    assertEquals(30000, shareFetchResponseData.acquisitionLockTimeoutMs)
    assertEquals(1, shareFetchResponseData.responses().size())
    assertEquals(topicId, shareFetchResponseData.responses().stream().findFirst().get().topicId())
    assertEquals(1, shareFetchResponseData.responses().stream().findFirst().get().partitions().size())

    var expectedFetchPartitionData = new ShareFetchResponseData.PartitionData()
      .setPartitionIndex(PARTITION)
      .setErrorCode(Errors.NONE.code())
      .setAcknowledgeErrorCode(Errors.NONE.code())
      .setAcquiredRecords(expectedAcquiredRecords(util.List.of(0), util.List.of(9), util.List.of(1)))

    var fetchPartitionData = shareFetchResponseData.responses().stream().findFirst().get().partitions().get(0)
    compareFetchResponsePartitions(expectedFetchPartitionData, fetchPartitionData)

    // Producing 10 more records to the topic created above
    produceData(topicIdPartition, 10)

    // Send a third Share Fetch request with piggybacked acknowledgements
    shareSessionEpoch = ShareRequestMetadata.nextEpoch(shareSessionEpoch)
    metadata = new ShareRequestMetadata(MEMBER_ID, shareSessionEpoch)
    acknowledgementsMapForFetch = util.Map.of(topicIdPartition, util.List.of(new ShareFetchRequestData.AcknowledgementBatch()
      .setFirstOffset(0)
      .setLastOffset(9)
      .setAcknowledgeTypes(util.List.of(3.toByte)))) // Reject the records
    shareFetchRequest = createShareFetchRequest(GROUP_ID, metadata, send, util.List.of, acknowledgementsMapForFetch)
    shareFetchResponse = IntegrationTestUtils.sendAndReceive[ShareFetchResponse](shareFetchRequest, socket)

    shareFetchResponseData = shareFetchResponse.data()
    assertEquals(Errors.NONE.code, shareFetchResponseData.errorCode)
    assertEquals(30000, shareFetchResponseData.acquisitionLockTimeoutMs)
    assertEquals(1, shareFetchResponseData.responses().size())
    assertEquals(topicId, shareFetchResponseData.responses().stream().findFirst().get().topicId())
    assertEquals(1, shareFetchResponseData.responses().stream().findFirst().get().partitions().size())

    expectedFetchPartitionData = new ShareFetchResponseData.PartitionData()
      .setPartitionIndex(PARTITION)
      .setErrorCode(Errors.NONE.code())
      .setAcknowledgeErrorCode(Errors.NONE.code())
      .setAcquiredRecords(expectedAcquiredRecords(util.List.of(10), util.List.of(19), util.List.of(1))) // The records at offsets 0 to 9 will not be re fetched because they have been rejected

    fetchPartitionData = shareFetchResponseData.responses().stream().findFirst().get().partitions().get(0)
    compareFetchResponsePartitions(expectedFetchPartitionData, fetchPartitionData)

    // Producing 10 more records to the topic
    produceData(topicIdPartition, 10)

    // Sending a fourth share fetch request to confirm if acknowledgements were done successfully
    shareSessionEpoch = ShareRequestMetadata.nextEpoch(shareSessionEpoch)
    metadata = new ShareRequestMetadata(MEMBER_ID, shareSessionEpoch)
    shareFetchRequest = createShareFetchRequest(GROUP_ID, metadata, send, util.List.of, util.Map.of)
    shareFetchResponse = IntegrationTestUtils.sendAndReceive[ShareFetchResponse](shareFetchRequest, socket)

    shareFetchResponseData = shareFetchResponse.data()
    assertEquals(Errors.NONE.code, shareFetchResponseData.errorCode)
    assertEquals(30000, shareFetchResponseData.acquisitionLockTimeoutMs)
    assertEquals(1, shareFetchResponseData.responses().size())
    assertEquals(topicId, shareFetchResponseData.responses().stream().findFirst().get().topicId())
    assertEquals(1, shareFetchResponseData.responses().stream().findFirst().get().partitions().size())

    expectedFetchPartitionData = new ShareFetchResponseData.PartitionData()
      .setPartitionIndex(PARTITION)
      .setErrorCode(Errors.NONE.code())
      .setAcknowledgeErrorCode(Errors.NONE.code())
      .setAcquiredRecords(expectedAcquiredRecords(util.List.of(20), util.List.of(29), util.List.of(1))) // Only the records from offset 20 onwards should be fetched because records at offsets 0-9 have been rejected before and 10 to 19 are currently acquired

    fetchPartitionData = shareFetchResponseData.responses().stream().findFirst().get().partitions().get(0)
    compareFetchResponsePartitions(expectedFetchPartitionData, fetchPartitionData)
  }

  @ClusterTests(
    Array(
      new ClusterTest(
        serverProperties = Array(
          new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
          new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1"),
          new ClusterConfigProperty(key = "group.share.delivery.count.limit", value = "2") // Setting max delivery count config to 2
        )
      ),
      new ClusterTest(
        serverProperties = Array(
          new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
          new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1"),
          new ClusterConfigProperty(key = "group.share.persister.class.name", value = "org.apache.kafka.server.share.persister.DefaultStatePersister"),
          new ClusterConfigProperty(key = "share.coordinator.state.topic.replication.factor", value = "1"),
          new ClusterConfigProperty(key = "share.coordinator.state.topic.num.partitions", value = "1"),
          new ClusterConfigProperty(key = "group.share.delivery.count.limit", value = "2") // Setting max delivery count config to 2
        )
      ),
    )
  )
  def testShareAcknowledgeRequestMaxDeliveryAttemptExhausted(): Unit = {
    createTopicAndReturnLeaders(TOPIC, numPartitions = 3)
    val topicIds = getTopicIds
    val topicId = topicIds.get(TOPIC)
    val topicIdPartition = new TopicIdPartition(topicId, new TopicPartition(TOPIC, PARTITION))

    val send = util.List.of(topicIdPartition)

    val socket: Socket = connectAny()

    createOffsetsTopic()
    shareHeartbeat(MEMBER_ID, GROUP_ID, util.Map.of[String, Int](TOPIC, 3))

    // Send the first share fetch request to initialize the shar partition
    sendFirstShareFetchRequest(MEMBER_ID, GROUP_ID, send, socket)

    initProducer()
    // Producing 10 records to the topic created above
    produceData(topicIdPartition, 10)

    // Send the second share fetch request to fetch the records produced above
    var shareSessionEpoch = ShareRequestMetadata.nextEpoch(ShareRequestMetadata.INITIAL_EPOCH)
    var metadata = new ShareRequestMetadata(MEMBER_ID, shareSessionEpoch)
    val acknowledgementsMapForFetch = util.Map.of[TopicIdPartition, util.List[ShareFetchRequestData.AcknowledgementBatch]]
    var shareFetchRequest = createShareFetchRequest(GROUP_ID, metadata, send, util.List.of, acknowledgementsMapForFetch)
    var shareFetchResponse = IntegrationTestUtils.sendAndReceive[ShareFetchResponse](shareFetchRequest, socket)

    var shareFetchResponseData = shareFetchResponse.data()
    assertEquals(Errors.NONE.code, shareFetchResponseData.errorCode)
    assertEquals(30000, shareFetchResponseData.acquisitionLockTimeoutMs)
    assertEquals(1, shareFetchResponseData.responses().size())
    assertEquals(topicId, shareFetchResponseData.responses().stream().findFirst().get().topicId())
    assertEquals(1, shareFetchResponseData.responses().stream().findFirst().get().partitions().size())

    var expectedFetchPartitionData = new ShareFetchResponseData.PartitionData()
      .setPartitionIndex(PARTITION)
      .setErrorCode(Errors.NONE.code())
      .setAcknowledgeErrorCode(Errors.NONE.code())
      .setAcquiredRecords(expectedAcquiredRecords(util.List.of(0), util.List.of(9), util.List.of(1)))

    var fetchPartitionData = shareFetchResponseData.responses().stream().findFirst().get().partitions().get(0)
    compareFetchResponsePartitions(expectedFetchPartitionData, fetchPartitionData)

    // Send a Share Acknowledge request to acknowledge the fetched records
    shareSessionEpoch = ShareRequestMetadata.nextEpoch(shareSessionEpoch)
    metadata = new ShareRequestMetadata(MEMBER_ID, shareSessionEpoch)
    var acknowledgementsMapForAcknowledge: util.Map[TopicIdPartition, util.List[ShareAcknowledgeRequestData.AcknowledgementBatch]] =
      util.Map.of(topicIdPartition, util.List.of(new ShareAcknowledgeRequestData.AcknowledgementBatch()
        .setFirstOffset(0)
        .setLastOffset(9)
        .setAcknowledgeTypes(util.List.of(2.toByte)))) // Release the records
    var shareAcknowledgeRequest = createShareAcknowledgeRequest(GROUP_ID, metadata, acknowledgementsMapForAcknowledge)
    var shareAcknowledgeResponse = IntegrationTestUtils.sendAndReceive[ShareAcknowledgeResponse](shareAcknowledgeRequest, socket)

    var shareAcknowledgeResponseData = shareAcknowledgeResponse.data()
    assertEquals(Errors.NONE.code, shareAcknowledgeResponseData.errorCode)
    assertEquals(1, shareAcknowledgeResponseData.responses().size())
    assertEquals(topicId, shareAcknowledgeResponseData.responses().stream().findFirst().get().topicId())
    assertEquals(1, shareAcknowledgeResponseData.responses().stream().findFirst().get().partitions().size())
    assertEquals(30000, shareAcknowledgeResponseData.acquisitionLockTimeoutMs())

    var expectedAcknowledgePartitionData = new ShareAcknowledgeResponseData.PartitionData()
      .setPartitionIndex(PARTITION)
      .setErrorCode(Errors.NONE.code())

    var acknowledgePartitionData = shareAcknowledgeResponseData.responses().stream().findFirst().get().partitions().get(0)
    compareAcknowledgeResponsePartitions(expectedAcknowledgePartitionData, acknowledgePartitionData)

    // Sending a third share fetch request to check if acknowledgements were done successfully
    shareSessionEpoch = ShareRequestMetadata.nextEpoch(shareSessionEpoch)
    metadata = new ShareRequestMetadata(MEMBER_ID, shareSessionEpoch)
    shareFetchRequest = createShareFetchRequest(GROUP_ID, metadata, send, util.List.of, util.Map.of)
    shareFetchResponse = IntegrationTestUtils.sendAndReceive[ShareFetchResponse](shareFetchRequest, socket)

    shareFetchResponseData = shareFetchResponse.data()
    assertEquals(Errors.NONE.code, shareFetchResponseData.errorCode)
    assertEquals(30000, shareFetchResponseData.acquisitionLockTimeoutMs)
    assertEquals(1, shareFetchResponseData.responses().size())
    assertEquals(topicId, shareFetchResponseData.responses().stream().findFirst().get().topicId())
    assertEquals(1, shareFetchResponseData.responses().stream().findFirst().get().partitions().size())

    expectedFetchPartitionData = new ShareFetchResponseData.PartitionData()
      .setPartitionIndex(PARTITION)
      .setErrorCode(Errors.NONE.code())
      .setAcknowledgeErrorCode(Errors.NONE.code())
      .setAcquiredRecords(expectedAcquiredRecords(util.List.of(0), util.List.of(9), util.List.of(2))) // Records at offsets 0 to 9 should be fetched again because they were released with delivery count as 2

    fetchPartitionData = shareFetchResponseData.responses().stream().findFirst().get().partitions().get(0)
    compareFetchResponsePartitions(expectedFetchPartitionData, fetchPartitionData)

    // Send a Share Acknowledge request to acknowledge the fetched records
    shareSessionEpoch = ShareRequestMetadata.nextEpoch(shareSessionEpoch)
    metadata = new ShareRequestMetadata(MEMBER_ID, shareSessionEpoch)
    acknowledgementsMapForAcknowledge = util.Map.of(topicIdPartition, util.List.of(new ShareAcknowledgeRequestData.AcknowledgementBatch()
      .setFirstOffset(0)
      .setLastOffset(9)
      .setAcknowledgeTypes(util.List.of(2.toByte)))) // Release the records again
    shareAcknowledgeRequest = createShareAcknowledgeRequest(GROUP_ID, metadata, acknowledgementsMapForAcknowledge)
    shareAcknowledgeResponse = IntegrationTestUtils.sendAndReceive[ShareAcknowledgeResponse](shareAcknowledgeRequest, socket)

    shareAcknowledgeResponseData = shareAcknowledgeResponse.data()
    assertEquals(Errors.NONE.code, shareAcknowledgeResponseData.errorCode)
    assertEquals(1, shareAcknowledgeResponseData.responses().size())
    assertEquals(topicId, shareAcknowledgeResponseData.responses().stream().findFirst().get().topicId())
    assertEquals(1, shareAcknowledgeResponseData.responses().stream().findFirst().get().partitions().size())

    expectedAcknowledgePartitionData = new ShareAcknowledgeResponseData.PartitionData()
      .setPartitionIndex(PARTITION)
      .setErrorCode(Errors.NONE.code())

    acknowledgePartitionData = shareAcknowledgeResponseData.responses().stream().findFirst().get().partitions().get(0)
    compareAcknowledgeResponsePartitions(expectedAcknowledgePartitionData, acknowledgePartitionData)

    // Producing 10 new records to the topic
    produceData(topicIdPartition, 10)

    // Sending a fourth share fetch request to check if acknowledgements were done successfully
    shareSessionEpoch = ShareRequestMetadata.nextEpoch(shareSessionEpoch)
    metadata = new ShareRequestMetadata(MEMBER_ID, shareSessionEpoch)
    shareFetchRequest = createShareFetchRequest(GROUP_ID, metadata, send, util.List.of, util.Map.of)
    shareFetchResponse = IntegrationTestUtils.sendAndReceive[ShareFetchResponse](shareFetchRequest, socket)

    shareFetchResponseData = shareFetchResponse.data()
    assertEquals(Errors.NONE.code, shareFetchResponseData.errorCode)
    assertEquals(30000, shareFetchResponseData.acquisitionLockTimeoutMs)
    assertEquals(1, shareFetchResponseData.responses().size())
    assertEquals(topicId, shareFetchResponseData.responses().stream().findFirst().get().topicId())
    assertEquals(1, shareFetchResponseData.responses().stream().findFirst().get().partitions().size())

    expectedFetchPartitionData = new ShareFetchResponseData.PartitionData()
      .setPartitionIndex(PARTITION)
      .setErrorCode(Errors.NONE.code())
      .setAcknowledgeErrorCode(Errors.NONE.code())
      .setAcquiredRecords(expectedAcquiredRecords(util.List.of(10), util.List.of(19), util.List.of(1))) // Only new records from offset 10 to 19 will be fetched, records at offsets 0 to 9 have been archived because delivery count limit has been exceeded

    fetchPartitionData = shareFetchResponseData.responses().stream().findFirst().get().partitions().get(0)
    compareFetchResponsePartitions(expectedFetchPartitionData, fetchPartitionData)
  }

  @ClusterTests(
    Array(
      new ClusterTest(
        serverProperties = Array(
          new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
          new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1")
        )
      ),
      new ClusterTest(
        serverProperties = Array(
          new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
          new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1"),
          new ClusterConfigProperty(key = "group.share.persister.class.name", value = "org.apache.kafka.server.share.persister.DefaultStatePersister"),
          new ClusterConfigProperty(key = "share.coordinator.state.topic.replication.factor", value = "1"),
          new ClusterConfigProperty(key = "share.coordinator.state.topic.num.partitions", value = "1")
        )
      ),
    )
  )
  def testShareFetchRequestSuccessfulSharingBetweenMultipleConsumers(): Unit = {
    val memberId1 = Uuid.randomUuid()
    val memberId2 = Uuid.randomUuid()
    val memberId3 = Uuid.randomUuid()

    createTopicAndReturnLeaders(TOPIC, numPartitions = 3)
    val topicIds = getTopicIds
    val topicId = topicIds.get(TOPIC)
    val topicIdPartition = new TopicIdPartition(topicId, new TopicPartition(TOPIC, PARTITION))

    val send = util.List.of(topicIdPartition)

    val socket1: Socket = connectAny()
    val socket2: Socket = connectAny()
    val socket3: Socket = connectAny()

    createOffsetsTopic()
    shareHeartbeat(memberId1, GROUP_ID, util.Map.of[String, Int](TOPIC, 3))
    shareHeartbeat(memberId2, GROUP_ID, util.Map.of[String, Int](TOPIC, 3))
    shareHeartbeat(memberId3, GROUP_ID, util.Map.of[String, Int](TOPIC, 3))

    // Sending a dummy share fetch request to initialize the share partition
    sendFirstShareFetchRequest(memberId1, GROUP_ID, send, socket1)

    initProducer()
    // Producing 1 record to the topic created above
    produceData(topicIdPartition, 1)

    // Sending a share Fetch Request
    val metadata1: ShareRequestMetadata = new ShareRequestMetadata(memberId1, ShareRequestMetadata.INITIAL_EPOCH)
    val acknowledgementsMap1 = util.Map.of[TopicIdPartition, util.List[ShareFetchRequestData.AcknowledgementBatch]]
    val shareFetchRequest1 = createShareFetchRequest(GROUP_ID, metadata1, send, util.List.of, acknowledgementsMap1, minBytes = 100, maxBytes = 1500)
    val shareFetchResponse1 = IntegrationTestUtils.sendAndReceive[ShareFetchResponse](shareFetchRequest1, socket1)
    val shareFetchResponseData1 = shareFetchResponse1.data()
    val partitionData1 = shareFetchResponseData1.responses().stream().findFirst().get().partitions().get(0)

    // Producing 1 record to the topic created above
    produceData(topicIdPartition, 1)

    // Sending another share Fetch Request with same groupId to the same topicPartition but with different memberId,
    // mocking the behaviour of multiple share consumers from the same share group
    val metadata2: ShareRequestMetadata = new ShareRequestMetadata(memberId2, ShareRequestMetadata.INITIAL_EPOCH)
    val acknowledgementsMap2 = util.Map.of[TopicIdPartition, util.List[ShareFetchRequestData.AcknowledgementBatch]]
    val shareFetchRequest2 = createShareFetchRequest(GROUP_ID, metadata2, send, util.List.of, acknowledgementsMap2, minBytes = 100, maxBytes = 1500)
    val shareFetchResponse2 = IntegrationTestUtils.sendAndReceive[ShareFetchResponse](shareFetchRequest2, socket2)
    val shareFetchResponseData2 = shareFetchResponse2.data()
    val partitionData2 = shareFetchResponseData2.responses().stream().findFirst().get().partitions().get(0)

    // Producing 1 record to the topic created above
    produceData(topicIdPartition, 1)

    // Sending another share Fetch Request with same groupId to the same topicPartition but with different memberId,
    // mocking the behaviour of multiple share consumers from the same share group
    val metadata3: ShareRequestMetadata = new ShareRequestMetadata(memberId3, ShareRequestMetadata.INITIAL_EPOCH)
    val acknowledgementsMap3 = util.Map.of[TopicIdPartition, util.List[ShareFetchRequestData.AcknowledgementBatch]]
    val shareFetchRequest3 = createShareFetchRequest(GROUP_ID, metadata3, send, util.List.of, acknowledgementsMap3, minBytes = 100, maxBytes = 1500)
    val shareFetchResponse3 = IntegrationTestUtils.sendAndReceive[ShareFetchResponse](shareFetchRequest3, socket3)
    val shareFetchResponseData3 = shareFetchResponse3.data()
    val partitionData3 = shareFetchResponseData3.responses().stream().findFirst().get().partitions().get(0)

    // Each consumer should have received 1 record and any record should only be consumed by 1 consumer
    assertEquals(partitionData1.acquiredRecords().get(0).firstOffset(), partitionData1.acquiredRecords().get(0).lastOffset())
    assertEquals(partitionData1.acquiredRecords().get(0).firstOffset(), 0)

    assertEquals(partitionData2.acquiredRecords().get(0).firstOffset(), partitionData2.acquiredRecords().get(0).lastOffset())
    assertEquals(partitionData2.acquiredRecords().get(0).firstOffset(), 1)

    assertEquals(partitionData3.acquiredRecords().get(0).firstOffset(), partitionData3.acquiredRecords().get(0).lastOffset())
    assertEquals(partitionData3.acquiredRecords().get(0).firstOffset(), 2)

  }

  @ClusterTests(
    Array(
      new ClusterTest(
        serverProperties = Array(
          new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
          new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1")
        )
      ),
      new ClusterTest(
        serverProperties = Array(
          new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
          new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1"),
          new ClusterConfigProperty(key = "group.share.persister.class.name", value = "org.apache.kafka.server.share.persister.DefaultStatePersister"),
          new ClusterConfigProperty(key = "share.coordinator.state.topic.replication.factor", value = "1"),
          new ClusterConfigProperty(key = "share.coordinator.state.topic.num.partitions", value = "1")
        )
      ),
    )
  )
  def testShareFetchRequestNoSharingBetweenMultipleConsumersFromDifferentGroups(): Unit = {
    val groupId1: String = "group1"
    val groupId2: String = "group2"
    val groupId3: String = "group3"

    val memberId1 = Uuid.randomUuid()
    val memberId2 = Uuid.randomUuid()
    val memberId3 = Uuid.randomUuid()

    createTopicAndReturnLeaders(TOPIC, numPartitions = 3)
    val topicIds = getTopicIds
    val topicId = topicIds.get(TOPIC)
    val topicIdPartition = new TopicIdPartition(topicId, new TopicPartition(TOPIC, PARTITION))

    val send = util.List.of(topicIdPartition)

    val socket1: Socket = connectAny()
    val socket2: Socket = connectAny()
    val socket3: Socket = connectAny()

    createOffsetsTopic()
    shareHeartbeat(memberId1, groupId1, util.Map.of[String, Int](TOPIC, 3))
    shareHeartbeat(memberId2, groupId2, util.Map.of[String, Int](TOPIC, 3))
    shareHeartbeat(memberId3, groupId3, util.Map.of[String, Int](TOPIC, 3))

    // Sending 3 dummy share Fetch Requests with to initialize the share partitions for each share group\
    sendFirstShareFetchRequest(memberId1, groupId1, send, socket1)
    sendFirstShareFetchRequest(memberId2, groupId2, send, socket2)
    sendFirstShareFetchRequest(memberId3, groupId3, send, socket3)

    initProducer()
    // Producing 10 records to the topic created above
    produceData(topicIdPartition, 10)

    // Sending 3 share Fetch Requests with different groupId and different memberIds to the same topicPartition,
    // mocking the behaviour of 3 different share groups
    val metadata1 = new ShareRequestMetadata(memberId1, ShareRequestMetadata.nextEpoch(ShareRequestMetadata.INITIAL_EPOCH))
    val acknowledgementsMap1 = util.Map.of[TopicIdPartition, util.List[ShareFetchRequestData.AcknowledgementBatch]]
    val shareFetchRequest1 = createShareFetchRequest(groupId1, metadata1, send, util.List.of, acknowledgementsMap1)

    val metadata2 = new ShareRequestMetadata(memberId2, ShareRequestMetadata.nextEpoch(ShareRequestMetadata.INITIAL_EPOCH))
    val acknowledgementsMap2 = util.Map.of[TopicIdPartition, util.List[ShareFetchRequestData.AcknowledgementBatch]]
    val shareFetchRequest2 = createShareFetchRequest(groupId2, metadata2, send, util.List.of, acknowledgementsMap2)

    val metadata3 = new ShareRequestMetadata(memberId3, ShareRequestMetadata.nextEpoch(ShareRequestMetadata.INITIAL_EPOCH))
    val acknowledgementsMap3 = util.Map.of[TopicIdPartition, util.List[ShareFetchRequestData.AcknowledgementBatch]]
    val shareFetchRequest3 = createShareFetchRequest(groupId3, metadata3, send, util.List.of, acknowledgementsMap3)

    val shareFetchResponse1 = IntegrationTestUtils.sendAndReceive[ShareFetchResponse](shareFetchRequest1, socket1)
    val shareFetchResponse2 = IntegrationTestUtils.sendAndReceive[ShareFetchResponse](shareFetchRequest2, socket2)
    val shareFetchResponse3 = IntegrationTestUtils.sendAndReceive[ShareFetchResponse](shareFetchRequest3, socket3)


    val shareFetchResponseData1 = shareFetchResponse1.data()
    assertEquals(Errors.NONE.code, shareFetchResponseData1.errorCode)
    assertEquals(1, shareFetchResponseData1.responses().size())
    assertEquals(topicId, shareFetchResponseData1.responses().stream().findFirst().get().topicId())
    assertEquals(1, shareFetchResponseData1.responses().stream().findFirst().get().partitions().size())

    val partitionData1 = shareFetchResponseData1.responses().stream().findFirst().get().partitions().get(0)

    val shareFetchResponseData2 = shareFetchResponse2.data()
    assertEquals(Errors.NONE.code, shareFetchResponseData2.errorCode)
    assertEquals(1, shareFetchResponseData2.responses().size())
    assertEquals(topicId, shareFetchResponseData2.responses().stream().findFirst().get().topicId())
    assertEquals(1, shareFetchResponseData2.responses().stream().findFirst().get().partitions().size())

    val partitionData2 = shareFetchResponseData2.responses().stream().findFirst().get().partitions().get(0)

    val shareFetchResponseData3 = shareFetchResponse3.data()
    assertEquals(Errors.NONE.code, shareFetchResponseData3.errorCode)
    assertEquals(1, shareFetchResponseData3.responses().size())
    assertEquals(topicId, shareFetchResponseData3.responses().stream().findFirst().get().topicId())
    assertEquals(1, shareFetchResponseData3.responses().stream().findFirst().get().partitions().size())

    val partitionData3 = shareFetchResponseData3.responses().stream().findFirst().get().partitions().get(0)

    // All the consumers should consume all the records since they are part of different groups
    assertEquals(partitionData1.acquiredRecords(), expectedAcquiredRecords(util.List.of(0), util.List.of(9), util.List.of(1)))
    assertEquals(partitionData2.acquiredRecords(), expectedAcquiredRecords(util.List.of(0), util.List.of(9), util.List.of(1)))
    assertEquals(partitionData3.acquiredRecords(), expectedAcquiredRecords(util.List.of(0), util.List.of(9), util.List.of(1)))
  }

  @ClusterTests(
    Array(
      new ClusterTest(
        serverProperties = Array(
          new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
          new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1")
        )
      ),
      new ClusterTest(
        serverProperties = Array(
          new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
          new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1"),
          new ClusterConfigProperty(key = "group.share.persister.class.name", value = "org.apache.kafka.server.share.persister.DefaultStatePersister"),
          new ClusterConfigProperty(key = "share.coordinator.state.topic.replication.factor", value = "1"),
          new ClusterConfigProperty(key = "share.coordinator.state.topic.num.partitions", value = "1")
        )
      ),
    )
  )
  def testShareSessionCloseWithShareFetch(): Unit = {
    createTopicAndReturnLeaders(TOPIC)
    val topicIds = getTopicIds
    val topicId = topicIds.get(TOPIC)
    val topicIdPartition = new TopicIdPartition(topicId, new TopicPartition(TOPIC, PARTITION))

    val send = util.List.of(topicIdPartition)

    val socket: Socket = connectAny()

    createOffsetsTopic()
    shareHeartbeat(MEMBER_ID, GROUP_ID, util.Map.of[String, Int](TOPIC, 3))

    // Send the first share fetch request to initialize the share partition
    sendFirstShareFetchRequest(MEMBER_ID, GROUP_ID, send, socket)

    initProducer()
    // Producing 10 records to the topic created above
    produceData(topicIdPartition, 10)

    // Send the second share fetch request to fetch the records produced above
    var shareSessionEpoch = ShareRequestMetadata.nextEpoch(ShareRequestMetadata.INITIAL_EPOCH)
    var metadata = new ShareRequestMetadata(MEMBER_ID, shareSessionEpoch)
    var acknowledgementsMapForFetch = util.Map.of[TopicIdPartition, util.List[ShareFetchRequestData.AcknowledgementBatch]]
    var shareFetchRequest = createShareFetchRequest(GROUP_ID, metadata, send, util.List.of, acknowledgementsMapForFetch)
    var shareFetchResponse = IntegrationTestUtils.sendAndReceive[ShareFetchResponse](shareFetchRequest, socket)

    var shareFetchResponseData = shareFetchResponse.data()
    assertEquals(Errors.NONE.code, shareFetchResponseData.errorCode)
    assertEquals(30000, shareFetchResponseData.acquisitionLockTimeoutMs)
    assertEquals(1, shareFetchResponseData.responses().size())
    assertEquals(topicId, shareFetchResponseData.responses().stream().findFirst().get().topicId())
    assertEquals(1, shareFetchResponseData.responses().stream().findFirst().get().partitions().size())

    var expectedFetchPartitionData = new ShareFetchResponseData.PartitionData()
      .setPartitionIndex(PARTITION)
      .setErrorCode(Errors.NONE.code())
      .setAcknowledgeErrorCode(Errors.NONE.code())
      .setAcquiredRecords(expectedAcquiredRecords(util.List.of(0), util.List.of(9), util.List.of(1)))

    var fetchPartitionData = shareFetchResponseData.responses().stream().findFirst().get().partitions().get(0)
    compareFetchResponsePartitions(expectedFetchPartitionData, fetchPartitionData)

    // Producing 10 more records to the topic created above
    produceData(topicIdPartition, 10)

    // Send a third Share Fetch request with piggybacked acknowledgements
    shareSessionEpoch = ShareRequestMetadata.nextEpoch(shareSessionEpoch)
    metadata = new ShareRequestMetadata(MEMBER_ID, shareSessionEpoch)
    acknowledgementsMapForFetch = util.Map.of(topicIdPartition, util.List.of(new ShareFetchRequestData.AcknowledgementBatch()
      .setFirstOffset(0)
      .setLastOffset(9)
      .setAcknowledgeTypes(util.List.of(1.toByte)))) // Accept the records
    shareFetchRequest = createShareFetchRequest(GROUP_ID, metadata, send, util.List.of, acknowledgementsMapForFetch)
    shareFetchResponse = IntegrationTestUtils.sendAndReceive[ShareFetchResponse](shareFetchRequest, socket)

    shareFetchResponseData = shareFetchResponse.data()
    assertEquals(Errors.NONE.code, shareFetchResponseData.errorCode)
    assertEquals(30000, shareFetchResponseData.acquisitionLockTimeoutMs)
    assertEquals(1, shareFetchResponseData.responses().size())
    assertEquals(topicId, shareFetchResponseData.responses().stream().findFirst().get().topicId())
    assertEquals(1, shareFetchResponseData.responses().stream().findFirst().get().partitions().size())

    expectedFetchPartitionData = new ShareFetchResponseData.PartitionData()
      .setPartitionIndex(PARTITION)
      .setErrorCode(Errors.NONE.code())
      .setAcknowledgeErrorCode(Errors.NONE.code())
      .setAcquiredRecords(expectedAcquiredRecords(util.List.of(10), util.List.of(19), util.List.of(1))) // The records at offsets 0 to 9 will not be re fetched because they have been acknowledged

    fetchPartitionData = shareFetchResponseData.responses().stream().findFirst().get().partitions().get(0)
    compareFetchResponsePartitions(expectedFetchPartitionData, fetchPartitionData)

    // Sending a final fetch request to close the session
    shareSessionEpoch = ShareRequestMetadata.FINAL_EPOCH
    metadata = new ShareRequestMetadata(MEMBER_ID, shareSessionEpoch)
    acknowledgementsMapForFetch = util.Map.of(topicIdPartition, util.List.of(new ShareFetchRequestData.AcknowledgementBatch()
      .setFirstOffset(10)
      .setLastOffset(19)
      .setAcknowledgeTypes(util.List.of(1.toByte)))) // Accept the records
    shareFetchRequest = createShareFetchRequest(GROUP_ID, metadata, send, util.List.of, util.Map.of)
    shareFetchResponse = IntegrationTestUtils.sendAndReceive[ShareFetchResponse](shareFetchRequest, socket)

    shareFetchResponseData = shareFetchResponse.data()
    assertEquals(Errors.NONE.code, shareFetchResponseData.errorCode)
    assertEquals(30000, shareFetchResponseData.acquisitionLockTimeoutMs)
    assertEquals(0, shareFetchResponseData.responses().size()) // responses list will be empty because there are no responses for the final fetch request
  }

  @ClusterTests(
    Array(
      new ClusterTest(
        serverProperties = Array(
          new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
          new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1")
        )
      ),
      new ClusterTest(
        serverProperties = Array(
          new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
          new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1"),
          new ClusterConfigProperty(key = "group.share.persister.class.name", value = "org.apache.kafka.server.share.persister.DefaultStatePersister"),
          new ClusterConfigProperty(key = "share.coordinator.state.topic.replication.factor", value = "1"),
          new ClusterConfigProperty(key = "share.coordinator.state.topic.num.partitions", value = "1")
        )
      ),
    )
  )
  def testShareSessionCloseWithShareAcknowledge(): Unit = {
    createTopicAndReturnLeaders(TOPIC)
    val topicIds = getTopicIds
    val topicId = topicIds.get(TOPIC)
    val topicIdPartition = new TopicIdPartition(topicId, new TopicPartition(TOPIC, PARTITION))

    val send = util.List.of(topicIdPartition)

    val socket: Socket = connectAny()

    createOffsetsTopic()
    shareHeartbeat(MEMBER_ID, GROUP_ID, util.Map.of[String, Int](TOPIC, 3))

    // Send the first share fetch request to initialize the share partition
    sendFirstShareFetchRequest(MEMBER_ID, GROUP_ID, send, socket)

    initProducer()
    // Producing 10 records to the topic created above
    produceData(topicIdPartition, 10)

    // Send the second share fetch request to fetch the records produced above
    var shareSessionEpoch = ShareRequestMetadata.nextEpoch(ShareRequestMetadata.INITIAL_EPOCH)
    var metadata = new ShareRequestMetadata(MEMBER_ID, shareSessionEpoch)
    var acknowledgementsMapForFetch = util.Map.of[TopicIdPartition, util.List[ShareFetchRequestData.AcknowledgementBatch]]
    var shareFetchRequest = createShareFetchRequest(GROUP_ID, metadata, send, util.List.of, acknowledgementsMapForFetch)
    var shareFetchResponse = IntegrationTestUtils.sendAndReceive[ShareFetchResponse](shareFetchRequest, socket)

    var shareFetchResponseData = shareFetchResponse.data()
    assertEquals(Errors.NONE.code, shareFetchResponseData.errorCode)
    assertEquals(30000, shareFetchResponseData.acquisitionLockTimeoutMs)
    assertEquals(1, shareFetchResponseData.responses().size())
    assertEquals(topicId, shareFetchResponseData.responses().stream().findFirst().get().topicId())
    assertEquals(1, shareFetchResponseData.responses().stream().findFirst().get().partitions().size())

    var expectedFetchPartitionData = new ShareFetchResponseData.PartitionData()
      .setPartitionIndex(PARTITION)
      .setErrorCode(Errors.NONE.code())
      .setAcknowledgeErrorCode(Errors.NONE.code())
      .setAcquiredRecords(expectedAcquiredRecords(util.List.of(0), util.List.of(9), util.List.of(1)))

    var fetchPartitionData = shareFetchResponseData.responses().stream().findFirst().get().partitions().get(0)
    compareFetchResponsePartitions(expectedFetchPartitionData, fetchPartitionData)

    // Producing 10 more records to the topic created above
    produceData(topicIdPartition, 10)

    // Send a third Share Fetch request with piggybacked acknowledgements
    shareSessionEpoch = ShareRequestMetadata.nextEpoch(shareSessionEpoch)
    metadata = new ShareRequestMetadata(MEMBER_ID, shareSessionEpoch)
    acknowledgementsMapForFetch = util.Map.of(topicIdPartition, util.List.of(new ShareFetchRequestData.AcknowledgementBatch()
      .setFirstOffset(0)
      .setLastOffset(9)
      .setAcknowledgeTypes(util.List.of(1.toByte)))) // Accept the records
    shareFetchRequest = createShareFetchRequest(GROUP_ID, metadata, send, util.List.of, acknowledgementsMapForFetch)
    shareFetchResponse = IntegrationTestUtils.sendAndReceive[ShareFetchResponse](shareFetchRequest, socket)

    shareFetchResponseData = shareFetchResponse.data()
    assertEquals(Errors.NONE.code, shareFetchResponseData.errorCode)
    assertEquals(30000, shareFetchResponseData.acquisitionLockTimeoutMs)
    assertEquals(1, shareFetchResponseData.responses().size())
    assertEquals(topicId, shareFetchResponseData.responses().stream().findFirst().get().topicId())
    assertEquals(1, shareFetchResponseData.responses().stream().findFirst().get().partitions().size())

    expectedFetchPartitionData = new ShareFetchResponseData.PartitionData()
      .setPartitionIndex(PARTITION)
      .setErrorCode(Errors.NONE.code())
      .setAcknowledgeErrorCode(Errors.NONE.code())
      .setAcquiredRecords(expectedAcquiredRecords(util.List.of(10), util.List.of(19), util.List.of(1))) // The records at offsets 0 to 9 will not be re fetched because they have been acknowledged

    fetchPartitionData = shareFetchResponseData.responses().stream().findFirst().get().partitions().get(0)
    compareFetchResponsePartitions(expectedFetchPartitionData, fetchPartitionData)

    // Sending a Share Acknowledge request to close the session
    shareSessionEpoch = ShareRequestMetadata.FINAL_EPOCH
    metadata = new ShareRequestMetadata(MEMBER_ID, shareSessionEpoch)
    val acknowledgementsMapForAcknowledge: util.Map[TopicIdPartition, util.List[ShareAcknowledgeRequestData.AcknowledgementBatch]] =
      util.Map.of(topicIdPartition, util.List.of(new ShareAcknowledgeRequestData.AcknowledgementBatch()
        .setFirstOffset(10)
        .setLastOffset(19)
        .setAcknowledgeTypes(util.List.of(1.toByte)))) // Accept the records
    val shareAcknowledgeRequest = createShareAcknowledgeRequest(GROUP_ID, metadata, acknowledgementsMapForAcknowledge)
    val shareAcknowledgeResponse = IntegrationTestUtils.sendAndReceive[ShareAcknowledgeResponse](shareAcknowledgeRequest, socket)

    val shareAcknowledgeResponseData = shareAcknowledgeResponse.data()
    assertEquals(Errors.NONE.code, shareAcknowledgeResponseData.errorCode)
    assertEquals(1, shareAcknowledgeResponseData.responses().size())
    assertEquals(topicId, shareAcknowledgeResponseData.responses().stream().findFirst().get().topicId())
    assertEquals(1, shareAcknowledgeResponseData.responses().stream().findFirst().get().partitions().size())

    val expectedAcknowledgePartitionData = new ShareAcknowledgeResponseData.PartitionData()
      .setPartitionIndex(PARTITION)
      .setErrorCode(Errors.NONE.code())

    val acknowledgePartitionData = shareAcknowledgeResponseData.responses().stream().findFirst().get().partitions().get(0)
    compareAcknowledgeResponsePartitions(expectedAcknowledgePartitionData, acknowledgePartitionData)
  }

  @ClusterTests(
    Array(
      new ClusterTest(
        serverProperties = Array(
          new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
          new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1")
        )
      ),
      new ClusterTest(
        serverProperties = Array(
          new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
          new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1"),
          new ClusterConfigProperty(key = "group.share.persister.class.name", value = "org.apache.kafka.server.share.persister.DefaultStatePersister"),
          new ClusterConfigProperty(key = "share.coordinator.state.topic.replication.factor", value = "1"),
          new ClusterConfigProperty(key = "share.coordinator.state.topic.num.partitions", value = "1")
        )
      ),
    )
  )
  def testShareFetchInitialEpochWithAcknowledgements(): Unit = {
    createTopicAndReturnLeaders(TOPIC, numPartitions = 3)
    val topicIds = getTopicIds
    val topicId = topicIds.get(TOPIC)
    val topicIdPartition = new TopicIdPartition(topicId, new TopicPartition(TOPIC, PARTITION))

    initProducer()
    // Producing 10 records to the topic created above
    produceData(topicIdPartition, 10)

    val send = util.List.of(topicIdPartition)

    val socket: Socket = connectAny()

    val metadata: ShareRequestMetadata = new ShareRequestMetadata(MEMBER_ID, ShareRequestMetadata.INITIAL_EPOCH)
    val acknowledgementsMap: util.Map[TopicIdPartition, util.List[ShareFetchRequestData.AcknowledgementBatch]] =
      util.Map.of(topicIdPartition, util.List.of(new ShareFetchRequestData.AcknowledgementBatch()
        .setFirstOffset(0)
        .setLastOffset(9)
        .setAcknowledgeTypes(util.List.of(1.toByte)))) // Acknowledgements in the Initial Fetch Request
    val shareFetchRequest = createShareFetchRequest(GROUP_ID, metadata, send, util.List.of, acknowledgementsMap)
    val shareFetchResponse = IntegrationTestUtils.sendAndReceive[ShareFetchResponse](shareFetchRequest, socket)

    val shareFetchResponseData = shareFetchResponse.data()
    // The response will have a top level error code because this is an Initial Fetch request with acknowledgement data present
    assertEquals(Errors.INVALID_REQUEST.code(), shareFetchResponseData.errorCode)
    assertEquals(0, shareFetchResponse.data.acquisitionLockTimeoutMs)
  }

  @ClusterTests(
    Array(
      new ClusterTest(
        serverProperties = Array(
          new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
          new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1")
        )
      ),
      new ClusterTest(
        serverProperties = Array(
          new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
          new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1"),
          new ClusterConfigProperty(key = "group.share.persister.class.name", value = "org.apache.kafka.server.share.persister.DefaultStatePersister"),
          new ClusterConfigProperty(key = "share.coordinator.state.topic.replication.factor", value = "1"),
          new ClusterConfigProperty(key = "share.coordinator.state.topic.num.partitions", value = "1")
        )
      ),
    )
  )
  def testShareAcknowledgeInitialRequestError(): Unit = {
    createTopicAndReturnLeaders(TOPIC, numPartitions = 3)
    val topicIds = getTopicIds
    val topicId = topicIds.get(TOPIC)
    val topicIdPartition = new TopicIdPartition(topicId, new TopicPartition(TOPIC, PARTITION))

    val socket: Socket = connectAny()

    // Send the share fetch request to fetch the records produced above
    val metadata: ShareRequestMetadata = new ShareRequestMetadata(MEMBER_ID, ShareRequestMetadata.INITIAL_EPOCH)
    val acknowledgementsMap: util.Map[TopicIdPartition, util.List[ShareAcknowledgeRequestData.AcknowledgementBatch]] =
      util.Map.of(topicIdPartition,
        util.List.of(new ShareAcknowledgeRequestData.AcknowledgementBatch()
          .setFirstOffset(0)
          .setLastOffset(9)
          .setAcknowledgeTypes(util.List.of(1.toByte))))
    val shareAcknowledgeRequest = createShareAcknowledgeRequest(GROUP_ID, metadata, acknowledgementsMap)
    val shareAcknowledgeResponse = IntegrationTestUtils.sendAndReceive[ShareAcknowledgeResponse](shareAcknowledgeRequest, socket)

    val shareAcknowledgeResponseData = shareAcknowledgeResponse.data()
    assertEquals(Errors.INVALID_SHARE_SESSION_EPOCH.code, shareAcknowledgeResponseData.errorCode)
    assertEquals(0, shareAcknowledgeResponseData.acquisitionLockTimeoutMs())
  }

  @ClusterTests(
    Array(
      new ClusterTest(
        serverProperties = Array(
          new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
          new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1")
        )
      ),
      new ClusterTest(
        serverProperties = Array(
          new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
          new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1"),
          new ClusterConfigProperty(key = "group.share.persister.class.name", value = "org.apache.kafka.server.share.persister.DefaultStatePersister"),
          new ClusterConfigProperty(key = "share.coordinator.state.topic.replication.factor", value = "1"),
          new ClusterConfigProperty(key = "share.coordinator.state.topic.num.partitions", value = "1")
        )
      ),
    )
  )
  def testShareFetchRequestInvalidShareSessionEpoch(): Unit = {
    createTopicAndReturnLeaders(TOPIC, numPartitions = 3)
    val topicIds = getTopicIds
    val topicId = topicIds.get(TOPIC)
    val topicIdPartition = new TopicIdPartition(topicId, new TopicPartition(TOPIC, PARTITION))

    val send = util.List.of(topicIdPartition)

    val socket: Socket = connectAny()

    createOffsetsTopic()
    shareHeartbeat(MEMBER_ID, GROUP_ID, util.Map.of[String, Int](TOPIC, 3))

    // Send the first share fetch request to initialize the share partition
    sendFirstShareFetchRequest(MEMBER_ID, GROUP_ID, send, socket)

    initProducer()
    // Producing 10 records to the topic created above
    produceData(topicIdPartition, 10)

    // Send the second share fetch request to fetch the records produced above
    var shareSessionEpoch = ShareRequestMetadata.nextEpoch(ShareRequestMetadata.INITIAL_EPOCH)
    var metadata = new ShareRequestMetadata(MEMBER_ID, shareSessionEpoch)
    var shareFetchRequest = createShareFetchRequest(GROUP_ID, metadata, send, util.List.of, util.Map.of)
    var shareFetchResponse = IntegrationTestUtils.sendAndReceive[ShareFetchResponse](shareFetchRequest, socket)

    var shareFetchResponseData = shareFetchResponse.data()
    assertEquals(Errors.NONE.code, shareFetchResponseData.errorCode)
    assertEquals(30000, shareFetchResponseData.acquisitionLockTimeoutMs)
    assertEquals(1, shareFetchResponseData.responses().size())
    assertEquals(topicId, shareFetchResponseData.responses().stream().findFirst().get().topicId())
    assertEquals(1, shareFetchResponseData.responses().stream().findFirst().get().partitions().size())

    val expectedPartitionData = new ShareFetchResponseData.PartitionData()
      .setPartitionIndex(PARTITION)
      .setErrorCode(Errors.NONE.code())
      .setAcknowledgeErrorCode(Errors.NONE.code())
      .setAcquiredRecords(expectedAcquiredRecords(util.List.of(0), util.List.of(9), util.List.of(1)))

    val partitionData = shareFetchResponseData.responses().stream().findFirst().get().partitions().get(0)
    compareFetchResponsePartitions(expectedPartitionData, partitionData)

    // Sending a thord Share Fetch request with invalid share session epoch
    shareSessionEpoch = ShareRequestMetadata.nextEpoch(ShareRequestMetadata.nextEpoch(shareSessionEpoch))
    metadata = new ShareRequestMetadata(MEMBER_ID, shareSessionEpoch)
    shareFetchRequest = createShareFetchRequest(GROUP_ID, metadata, send, util.List.of, util.Map.of)
    shareFetchResponse = IntegrationTestUtils.sendAndReceive[ShareFetchResponse](shareFetchRequest, socket)

    shareFetchResponseData = shareFetchResponse.data()
    assertEquals(Errors.INVALID_SHARE_SESSION_EPOCH.code, shareFetchResponseData.errorCode)
  }

  @ClusterTests(
    Array(
      new ClusterTest(
        serverProperties = Array(
          new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
          new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1")
        )
      ),
      new ClusterTest(
        serverProperties = Array(
          new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
          new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1"),
          new ClusterConfigProperty(key = "group.share.persister.class.name", value = "org.apache.kafka.server.share.persister.DefaultStatePersister"),
          new ClusterConfigProperty(key = "share.coordinator.state.topic.replication.factor", value = "1"),
          new ClusterConfigProperty(key = "share.coordinator.state.topic.num.partitions", value = "1")
        )
      ),
    )
  )
  def testShareAcknowledgeRequestInvalidShareSessionEpoch(): Unit = {
    createTopicAndReturnLeaders(TOPIC, numPartitions = 3)
    val topicIds = getTopicIds
    val topicId = topicIds.get(TOPIC)
    val topicIdPartition = new TopicIdPartition(topicId, new TopicPartition(TOPIC, PARTITION))

    val send = util.List.of(topicIdPartition)

    val socket: Socket = connectAny()

    createOffsetsTopic()
    shareHeartbeat(MEMBER_ID, GROUP_ID, util.Map.of[String, Int](TOPIC, 3))

    // Send the first share fetch request to initialize the share partition
    sendFirstShareFetchRequest(MEMBER_ID, GROUP_ID, send, socket)

    initProducer()
    // Producing 10 records to the topic created above
    produceData(topicIdPartition, 10)

    // Send the second share fetch request to fetch the records produced above
    var shareSessionEpoch = ShareRequestMetadata.nextEpoch(ShareRequestMetadata.INITIAL_EPOCH)
    var metadata = new ShareRequestMetadata(MEMBER_ID, shareSessionEpoch)
    val shareFetchRequest = createShareFetchRequest(GROUP_ID, metadata, send, util.List.of, util.Map.of)
    val shareFetchResponse = IntegrationTestUtils.sendAndReceive[ShareFetchResponse](shareFetchRequest, socket)

    val shareFetchResponseData = shareFetchResponse.data()
    assertEquals(Errors.NONE.code, shareFetchResponseData.errorCode)
    assertEquals(30000, shareFetchResponseData.acquisitionLockTimeoutMs)
    assertEquals(1, shareFetchResponseData.responses().size())
    assertEquals(topicId, shareFetchResponseData.responses().stream().findFirst().get().topicId())
    assertEquals(1, shareFetchResponseData.responses().stream().findFirst().get().partitions().size())

    val expectedPartitionData = new ShareFetchResponseData.PartitionData()
      .setPartitionIndex(PARTITION)
      .setErrorCode(Errors.NONE.code())
      .setAcknowledgeErrorCode(Errors.NONE.code())
      .setAcquiredRecords(expectedAcquiredRecords(util.List.of(0), util.List.of(9), util.List.of(1)))

    val partitionData = shareFetchResponseData.responses().stream().findFirst().get().partitions().get(0)
    compareFetchResponsePartitions(expectedPartitionData, partitionData)

    // Sending Share Acknowledge request with invalid share session epoch
    shareSessionEpoch = ShareRequestMetadata.nextEpoch(ShareRequestMetadata.nextEpoch(shareSessionEpoch))
    metadata = new ShareRequestMetadata(MEMBER_ID, shareSessionEpoch)
    val acknowledgementsMap: util.Map[TopicIdPartition, util.List[ShareAcknowledgeRequestData.AcknowledgementBatch]] =
      util.Map.of(topicIdPartition, util.List.of(new ShareAcknowledgeRequestData.AcknowledgementBatch()
        .setFirstOffset(0)
        .setLastOffset(9)
        .setAcknowledgeTypes(util.List.of(1.toByte))))
    val shareAcknowledgeRequest = createShareAcknowledgeRequest(GROUP_ID, metadata, acknowledgementsMap)
    val shareAcknowledgeResponse = IntegrationTestUtils.sendAndReceive[ShareAcknowledgeResponse](shareAcknowledgeRequest, socket)

    val shareAcknowledgeResponseData = shareAcknowledgeResponse.data()
    assertEquals(Errors.INVALID_SHARE_SESSION_EPOCH.code, shareAcknowledgeResponseData.errorCode)
    assertEquals(0, shareAcknowledgeResponseData.acquisitionLockTimeoutMs())
  }

  @ClusterTests(
    Array(
      new ClusterTest(
        serverProperties = Array(
          new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
          new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1")
        )
      ),
      new ClusterTest(
        serverProperties = Array(
          new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
          new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1"),
          new ClusterConfigProperty(key = "group.share.persister.class.name", value = "org.apache.kafka.server.share.persister.DefaultStatePersister"),
          new ClusterConfigProperty(key = "share.coordinator.state.topic.replication.factor", value = "1"),
          new ClusterConfigProperty(key = "share.coordinator.state.topic.num.partitions", value = "1")
        )
      ),
    )
  )
  def testShareFetchRequestShareSessionNotFound(): Unit = {
    val wrongMemberId = Uuid.randomUuid()

    createTopicAndReturnLeaders(TOPIC, numPartitions = 3)
    val topicIds = getTopicIds
    val topicId = topicIds.get(TOPIC)
    val topicIdPartition = new TopicIdPartition(topicId, new TopicPartition(TOPIC, PARTITION))

    val send = util.List.of(topicIdPartition)

    val socket: Socket = connectAny()

    createOffsetsTopic()
    shareHeartbeat(MEMBER_ID, GROUP_ID, util.Map.of[String, Int](TOPIC, 3))

    // Send the first share fetch request to initialize the share partition
    sendFirstShareFetchRequest(MEMBER_ID, GROUP_ID, send, socket)

    initProducer()
    // Producing 10 records to the topic created above
    produceData(topicIdPartition, 10)

    // Send the second share fetch request to fetch the records produced above
    var shareSessionEpoch = ShareRequestMetadata.nextEpoch(ShareRequestMetadata.INITIAL_EPOCH)
    var metadata = new ShareRequestMetadata(MEMBER_ID, shareSessionEpoch)
    var shareFetchRequest = createShareFetchRequest(GROUP_ID, metadata, send, util.List.of, util.Map.of)
    var shareFetchResponse = IntegrationTestUtils.sendAndReceive[ShareFetchResponse](shareFetchRequest, socket)

    var shareFetchResponseData = shareFetchResponse.data()
    assertEquals(Errors.NONE.code, shareFetchResponseData.errorCode)
    assertEquals(30000, shareFetchResponseData.acquisitionLockTimeoutMs)
    assertEquals(1, shareFetchResponseData.responses().size())
    assertEquals(topicId, shareFetchResponseData.responses().stream().findFirst().get().topicId())
    assertEquals(1, shareFetchResponseData.responses().stream().findFirst().get().partitions().size())
    val expectedPartitionData = new ShareFetchResponseData.PartitionData()
      .setPartitionIndex(PARTITION)
      .setErrorCode(Errors.NONE.code())
      .setAcknowledgeErrorCode(Errors.NONE.code())
      .setAcquiredRecords(expectedAcquiredRecords(util.List.of(0), util.List.of(9), util.List.of(1)))

    val partitionData = shareFetchResponseData.responses().stream().findFirst().get().partitions().get(0)
    compareFetchResponsePartitions(expectedPartitionData, partitionData)

    // Sending a third Share Fetch request with wrong memberId
    shareSessionEpoch = ShareRequestMetadata.nextEpoch(shareSessionEpoch)
    metadata = new ShareRequestMetadata(wrongMemberId, shareSessionEpoch)
    shareFetchRequest = createShareFetchRequest(GROUP_ID, metadata, send, util.List.of, util.Map.of)
    shareFetchResponse = IntegrationTestUtils.sendAndReceive[ShareFetchResponse](shareFetchRequest, socket)

    shareFetchResponseData = shareFetchResponse.data()
    assertEquals(Errors.SHARE_SESSION_NOT_FOUND.code, shareFetchResponseData.errorCode)
  }

  @ClusterTests(
    Array(
      new ClusterTest(
        serverProperties = Array(
          new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
          new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1"),
          new ClusterConfigProperty(key = "group.share.max.share.sessions", value = "2"),
          new ClusterConfigProperty(key = "group.share.max.size", value = "2")
        )
      ),
      new ClusterTest(
        serverProperties = Array(
          new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
          new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1"),
          new ClusterConfigProperty(key = "group.share.persister.class.name", value = "org.apache.kafka.server.share.persister.DefaultStatePersister"),
          new ClusterConfigProperty(key = "share.coordinator.state.topic.replication.factor", value = "1"),
          new ClusterConfigProperty(key = "share.coordinator.state.topic.num.partitions", value = "1"),
          new ClusterConfigProperty(key = "group.share.max.share.sessions", value = "2"),
          new ClusterConfigProperty(key = "group.share.max.size", value = "2")
        )
      ),
    )
  )
  def testShareSessionEvictedOnConnectionDrop(): Unit = {
    val memberId1 = Uuid.randomUuid()
    val memberId2 = Uuid.randomUuid()
    val memberId3 = Uuid.randomUuid()

    createTopicAndReturnLeaders(TOPIC, numPartitions = 3)
    val topicIds = getTopicIds
    val topicId = topicIds.get(TOPIC)
    val topicIdPartition = new TopicIdPartition(topicId, new TopicPartition(TOPIC, PARTITION))

    val send = util.List.of(topicIdPartition)

    val socket1: Socket = connectAny()
    val socket2: Socket = connectAny()
    val socket3: Socket = connectAny()

    // member1 sends share fetch request to register its share session. Note it does not close the socket connection after.
    TestUtils.waitUntilTrue(() => {
      val metadata = new ShareRequestMetadata(memberId1, ShareRequestMetadata.INITIAL_EPOCH)
      val shareFetchRequest = createShareFetchRequest(GROUP_ID, metadata, send, util.List.of, util.Map.of)
      val shareFetchResponse = IntegrationTestUtils.sendAndReceive[ShareFetchResponse](shareFetchRequest, socket1)
      val shareFetchResponseData = shareFetchResponse.data()
      shareFetchResponseData.errorCode == Errors.NONE.code
    }, "Share fetch request failed", 5000)

    // member2 sends share fetch request to register its share session. Note it does not close the socket connection after.
    TestUtils.waitUntilTrue(() => {
      val metadata = new ShareRequestMetadata(memberId2, ShareRequestMetadata.INITIAL_EPOCH)
      val shareFetchRequest = createShareFetchRequest(GROUP_ID, metadata, send, util.List.of, util.Map.of)
      val shareFetchResponse = IntegrationTestUtils.sendAndReceive[ShareFetchResponse](shareFetchRequest, socket2)
      val shareFetchResponseData = shareFetchResponse.data()
      shareFetchResponseData.errorCode == Errors.NONE.code
    }, "Share fetch request failed", 5000)

    // member3 sends share fetch request to register its share session. Since the maximum number of share sessions that could
    // exist in the share session cache is 2 (group.share.max.share.sessions), the attempt to register a third
    // share session with the ShareSessionCache would throw SHARE_SESSION_LIMIT_REACHED
    TestUtils.waitUntilTrue(() => {
      val metadata = new ShareRequestMetadata(memberId3, ShareRequestMetadata.INITIAL_EPOCH)
      val shareFetchRequest = createShareFetchRequest(GROUP_ID, metadata, send, util.List.of, util.Map.of)
      val shareFetchResponse = IntegrationTestUtils.sendAndReceive[ShareFetchResponse](shareFetchRequest, socket3)
      val shareFetchResponseData = shareFetchResponse.data()
      shareFetchResponseData.errorCode == Errors.SHARE_SESSION_LIMIT_REACHED.code
    }, "Share fetch request failed", 5000)

    // Now we will close the socket connections for the members, mimicking a client disconnection
    closeSockets()

    val socket4: Socket = connectAny()

    // Since one of the socket connections was closed before, the corresponding share session was dropped from the ShareSessionCache
    // on the broker. Now, since the cache is not full, new share sessions can be registered
    TestUtils.waitUntilTrue(() => {
      val metadata = new ShareRequestMetadata(memberId3, ShareRequestMetadata.INITIAL_EPOCH)
      val shareFetchRequest = createShareFetchRequest(GROUP_ID, metadata, send, util.List.of, util.Map.of)
      val shareFetchResponse = IntegrationTestUtils.sendAndReceive[ShareFetchResponse](shareFetchRequest, socket4)
      val shareFetchResponseData = shareFetchResponse.data()
      shareFetchResponseData.errorCode == Errors.NONE.code
    }, "Share fetch request failed", 5000)
  }

  @ClusterTests(
    Array(
      new ClusterTest(
        serverProperties = Array(
          new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
          new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1")
        )
      ),
      new ClusterTest(
        serverProperties = Array(
          new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
          new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1"),
          new ClusterConfigProperty(key = "group.share.persister.class.name", value = "org.apache.kafka.server.share.persister.DefaultStatePersister"),
          new ClusterConfigProperty(key = "share.coordinator.state.topic.replication.factor", value = "1"),
          new ClusterConfigProperty(key = "share.coordinator.state.topic.num.partitions", value = "1")
        )
      ),
    )
  )
  def testShareAcknowledgeRequestShareSessionNotFound(): Unit = {
    val wrongMemberId = Uuid.randomUuid()

    createTopicAndReturnLeaders(TOPIC, numPartitions = 3)
    val topicIds = getTopicIds
    val topicId = topicIds.get(TOPIC)
    val topicIdPartition = new TopicIdPartition(topicId, new TopicPartition(TOPIC, PARTITION))

    val send = util.List.of(topicIdPartition)

    val socket: Socket = connectAny()

    createOffsetsTopic()
    shareHeartbeat(MEMBER_ID, GROUP_ID, util.Map.of[String, Int](TOPIC, 3))

    // Send the first share fetch request to initialize the share partition
    sendFirstShareFetchRequest(MEMBER_ID, GROUP_ID, send, socket)

    initProducer()
    // Producing 10 records to the topic created above
    produceData(topicIdPartition, 10)

    // Send the second share fetch request to fetch the records produced above
    var shareSessionEpoch = ShareRequestMetadata.nextEpoch(ShareRequestMetadata.INITIAL_EPOCH)
    var metadata = new ShareRequestMetadata(MEMBER_ID, shareSessionEpoch)
    val shareFetchRequest = createShareFetchRequest(GROUP_ID, metadata, send, util.List.of, util.Map.of)
    val shareFetchResponse = IntegrationTestUtils.sendAndReceive[ShareFetchResponse](shareFetchRequest, socket)

    val shareFetchResponseData = shareFetchResponse.data()
    assertEquals(Errors.NONE.code, shareFetchResponseData.errorCode)
    assertEquals(30000, shareFetchResponseData.acquisitionLockTimeoutMs)
    assertEquals(1, shareFetchResponseData.responses().size())
    assertEquals(topicId, shareFetchResponseData.responses().stream().findFirst().get().topicId())
    assertEquals(1, shareFetchResponseData.responses().stream().findFirst().get().partitions().size())

    val expectedPartitionData = new ShareFetchResponseData.PartitionData()
      .setPartitionIndex(PARTITION)
      .setErrorCode(Errors.NONE.code())
      .setAcknowledgeErrorCode(Errors.NONE.code())
      .setAcquiredRecords(expectedAcquiredRecords(util.List.of(0), util.List.of(9), util.List.of(1)))

    val partitionData = shareFetchResponseData.responses().stream().findFirst().get().partitions().get(0)
    compareFetchResponsePartitions(expectedPartitionData, partitionData)

    // Sending a Share Acknowledge request with wrong memberId
    shareSessionEpoch = ShareRequestMetadata.nextEpoch(shareSessionEpoch)
    metadata = new ShareRequestMetadata(wrongMemberId, shareSessionEpoch)
    val acknowledgementsMap: util.Map[TopicIdPartition, util.List[ShareAcknowledgeRequestData.AcknowledgementBatch]] =
      util.Map.of(topicIdPartition, util.List.of(new ShareAcknowledgeRequestData.AcknowledgementBatch()
        .setFirstOffset(0)
        .setLastOffset(9)
        .setAcknowledgeTypes(util.List.of(1.toByte))))
    val shareAcknowledgeRequest = createShareAcknowledgeRequest(GROUP_ID, metadata, acknowledgementsMap)
    val shareAcknowledgeResponse = IntegrationTestUtils.sendAndReceive[ShareAcknowledgeResponse](shareAcknowledgeRequest, socket)

    val shareAcknowledgeResponseData = shareAcknowledgeResponse.data()
    assertEquals(Errors.SHARE_SESSION_NOT_FOUND.code, shareAcknowledgeResponseData.errorCode)
  }

  @ClusterTests(
    Array(
      new ClusterTest(
        serverProperties = Array(
          new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
          new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1")
        )
      ),
      new ClusterTest(
        serverProperties = Array(
          new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
          new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1"),
          new ClusterConfigProperty(key = "group.share.persister.class.name", value = "org.apache.kafka.server.share.persister.DefaultStatePersister"),
          new ClusterConfigProperty(key = "share.coordinator.state.topic.replication.factor", value = "1"),
          new ClusterConfigProperty(key = "share.coordinator.state.topic.num.partitions", value = "1")
        )
      ),
    )
  )
  def testShareFetchRequestForgetTopicPartitions(): Unit = {
    val partition1 = 0
    val partition2 = 1

    createTopicAndReturnLeaders(TOPIC, numPartitions = 3)
    val topicIds = getTopicIds
    val topicId = topicIds.get(TOPIC)
    val topicIdPartition1 = new TopicIdPartition(topicId, new TopicPartition(TOPIC, partition1))
    val topicIdPartition2 = new TopicIdPartition(topicId, new TopicPartition(TOPIC, partition2))

    val send = util.List.of(topicIdPartition1, topicIdPartition2)

    val socket: Socket = connectAny()

    createOffsetsTopic()
    shareHeartbeat(MEMBER_ID, GROUP_ID, util.Map.of[String, Int](TOPIC, 3))

    // Send the first share fetch request to initialize the share partition
    sendFirstShareFetchRequest(MEMBER_ID, GROUP_ID, send, socket)

    initProducer()
    // Producing 10 records to the topic partitions created above
    produceData(topicIdPartition1, 10)
    produceData(topicIdPartition2, 10)

    // Send the second share fetch request to fetch the records produced above
    var shareSessionEpoch = ShareRequestMetadata.nextEpoch(ShareRequestMetadata.INITIAL_EPOCH)
    var metadata = new ShareRequestMetadata(MEMBER_ID, shareSessionEpoch)
    val acknowledgementsMap = util.Map.of[TopicIdPartition, util.List[ShareFetchRequestData.AcknowledgementBatch]]
    var shareFetchRequest = createShareFetchRequest(GROUP_ID, metadata, send, util.List.of, acknowledgementsMap)

    // For the multi partition fetch request, the response may not be available in the first attempt
    // as the share partitions might not be initialized yet. So, we retry until we get the response.
    var responses = Seq[ShareFetchResponseData.PartitionData]()
    TestUtils.waitUntilTrue(() => {
      val shareFetchResponse = IntegrationTestUtils.sendAndReceive[ShareFetchResponse](shareFetchRequest, socket)
      val shareFetchResponseData = shareFetchResponse.data()
      assertEquals(Errors.NONE.code, shareFetchResponseData.errorCode)
      assertEquals(30000, shareFetchResponseData.acquisitionLockTimeoutMs)
      assertEquals(1, shareFetchResponseData.responses().size())
      val partitionsCount = shareFetchResponseData.responses().stream().findFirst().get().partitions().size()
      if (partitionsCount > 0) {
        assertEquals(topicId, shareFetchResponseData.responses().stream().findFirst().get().topicId())
        shareFetchResponseData.responses().stream().findFirst().get().partitions().forEach(partitionData => {
          if (!partitionData.acquiredRecords().isEmpty) {
            responses = responses :+ partitionData
          }
        })
      }
      responses.size == 2
    }, "Share fetch request failed", 5000)

    // Producing 10 more records to the topic partitions created above
    produceData(topicIdPartition1, 10)
    produceData(topicIdPartition2, 10)

    // Send another share fetch request with forget list populated with topicIdPartition2
    shareSessionEpoch = ShareRequestMetadata.nextEpoch(shareSessionEpoch)
    metadata = new ShareRequestMetadata(MEMBER_ID, shareSessionEpoch)
    val forget = util.List.of(topicIdPartition1)
    shareFetchRequest = createShareFetchRequest(GROUP_ID, metadata, util.List.of, forget, acknowledgementsMap)
    val shareFetchResponse = IntegrationTestUtils.sendAndReceive[ShareFetchResponse](shareFetchRequest, socket)

    val shareFetchResponseData = shareFetchResponse.data()
    assertEquals(Errors.NONE.code, shareFetchResponseData.errorCode)
    assertEquals(30000, shareFetchResponseData.acquisitionLockTimeoutMs)
    assertEquals(1, shareFetchResponseData.responses().size())
    assertEquals(topicId, shareFetchResponseData.responses().stream().findFirst().get().topicId())
    assertEquals(1, shareFetchResponseData.responses().stream().findFirst().get().partitions().size())

    val expectedPartitionData = new ShareFetchResponseData.PartitionData()
      .setPartitionIndex(partition2)
      .setErrorCode(Errors.NONE.code())
      .setAcknowledgeErrorCode(Errors.NONE.code())
      .setAcquiredRecords(expectedAcquiredRecords(util.List.of(10), util.List.of(19), util.List.of(1)))

    val partitionData = shareFetchResponseData.responses().stream().findFirst().get().partitions().get(0)
    compareFetchResponsePartitions(expectedPartitionData, partitionData)
  }

  @ClusterTests(
    Array(
      new ClusterTest(
        serverProperties = Array(
          new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
          new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1")
        )
      ),
      new ClusterTest(
        serverProperties = Array(
          new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
          new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1"),
          new ClusterConfigProperty(key = "group.share.persister.class.name", value = "org.apache.kafka.server.share.persister.DefaultStatePersister"),
          new ClusterConfigProperty(key = "share.coordinator.state.topic.replication.factor", value = "1"),
          new ClusterConfigProperty(key = "share.coordinator.state.topic.num.partitions", value = "1")
        )
      )
    )
  )
  def testShareFetchRequestWithMaxRecordsAndBatchSize(): Unit = {
    createTopicAndReturnLeaders(TOPIC, numPartitions = 3)
    val topicIds = getTopicIds
    val topicId = topicIds.get(TOPIC)
    val topicIdPartition = new TopicIdPartition(topicId, new TopicPartition(TOPIC, PARTITION))

    val send = util.List.of(topicIdPartition)

    val socket: Socket = connectAny()

    createOffsetsTopic()
    shareHeartbeat(MEMBER_ID, GROUP_ID, util.Map.of[String, Int](TOPIC, 3))

    // Send the first share fetch request to initialize the share partition
    sendFirstShareFetchRequest(MEMBER_ID, GROUP_ID, send, socket)

    initProducer()
    // Producing 10 records to the topic created above
    produceData(topicIdPartition, 10)

    // Send the second share fetch request to fetch the records produced above
    val metadata = new ShareRequestMetadata(MEMBER_ID, ShareRequestMetadata.nextEpoch(ShareRequestMetadata.INITIAL_EPOCH))
    val acknowledgementsMap = util.Map.of[TopicIdPartition, util.List[ShareFetchRequestData.AcknowledgementBatch]]
    val shareFetchRequest = createShareFetchRequest(GROUP_ID, metadata, send, util.List.of, acknowledgementsMap, maxRecords = 1, batchSize = 1)
    val shareFetchResponse = IntegrationTestUtils.sendAndReceive[ShareFetchResponse](shareFetchRequest, socket)

    val shareFetchResponseData = shareFetchResponse.data
    assertEquals(Errors.NONE.code, shareFetchResponseData.errorCode)
    assertEquals(30000, shareFetchResponseData.acquisitionLockTimeoutMs)
    assertEquals(1, shareFetchResponseData.responses.size)
    assertEquals(topicId, shareFetchResponseData.responses.stream().findFirst().get().topicId)
    assertEquals(1, shareFetchResponseData.responses.stream().findFirst().get().partitions.size)

    val expectedPartitionData = new ShareFetchResponseData.PartitionData()
      .setPartitionIndex(PARTITION)
      .setErrorCode(Errors.NONE.code)
      .setAcknowledgeErrorCode(Errors.NONE.code)
      .setAcquiredRecords(expectedAcquiredRecords(util.List.of(0), util.List.of(0), util.List.of(1)))

    val partitionData = shareFetchResponseData.responses.stream().findFirst().get().partitions.get(0)
    compareFetchResponsePartitions(expectedPartitionData, partitionData)
  }

  @ClusterTests(
    Array(
      new ClusterTest(
        serverProperties = Array(
          new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
          new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1")
        )
      ),
      new ClusterTest(
        serverProperties = Array(
          new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
          new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1"),
          new ClusterConfigProperty(key = "group.share.persister.class.name", value = "org.apache.kafka.server.share.persister.DefaultStatePersister"),
          new ClusterConfigProperty(key = "share.coordinator.state.topic.replication.factor", value = "1"),
          new ClusterConfigProperty(key = "share.coordinator.state.topic.num.partitions", value = "1")
        )
      )
    )
  )
  def testShareFetchRequestMultipleBatchesWithMaxRecordsAndBatchSize(): Unit = {
    createTopicAndReturnLeaders(TOPIC, numPartitions = 3)
    val topicIds = getTopicIds
    val topicId = topicIds.get(TOPIC)
    val topicIdPartition = new TopicIdPartition(topicId, new TopicPartition(TOPIC, PARTITION))

    val send = util.List.of(topicIdPartition)

    val socket: Socket = connectAny()

    createOffsetsTopic()
    shareHeartbeat(MEMBER_ID, GROUP_ID, util.Map.of[String, Int](TOPIC, 3))

    // Send the first share fetch request to initialize the share partition
    sendFirstShareFetchRequest(MEMBER_ID, GROUP_ID, send, socket)

    initProducer()
    // Producing 10 records to the topic created above
    produceData(topicIdPartition, 10)

    // Send the second share fetch request to fetch the records produced above
    val metadata = new ShareRequestMetadata(MEMBER_ID, ShareRequestMetadata.nextEpoch(ShareRequestMetadata.INITIAL_EPOCH))
    val acknowledgementsMap = util.Map.of[TopicIdPartition, util.List[ShareFetchRequestData.AcknowledgementBatch]]
    val shareFetchRequest = createShareFetchRequest(GROUP_ID, metadata, send, util.List.of, acknowledgementsMap, maxRecords = 5, batchSize = 1)
    val shareFetchResponse = IntegrationTestUtils.sendAndReceive[ShareFetchResponse](shareFetchRequest, socket)

    val shareFetchResponseData = shareFetchResponse.data
    assertEquals(Errors.NONE.code, shareFetchResponseData.errorCode)
    assertEquals(30000, shareFetchResponseData.acquisitionLockTimeoutMs)
    assertEquals(1, shareFetchResponseData.responses.size)
    assertEquals(topicId, shareFetchResponseData.responses.stream().findFirst().get().topicId)
    assertEquals(1, shareFetchResponseData.responses.stream().findFirst().get().partitions.size)

    val expectedPartitionData = new ShareFetchResponseData.PartitionData()
      .setPartitionIndex(PARTITION)
      .setErrorCode(Errors.NONE.code)
      .setAcknowledgeErrorCode(Errors.NONE.code)
      .setAcquiredRecords(expectedAcquiredRecords(util.List.of(0, 1, 2, 3, 4), util.List.of(0, 1, 2, 3, 4), util.List.of(1, 1, 1, 1, 1)))

    val partitionData = shareFetchResponseData.responses.stream().findFirst().get().partitions.get(0)
    compareFetchResponsePartitions(expectedPartitionData, partitionData)
  }

  // For initial fetch request, the response may not be available in the first attempt when the share
  // partition is not initialized yet. Hence, wait for response from all partitions before proceeding.
  private def sendFirstShareFetchRequest(memberId: Uuid, groupId: String, topicIdPartitions: util.List[TopicIdPartition], socket: Socket, lockTimeout: Int = 30000): Unit = {
    val partitions: util.Set[Integer] = new util.HashSet()
    TestUtils.waitUntilTrue(() => {
      val metadata = new ShareRequestMetadata(memberId, ShareRequestMetadata.INITIAL_EPOCH)
      val shareFetchRequest = createShareFetchRequest(groupId, metadata, topicIdPartitions, util.List.of, util.Map.of)
      val shareFetchResponse = IntegrationTestUtils.sendAndReceive[ShareFetchResponse](shareFetchRequest, socket)
      val shareFetchResponseData = shareFetchResponse.data()

      assertEquals(Errors.NONE.code, shareFetchResponseData.errorCode)
      assertEquals(lockTimeout, shareFetchResponseData.acquisitionLockTimeoutMs)
      shareFetchResponseData.responses().forEach(response => {
        if (!response.partitions().isEmpty) {
          response.partitions().forEach(partitionData => partitions.add(partitionData.partitionIndex))
        }
      })

      partitions.size() == topicIdPartitions.size
    }, "Share fetch request failed", 5000)
  }

  private def shareHeartbeat(memberId: Uuid, groupId: String, topics: util.Map[String, Int]): Unit = {
    val coordResp = connectAndReceive[FindCoordinatorResponse](new FindCoordinatorRequest.Builder(new FindCoordinatorRequestData()
      .setKey(groupId)
      .setKeyType(0.toByte)
    ).build(0)
    )

    val shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequest.Builder(
      new ShareGroupHeartbeatRequestData()
        .setMemberId(memberId.toString)
        .setGroupId(groupId)
        .setMemberEpoch(0)
        .setSubscribedTopicNames(new java.util.ArrayList[String](topics.keySet()))
    ).build()

    TestUtils.waitUntilTrue(() => {
      val resp = connectAndReceive[ShareGroupHeartbeatResponse](shareGroupHeartbeatRequest, coordResp.node().id())
      resp.data().errorCode() == Errors.NONE.code() && assignment(memberId.toString, groupId)
    }, "Heartbeat failed")
  }

  private def assignment(memberId: String, groupId: String): Boolean = {
    val admin = cluster.admin()

    val members = admin
      .describeShareGroups(util.List.of(groupId), new DescribeShareGroupsOptions().includeAuthorizedOperations(true))
      .describedGroups()
      .get(groupId)
      .get()
      .members()

    var isAssigned = false
    val iter = members.iterator()
    while (iter.hasNext && !isAssigned) {
      val desc = iter.next()
      if (desc.consumerId() == memberId && !desc.assignment().topicPartitions().isEmpty)
        isAssigned = true
    }

    admin.close()
    isAssigned
  }

  private def expectedAcquiredRecords(firstOffsets: util.List[Long], lastOffsets: util.List[Long], deliveryCounts: util.List[Int]): util.List[AcquiredRecords] = {
    val acquiredRecordsList: util.List[AcquiredRecords] = new util.ArrayList()
    for (i <- 0 until firstOffsets.size()) {
      acquiredRecordsList.add(new AcquiredRecords()
        .setFirstOffset(firstOffsets.get(i))
        .setLastOffset(lastOffsets.get(i))
        .setDeliveryCount(deliveryCounts.get(i).toShort))
    }
    acquiredRecordsList
  }

  private def compareFetchResponsePartitions(expectedResponse: ShareFetchResponseData.PartitionData,
                                             actualResponse: ShareFetchResponseData.PartitionData): Unit = {
    assertEquals(expectedResponse.partitionIndex, actualResponse.partitionIndex)
    assertEquals(expectedResponse.errorCode, actualResponse.errorCode)
    assertEquals(expectedResponse.errorMessage, actualResponse.errorMessage)
    assertEquals(expectedResponse.acknowledgeErrorCode, actualResponse.acknowledgeErrorCode)
    assertEquals(expectedResponse.acquiredRecords, actualResponse.acquiredRecords)
  }

  private def compareAcknowledgeResponsePartitions(expectedResponse: ShareAcknowledgeResponseData.PartitionData,
                                                   actualResponse: ShareAcknowledgeResponseData.PartitionData): Unit = {
    assertEquals(expectedResponse.partitionIndex, actualResponse.partitionIndex)
    assertEquals(expectedResponse.errorCode, actualResponse.errorCode)
  }

  private def createShareFetchRequest(groupId: String,
                                      metadata: ShareRequestMetadata,
                                      send: util.List[TopicIdPartition],
                                      forget: util.List[TopicIdPartition],
                                      acknowledgementsMap: util.Map[TopicIdPartition, util.List[ShareFetchRequestData.AcknowledgementBatch]],
                                      maxWaitMs: Int = MAX_WAIT_MS,
                                      minBytes: Int = 0,
                                      maxBytes: Int = Int.MaxValue,
                                      maxRecords: Int = 500,
                                      batchSize: Int = 500,
                                      shareAcquireMode: ShareAcquireMode = ShareAcquireMode.BATCH_OPTIMIZED,
                                      isRenewAck: Boolean = false): ShareFetchRequest = {
    ShareFetchRequest.Builder.forConsumer(groupId, metadata, maxWaitMs, minBytes, maxBytes, maxRecords, batchSize, shareAcquireMode.id, isRenewAck, send, forget, acknowledgementsMap)
      .build()
  }

  private def createShareAcknowledgeRequest(groupId: String,
                                            metadata: ShareRequestMetadata,
                                            acknowledgementsMap: util.Map[TopicIdPartition, util.List[ShareAcknowledgeRequestData.AcknowledgementBatch]],
                                            isRenewAck: Boolean = false): ShareAcknowledgeRequest = {
    ShareAcknowledgeRequest.Builder.forConsumer(groupId, metadata, isRenewAck, acknowledgementsMap)
      .build()
  }
}
