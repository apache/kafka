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

package kafka.network

import java.util

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.{ArrayNode, BinaryNode, DoubleNode, IntNode, JsonNodeFactory, LongNode, NullNode, ObjectNode, ShortNode, TextNode}
import kafka.network.RequestChannel.{Response, Session}
import org.apache.kafka.common.message._
import org.apache.kafka.common.network.ClientInformation
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.requests._
import org.apache.kafka.common.utils.CollectionUtils

import scala.jdk.CollectionConverters._

object RequestConvertToJson {
  def request(request: AbstractRequest, verbose: Boolean): JsonNode = {
    request match {
      case req: AddOffsetsToTxnRequest => AddOffsetsToTxnRequestDataJsonConverter.write(req.data, request.version, verbose)
      case req: AddPartitionsToTxnRequest => AddPartitionsToTxnRequestDataJsonConverter.write(req.data, request.version, verbose)
      case req: AlterClientQuotasRequest => AlterClientQuotasRequestDataJsonConverter.write(req.data, request.version, verbose)
      case req: AlterConfigsRequest => AlterConfigsRequestDataJsonConverter.write(req.data, request.version, verbose)
      case req: AlterIsrRequest => AlterIsrRequestDataJsonConverter.write(req.data, request.version, verbose)
      case req: AlterPartitionReassignmentsRequest => AlterPartitionReassignmentsRequestDataJsonConverter.write(req.data, request.version, verbose)
      case req: AlterReplicaLogDirsRequest => AlterReplicaLogDirsRequestDataJsonConverter.write(req.data, request.version, verbose)
      case res: AlterUserScramCredentialsRequest => AlterUserScramCredentialsRequestDataJsonConverter.write(res.data, request.version, verbose)
      case req: ApiVersionsRequest => ApiVersionsRequestDataJsonConverter.write(req.data, request.version, verbose)
      case req: BeginQuorumEpochRequest => BeginQuorumEpochRequestDataJsonConverter.write(req.data, request.version, verbose)
      case req: ControlledShutdownRequest => ControlledShutdownRequestDataJsonConverter.write(req.data, request.version, verbose)
      case req: CreateAclsRequest => CreateAclsRequestDataJsonConverter.write(req.data, request.version, verbose)
      case req: CreateDelegationTokenRequest => CreateDelegationTokenRequestDataJsonConverter.write(req.data, request.version, verbose)
      case req: CreatePartitionsRequest => CreatePartitionsRequestDataJsonConverter.write(req.data, request.version, verbose)
      case req: CreateTopicsRequest => CreateTopicsRequestDataJsonConverter.write(req.data, request.version, verbose)
      case req: DeleteAclsRequest => DeleteAclsRequestDataJsonConverter.write(req.data, request.version, verbose)
      case req: DeleteGroupsRequest => DeleteGroupsRequestDataJsonConverter.write(req.data, request.version, verbose)
      case req: DeleteRecordsRequest => DeleteRecordsRequestDataJsonConverter.write(req.data, request.version, verbose)
      case req: DeleteTopicsRequest => DeleteTopicsRequestDataJsonConverter.write(req.data, request.version, verbose)
      case req: DescribeAclsRequest => DescribeAclsRequestDataJsonConverter.write(req.data, request.version, verbose)
      case req: DescribeClientQuotasRequest => DescribeClientQuotasRequestDataJsonConverter.write(req.data, request.version, verbose)
      case req: DescribeConfigsRequest => DescribeConfigsRequestDataJsonConverter.write(req.data, request.version, verbose)
      case req: DescribeDelegationTokenRequest => DescribeDelegationTokenRequestDataJsonConverter.write(req.data, request.version, verbose)
      case req: DescribeGroupsRequest => DescribeGroupsRequestDataJsonConverter.write(req.data, request.version, verbose)
      case req: DescribeLogDirsRequest => DescribeLogDirsRequestDataJsonConverter.write(req.data, request.version, verbose)
      case req: DescribeQuorumRequest => DescribeQuorumRequestDataJsonConverter.write(req.data, request.version, verbose)
      case res: DescribeUserScramCredentialsRequest => DescribeUserScramCredentialsRequestDataJsonConverter.write(res.data, request.version, verbose)
      case req: ElectLeadersRequest => ElectLeadersRequestDataJsonConverter.write(req.data, request.version, verbose)
      case req: EndTxnRequest => EndTxnRequestDataJsonConverter.write(req.data, request.version, verbose)
      case req: EndQuorumEpochRequest => EndQuorumEpochRequestDataJsonConverter.write(req.data, request.version, verbose)
      case req: ExpireDelegationTokenRequest => ExpireDelegationTokenRequestDataJsonConverter.write(req.data, request.version, verbose)
      case req: FetchRequest => FetchRequestDataJsonConverter.write(req.data, request.version, verbose)
      case req: FindCoordinatorRequest => FindCoordinatorRequestDataJsonConverter.write(req.data, request.version, verbose)
      case req: HeartbeatRequest => HeartbeatRequestDataJsonConverter.write(req.data, request.version, verbose)
      case req: IncrementalAlterConfigsRequest => IncrementalAlterConfigsRequestDataJsonConverter.write(req.data, request.version, verbose)
      case req: InitProducerIdRequest => InitProducerIdRequestDataJsonConverter.write(req.data, request.version, verbose)
      case req: JoinGroupRequest => JoinGroupRequestDataJsonConverter.write(req.data, request.version, verbose)
      case req: LeaderAndIsrRequest => LeaderAndIsrRequestDataJsonConverter.write(req.data, request.version, verbose)
      case req: LeaveGroupRequest => LeaveGroupRequestDataJsonConverter.write(req.data, request.version, verbose)
      case req: ListGroupsRequest => ListGroupsRequestDataJsonConverter.write(req.data, request.version, verbose)
      case req: ListOffsetRequest => ListOffsetRequestDataJsonConverter.write(req.data, request.version, verbose)
      case req: ListPartitionReassignmentsRequest => ListPartitionReassignmentsRequestDataJsonConverter.write(req.data, request.version, verbose)
      case req: MetadataRequest => MetadataRequestDataJsonConverter.write(req.data, request.version, verbose)
      case req: OffsetCommitRequest => OffsetCommitRequestDataJsonConverter.write(req.data, request.version, verbose)
      case req: OffsetDeleteRequest => OffsetDeleteRequestDataJsonConverter.write(req.data, request.version, verbose)
      case req: OffsetFetchRequest => OffsetFetchRequestDataJsonConverter.write(req.data, request.version, verbose)
      case req: OffsetsForLeaderEpochRequest => offsetsForLeaderEpochRequestNode(req, request.version, verbose)
      case req: ProduceRequest => produceRequestNode(req, request.version, verbose)
      case req: RenewDelegationTokenRequest => RenewDelegationTokenRequestDataJsonConverter.write(req.data, request.version, verbose)
      case req: SaslAuthenticateRequest => SaslAuthenticateRequestDataJsonConverter.write(req.data, request.version, verbose)
      case req: SaslHandshakeRequest => SaslHandshakeRequestDataJsonConverter.write(req.data, request.version, verbose)
      case req: StopReplicaRequest => StopReplicaRequestDataJsonConverter.write(req.data, request.version, verbose)
      case req: SyncGroupRequest => SyncGroupRequestDataJsonConverter.write(req.data, request.version, verbose)
      case req: TxnOffsetCommitRequest => TxnOffsetCommitRequestDataJsonConverter.write(req.data, request.version, verbose)
      case req: UpdateFeaturesRequest => UpdateFeaturesRequestDataJsonConverter.write(req.data, request.version, verbose)
      case req: UpdateMetadataRequest => UpdateMetadataRequestDataJsonConverter.write(req.data, request.version, verbose)
      case req: VoteRequest => VoteRequestDataJsonConverter.write(req.data, request.version, verbose)
      case req: WriteTxnMarkersRequest => WriteTxnMarkersRequestDataJsonConverter.write(req.data, request.version, verbose)
      case _ => throw new IllegalStateException(s"ApiKey ${request.api} is not currently handled in `request`, the " +
        "code should be updated to do so.");
    }
  }

  def response(response: AbstractResponse, version: Short, verbose: Boolean): JsonNode = {
    response match {
      case res: AddOffsetsToTxnResponse => AddOffsetsToTxnResponseDataJsonConverter.write(res.data, version, verbose)
      case res: AddPartitionsToTxnResponse => AddPartitionsToTxnResponseDataJsonConverter.write(res.data, version, verbose)
      case res: AlterClientQuotasResponse => AlterClientQuotasResponseDataJsonConverter.write(res.data, version, verbose)
      case res: AlterConfigsResponse => AlterConfigsResponseDataJsonConverter.write(res.data, version, verbose)
      case res: AlterIsrResponse => AlterIsrResponseDataJsonConverter.write(res.data, version, verbose)
      case res: AlterPartitionReassignmentsResponse => AlterPartitionReassignmentsResponseDataJsonConverter.write(res.data, version, verbose)
      case res: AlterReplicaLogDirsResponse => AlterReplicaLogDirsResponseDataJsonConverter.write(res.data, version, verbose)
      case res: AlterUserScramCredentialsResponse => AlterUserScramCredentialsResponseDataJsonConverter.write(res.data, version, verbose)
      case res: ApiVersionsResponse => ApiVersionsResponseDataJsonConverter.write(res.data, version, verbose)
      case res: BeginQuorumEpochResponse => BeginQuorumEpochResponseDataJsonConverter.write(res.data, version, verbose)
      case res: ControlledShutdownResponse => ControlledShutdownResponseDataJsonConverter.write(res.data, version, verbose)
      case res: CreateAclsResponse => CreateAclsResponseDataJsonConverter.write(res.data, version, verbose)
      case res: CreateDelegationTokenResponse => CreateDelegationTokenResponseDataJsonConverter.write(res.data, version, verbose)
      case res: CreatePartitionsResponse => CreatePartitionsResponseDataJsonConverter.write(res.data, version, verbose)
      case res: CreateTopicsResponse => CreateTopicsResponseDataJsonConverter.write(res.data, version, verbose)
      case res: DeleteAclsResponse => DeleteAclsResponseDataJsonConverter.write(res.data, version, verbose)
      case res: DeleteGroupsResponse => DeleteGroupsResponseDataJsonConverter.write(res.data, version, verbose)
      case res: DeleteRecordsResponse => DeleteRecordsResponseDataJsonConverter.write(res.data, version, verbose)
      case res: DeleteTopicsResponse => DeleteTopicsResponseDataJsonConverter.write(res.data, version, verbose)
      case res: DescribeAclsResponse => DescribeAclsResponseDataJsonConverter.write(res.data, version, verbose)
      case res: DescribeClientQuotasResponse => DescribeClientQuotasResponseDataJsonConverter.write(res.data, version, verbose)
      case res: DescribeConfigsResponse => DescribeConfigsResponseDataJsonConverter.write(res.data, version, verbose)
      case res: DescribeDelegationTokenResponse => DescribeDelegationTokenResponseDataJsonConverter.write(res.data, version, verbose)
      case res: DescribeGroupsResponse => DescribeGroupsResponseDataJsonConverter.write(res.data, version, verbose)
      case res: DescribeLogDirsResponse => DescribeLogDirsResponseDataJsonConverter.write(res.data, version, verbose)
      case res: DescribeQuorumResponse => DescribeQuorumResponseDataJsonConverter.write(res.data, version, verbose)
      case res: DescribeUserScramCredentialsResponse => DescribeUserScramCredentialsResponseDataJsonConverter.write(res.data, version, verbose)
      case res: ElectLeadersResponse => ElectLeadersResponseDataJsonConverter.write(res.data, version, verbose)
      case res: EndTxnResponse => EndTxnResponseDataJsonConverter.write(res.data, version, verbose)
      case res: EndQuorumEpochResponse => EndQuorumEpochResponseDataJsonConverter.write(res.data, version, verbose)
      case res: ExpireDelegationTokenResponse => ExpireDelegationTokenResponseDataJsonConverter.write(res.data, version, verbose)
      case res: FetchResponse[_] => FetchResponseDataJsonConverter.write(res.data, version, verbose)
      case res: FindCoordinatorResponse => FindCoordinatorResponseDataJsonConverter.write(res.data, version, verbose)
      case res: HeartbeatResponse => HeartbeatResponseDataJsonConverter.write(res.data, version, verbose)
      case res: IncrementalAlterConfigsResponse => IncrementalAlterConfigsResponseDataJsonConverter.write(res.data, version, verbose)
      case res: InitProducerIdResponse => InitProducerIdResponseDataJsonConverter.write(res.data, version, verbose)
      case res: JoinGroupResponse => JoinGroupResponseDataJsonConverter.write(res.data, version, verbose)
      case res: LeaderAndIsrResponse => LeaderAndIsrResponseDataJsonConverter.write(res.data, version, verbose)
      case res: LeaveGroupResponse => LeaveGroupResponseDataJsonConverter.write(res.data, version, verbose)
      case res: ListGroupsResponse => ListGroupsResponseDataJsonConverter.write(res.data, version, verbose)
      case res: ListOffsetResponse => ListOffsetResponseDataJsonConverter.write(res.data, version, verbose)
      case res: ListPartitionReassignmentsResponse => ListPartitionReassignmentsResponseDataJsonConverter.write(res.data, version, verbose)
      case res: MetadataResponse => MetadataResponseDataJsonConverter.write(res.data, version, verbose)
      case res: OffsetCommitResponse => OffsetCommitResponseDataJsonConverter.write(res.data, version, verbose)
      case res: OffsetDeleteResponse => OffsetDeleteResponseDataJsonConverter.write(res.data, version, verbose)
      case res: OffsetFetchResponse => OffsetFetchResponseDataJsonConverter.write(res.data, version, verbose)
      case res: OffsetsForLeaderEpochResponse => offsetsForLeaderEpochResponseNode(res, version, verbose)
      case res: ProduceResponse => produceResponseNode(res, version, verbose)
      case res: RenewDelegationTokenResponse => RenewDelegationTokenResponseDataJsonConverter.write(res.data, version, verbose)
      case res: SaslAuthenticateResponse => SaslAuthenticateResponseDataJsonConverter.write(res.data, version, verbose)
      case res: SaslHandshakeResponse => SaslHandshakeResponseDataJsonConverter.write(res.data, version, verbose)
      case res: StopReplicaResponse => StopReplicaResponseDataJsonConverter.write(res.data, version, verbose)
      case res: SyncGroupResponse => SyncGroupResponseDataJsonConverter.write(res.data, version, verbose)
      case res: TxnOffsetCommitResponse => TxnOffsetCommitResponseDataJsonConverter.write(res.data, version, verbose)
      case res: UpdateFeaturesResponse => UpdateFeaturesResponseDataJsonConverter.write(res.data, version, verbose)
      case res: UpdateMetadataResponse => UpdateMetadataResponseDataJsonConverter.write(res.data, version, verbose)
      case res: WriteTxnMarkersResponse => WriteTxnMarkersResponseDataJsonConverter.write(res.data, version, verbose)
      case res: VoteResponse => VoteResponseDataJsonConverter.write(res.data, version, verbose)
      case _ => throw new IllegalStateException(s"ApiKey $response is not currently handled in `response`, the " +
        "code should be updated to do so.");
    }
  }

  def requestHeaderNode(header: RequestHeader, verbose: Boolean): JsonNode = {
    val node = RequestHeaderDataJsonConverter.write(header.data(), header.headerVersion(), verbose).asInstanceOf[ObjectNode]
    node.set("requestApiKeyName", new TextNode(header.apiKey.toString))
    node
  }

  def clientInfoNode(clientInfo: ClientInformation): JsonNode = {
    val node = new ObjectNode(JsonNodeFactory.instance)
    node.set("softwareName", new TextNode(clientInfo.softwareName()))
    node.set("softwareVersion", new TextNode(clientInfo.softwareVersion()))
    node
  }

  def requestDescMetrics(header: RequestHeader, res: Response, req: AbstractRequest,
                         context: RequestContext, session: Session, verbose: Boolean,
                         totalTimeMs: Double, requestQueueTimeMs: Double, apiLocalTimeMs: Double,
                         apiRemoteTimeMs: Double, apiThrottleTimeMs: Long, responseQueueTimeMs: Double,
                         responseSendTimeMs: Double, temporaryMemoryBytes: Long,
                         messageConversionsTimeMs: Double): JsonNode = {
    val node = new ObjectNode(JsonNodeFactory.instance)
    node.set("requestHeader", requestHeaderNode(header, verbose))
    node.set("request", request(req, verbose))
    node.set("response", res.responseLog.getOrElse(new TextNode("")))
    node.set("connection", new TextNode(context.connectionId))
    node.set("totalTimeMs", new DoubleNode(totalTimeMs))
    node.set("requestQueueTimeMs", new DoubleNode(requestQueueTimeMs))
    node.set("localTimeMs", new DoubleNode(apiLocalTimeMs))
    node.set("remoteTimeMs", new DoubleNode(apiRemoteTimeMs))
    node.set("throttleTimeMs", new LongNode(apiThrottleTimeMs))
    node.set("responseQueueTimeMs", new DoubleNode(responseQueueTimeMs))
    node.set("sendTimeMs", new DoubleNode(responseSendTimeMs))
    node.set("securityProtocol", new TextNode(context.securityProtocol.toString))
    node.set("principal", new TextNode(session.principal.toString))
    node.set("listener", new TextNode(context.listenerName.value))
    node.set("clientInformation", clientInfoNode(context.clientInformation))
    if (temporaryMemoryBytes > 0)
      node.set("temporaryMemoryBytes", new LongNode(temporaryMemoryBytes))
    if (messageConversionsTimeMs > 0)
      node.set("messageConversionsTime", new DoubleNode(messageConversionsTimeMs))
    node
  }

  def requestDesc(header: RequestHeader, req: AbstractRequest, verbose: Boolean): JsonNode = {
    val node = new ObjectNode(JsonNodeFactory.instance)
    node.set("requestHeader", requestHeaderNode(header, verbose))
    node.set("request", request(req, verbose))
    node
  }

  /**
   * Temporary until switch to use the generated schemas.
   */
  def offsetsForLeaderEpochRequestNode(request: OffsetsForLeaderEpochRequest, version: Short, verbose: Boolean): JsonNode = {
    val node = new ObjectNode(JsonNodeFactory.instance)
    if (version >= 3) {
      node.set("replicaId", new IntNode(request.replicaId))
    }
    val topics = CollectionUtils.groupPartitionDataByTopic(request.epochsByTopicPartition)
    val topicsArray = new ArrayNode(JsonNodeFactory.instance)
    topics.forEach { (topicName, partitions) =>
      val topicNode = new ObjectNode(JsonNodeFactory.instance)
      topicNode.set("name", new TextNode(topicName))
      val partitionsArray = new ArrayNode(JsonNodeFactory.instance)
      partitions.forEach { (partitionIndex, partitionData) =>
        val partitionNode = new ObjectNode(JsonNodeFactory.instance)
        partitionNode.set("partitionIndex", new IntNode(partitionIndex))
        if (version >= 2) {
          val leaderEpoch = partitionData.currentLeaderEpoch
          partitionNode.set("currentLeaderEpoch", new IntNode(leaderEpoch.orElse(RecordBatch.NO_PARTITION_LEADER_EPOCH)))
        }
        partitionNode.set("leaderEpoch", new IntNode(partitionData.leaderEpoch))
        partitionsArray.add(partitionNode)
      }
      topicNode.set("partitions", partitionsArray)
      topicsArray.add(topicNode)
    }
    node.set("topics", topicsArray)
    node
  }

  /**
   * Temporary until switch to use the generated schemas.
   */
  def produceRequestNode(request: ProduceRequest, version: Short, verbose: Boolean): JsonNode = {
    val node = new ObjectNode(JsonNodeFactory.instance)
    if (version >= 3) {
      if (request.transactionalId == null) {
        node.set("transactionalId", NullNode.instance)
      } else {
        node.set("transactionalId", new TextNode(request.transactionalId))
      }
    }
    node.set("acks", new ShortNode(request.acks))
    node.set("timeoutMs", new IntNode(request.timeout))
    val topics = CollectionUtils.groupPartitionDataByTopic(request.partitionRecordsOrFail())
    val topicsArray = new ArrayNode(JsonNodeFactory.instance)
    topics.forEach { (topicName, partitions) =>
      val topicNode = new ObjectNode(JsonNodeFactory.instance)
      topicNode.set("name", new TextNode(topicName))
      val partitionsArray = new ArrayNode(JsonNodeFactory.instance)
      partitions.forEach { (partitionIndex, partitionData) =>
        val partitionNode = new ObjectNode(JsonNodeFactory.instance)
        partitionNode.set("partitionIndex", new IntNode(partitionIndex))
        if (partitionData == null)
          partitionNode.set("records", NullNode.instance)
        else
          partitionNode.set("records", new BinaryNode(util.Arrays.copyOf(partitionData.buffer().array(), partitionData.validBytes())))
        partitionsArray.add(partitionNode)
      }
      topicNode.set("partitions", partitionsArray)
      topicsArray.add(topicNode)
    }
    node.set("topics", topicsArray)
    node
  }

  /**
   * Temporary until switch to use the generated schemas.
   */
  def offsetsForLeaderEpochResponseNode(response: OffsetsForLeaderEpochResponse, version: Short, verbose: Boolean): JsonNode = {
    val node = new ObjectNode(JsonNodeFactory.instance)
    if (version >= 2) {
      node.set("throttleTimeMs", new IntNode(response.throttleTimeMs))
    }
    val topics = CollectionUtils.groupPartitionDataByTopic(response.responses)
    val topicsArray = new ArrayNode(JsonNodeFactory.instance)
    topics.forEach { (topicName, partitions) =>
      val topicNode = new ObjectNode(JsonNodeFactory.instance)
      topicNode.set("name", new TextNode(topicName))
      val partitionsArray = new ArrayNode(JsonNodeFactory.instance)
      partitions.forEach { (partitionIndex, partitionData) =>
        val partitionNode = new ObjectNode(JsonNodeFactory.instance)
        partitionNode.set("errorCode", new ShortNode(partitionData.error.code))
        partitionNode.set("partitionIndex", new IntNode(partitionIndex))
        if (version >= 1) partitionNode.set("leaderEpoch", new IntNode(partitionData.leaderEpoch))
        partitionNode.set("endOffset", new LongNode(partitionData.endOffset))
        partitionsArray.add(partitionNode)
      }
      topicNode.set("partitions", partitionsArray)
      topicsArray.add(topicNode)
    }
    node.set("topics", topicsArray)
    node
  }

  /**
   * Temporary until switch to use the generated schemas.
   */
  def produceResponseNode(response: ProduceResponse, version: Short, verbose: Boolean): JsonNode = {
    val node = new ObjectNode(JsonNodeFactory.instance)
    val topics = CollectionUtils.groupPartitionDataByTopic(response.responses)
    val responsesArray = new ArrayNode(JsonNodeFactory.instance)
    topics.forEach { (topicName, partitions) =>
      val topicNode = new ObjectNode(JsonNodeFactory.instance)
      topicNode.set("name", new TextNode(topicName))
      val partitionsArray = new ArrayNode(JsonNodeFactory.instance)
      partitions.forEach { (partitionIndex, partitionData) =>
        val partitionNode = new ObjectNode(JsonNodeFactory.instance)
        partitionNode.set("partitionIndex", new IntNode(partitionIndex))
        var errorCode = partitionData.error.code
        if (errorCode == Errors.KAFKA_STORAGE_ERROR.code && version <= 3) {
          errorCode = Errors.NOT_LEADER_OR_FOLLOWER.code
        }
        partitionNode.set("errorCode", new ShortNode(errorCode))
        partitionNode.set("baseOffset", new LongNode(partitionData.baseOffset))
        if (version >= 2) {
          partitionNode.set("logAppendTimeMs", new LongNode(partitionData.logAppendTime))
        }
        if (version >= 5) {
          partitionNode.set("logStartOffset", new LongNode(partitionData.logStartOffset))
        }
        if (version >= 8) {
          val recordErrorsArray = new ArrayNode(JsonNodeFactory.instance)
          for (indexAndMessage <- partitionData.recordErrors.asScala) {
            val indexAndMessageData = new ObjectNode(JsonNodeFactory.instance)
            indexAndMessageData.set("batchIndex", new IntNode(indexAndMessage.batchIndex))
            if (indexAndMessage.message == null) indexAndMessageData.set("batchIndexErrorMessage", NullNode.instance)
            else indexAndMessageData.set("batchIndexErrorMessage", new TextNode(indexAndMessage.message))
            recordErrorsArray.add(indexAndMessageData)
          }
          partitionNode.set("recordErrors", recordErrorsArray)
          if (partitionData.errorMessage == null) partitionNode.set("errorMessage", NullNode.instance)
          else partitionNode.set("errorMessage", new TextNode(partitionData.errorMessage))
        }
        partitionsArray.add(partitionNode)
      }
      topicNode.set("partitions", partitionsArray)
      responsesArray.add(topicNode)
    }
    node.set("responses", responsesArray)
    if (version >= 1) {
      node.set("throttleTimeMs", new IntNode(response.throttleTimeMs))
    }
    node
  }
}
