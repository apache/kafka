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

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.{ArrayNode, DoubleNode, IntNode, JsonNodeFactory, LongNode, NullNode, ObjectNode, ShortNode, TextNode}
import kafka.network.RequestChannel.{Response, Session}
import org.apache.kafka.common.message._
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.requests._
import org.apache.kafka.common.utils.CollectionUtils

import scala.jdk.CollectionConverters._

object RequestConvertToJson {
  def request(request: AbstractRequest, verbose: Boolean): JsonNode = {
    request match {
      case req: AddOffsetsToTxnRequest => AddOffsetsToTxnRequestDataJsonConverter.write(req.data(), request.version())
      case req: AddPartitionsToTxnRequest => AddPartitionsToTxnRequestDataJsonConverter.write(req.data(), request.version())
      case req: AlterClientQuotasRequest => AlterClientQuotasRequestDataJsonConverter.write(req.data(), request.version())
      case req: AlterConfigsRequest => AlterConfigsRequestDataJsonConverter.write(req.data(), request.version())
      case req: AlterIsrRequest => AlterIsrRequestDataJsonConverter.write(req.data(), request.version())
      case req: AlterPartitionReassignmentsRequest => AlterPartitionReassignmentsRequestDataJsonConverter.write(req.data(), request.version())
      case req: AlterReplicaLogDirsRequest => AlterReplicaLogDirsRequestDataJsonConverter.write(req.data(), request.version())
      case res: AlterUserScramCredentialsRequest => AlterUserScramCredentialsRequestDataJsonConverter.write(res.data(), request.version())
      case req: ApiVersionsRequest => ApiVersionsRequestDataJsonConverter.write(req.data(), request.version())
      case req: BeginQuorumEpochRequest => BeginQuorumEpochRequestDataJsonConverter.write(req.data, request.version())
      case req: ControlledShutdownRequest => ControlledShutdownRequestDataJsonConverter.write(req.data(), request.version())
      case req: CreateAclsRequest => CreateAclsRequestDataJsonConverter.write(req.data(), request.version())
      case req: CreateDelegationTokenRequest => CreateDelegationTokenRequestDataJsonConverter.write(req.data(), request.version())
      case req: CreatePartitionsRequest => CreatePartitionsRequestDataJsonConverter.write(req.data(), request.version())
      case req: CreateTopicsRequest => CreateTopicsRequestDataJsonConverter.write(req.data(), request.version())
      case req: DeleteAclsRequest => DeleteAclsRequestDataJsonConverter.write(req.data(), request.version())
      case req: DeleteGroupsRequest => DeleteGroupsRequestDataJsonConverter.write(req.data(), request.version())
      case req: DeleteRecordsRequest => DeleteRecordsRequestDataJsonConverter.write(req.data(), request.version())
      case req: DeleteTopicsRequest => DeleteTopicsRequestDataJsonConverter.write(req.data(), request.version())
      case req: DescribeAclsRequest => DescribeAclsRequestDataJsonConverter.write(req.data(), request.version())
      case req: DescribeClientQuotasRequest => DescribeClientQuotasRequestDataJsonConverter.write(req.data(), request.version())
      case req: DescribeConfigsRequest => DescribeConfigsRequestDataJsonConverter.write(req.data(), request.version())
      case req: DescribeDelegationTokenRequest => DescribeDelegationTokenRequestDataJsonConverter.write(req.data(), request.version())
      case req: DescribeGroupsRequest => DescribeGroupsRequestDataJsonConverter.write(req.data(), request.version())
      case req: DescribeLogDirsRequest => DescribeLogDirsRequestDataJsonConverter.write(req.data(), request.version())
      case req: DescribeQuorumRequest => DescribeQuorumRequestDataJsonConverter.write(req.data, request.version())
      case res: DescribeUserScramCredentialsRequest => DescribeUserScramCredentialsRequestDataJsonConverter.write(res.data(), request.version())
      case req: ElectLeadersRequest => ElectLeadersRequestDataJsonConverter.write(req.data(), request.version())
      case req: EndTxnRequest => EndTxnRequestDataJsonConverter.write(req.data(), request.version())
      case req: EndQuorumEpochRequest => EndQuorumEpochRequestDataJsonConverter.write(req.data, request.version())
      case req: ExpireDelegationTokenRequest => ExpireDelegationTokenRequestDataJsonConverter.write(req.data(), request.version())
      case req: FetchRequest => FetchRequestDataJsonConverter.write(req.data(), request.version())
      case req: FindCoordinatorRequest => FindCoordinatorRequestDataJsonConverter.write(req.data(), request.version())
      case req: HeartbeatRequest => HeartbeatRequestDataJsonConverter.write(req.data(), request.version())
      case req: IncrementalAlterConfigsRequest => IncrementalAlterConfigsRequestDataJsonConverter.write(req.data(), request.version())
      case req: InitProducerIdRequest => InitProducerIdRequestDataJsonConverter.write(req.data(), request.version())
      case req: JoinGroupRequest => JoinGroupRequestDataJsonConverter.write(req.data(), request.version())
      case req: LeaderAndIsrRequest => LeaderAndIsrRequestDataJsonConverter.write(req.data(), request.version())
      case req: LeaveGroupRequest => LeaveGroupRequestDataJsonConverter.write(req.data(), request.version())
      case req: ListGroupsRequest => ListGroupsRequestDataJsonConverter.write(req.data(), request.version())
      case req: ListOffsetRequest => ListOffsetRequestDataJsonConverter.write(req.data(), request.version())
      case req: ListPartitionReassignmentsRequest => ListPartitionReassignmentsRequestDataJsonConverter.write(req.data(), request.version())
      case req: MetadataRequest => MetadataRequestDataJsonConverter.write(req.data(), request.version())
      case req: OffsetCommitRequest => OffsetCommitRequestDataJsonConverter.write(req.data(), request.version())
      case req: OffsetDeleteRequest => OffsetDeleteRequestDataJsonConverter.write(req.data(), request.version())
      case req: OffsetFetchRequest => OffsetFetchRequestDataJsonConverter.write(req.data(), request.version())
      case req: OffsetsForLeaderEpochRequest => offsetsForLeaderEpochRequestNode(req, request.version())
      case req: ProduceRequest => produceRequestNode(req, request.version(), verbose)
      case req: RenewDelegationTokenRequest => RenewDelegationTokenRequestDataJsonConverter.write(req.data(), request.version())
      case req: SaslAuthenticateRequest => SaslAuthenticateRequestDataJsonConverter.write(req.data(), request.version())
      case req: SaslHandshakeRequest => SaslHandshakeRequestDataJsonConverter.write(req.data(), request.version())
      case req: StopReplicaRequest => StopReplicaRequestDataJsonConverter.write(req.data(), request.version())
      case req: SyncGroupRequest => SyncGroupRequestDataJsonConverter.write(req.data(), request.version())
      case req: TxnOffsetCommitRequest => TxnOffsetCommitRequestDataJsonConverter.write(req.data(), request.version())
      case req: UpdateFeaturesRequest => UpdateFeaturesRequestDataJsonConverter.write(req.data(), request.version())
      case req: UpdateMetadataRequest => UpdateMetadataRequestDataJsonConverter.write(req.data(), request.version())
      case req: VoteRequest => VoteRequestDataJsonConverter.write(req.data, request.version())
      case req: WriteTxnMarkersRequest => WriteTxnMarkersRequestDataJsonConverter.write(req.data(), request.version())
    }
  }

  def response(response: AbstractResponse, version: Short): JsonNode = {
    response match {
      case res: AddOffsetsToTxnResponse => AddOffsetsToTxnResponseDataJsonConverter.write(res.data, version)
      case res: AddPartitionsToTxnResponse => AddPartitionsToTxnResponseDataJsonConverter.write(res.data, version)
      case res: AlterClientQuotasResponse => AlterClientQuotasResponseDataJsonConverter.write(res.data(), version)
      case res: AlterConfigsResponse => AlterConfigsResponseDataJsonConverter.write(res.data(), version)
      case res: AlterIsrResponse => AlterIsrResponseDataJsonConverter.write(res.data(), version)
      case res: AlterPartitionReassignmentsResponse => AlterPartitionReassignmentsResponseDataJsonConverter.write(res.data(), version)
      case res: AlterReplicaLogDirsResponse => AlterReplicaLogDirsResponseDataJsonConverter.write(res.data(), version)
      case res: AlterUserScramCredentialsResponse => AlterUserScramCredentialsResponseDataJsonConverter.write(res.data(), version)
      case res: ApiVersionsResponse => ApiVersionsResponseDataJsonConverter.write(res.data, version)
      case res: BeginQuorumEpochResponse => BeginQuorumEpochResponseDataJsonConverter.write(res.data, version)
      case res: ControlledShutdownResponse => ControlledShutdownResponseDataJsonConverter.write(res.data(), version)
      case res: CreateAclsResponse => CreateAclsResponseDataJsonConverter.write(res.data(), version)
      case res: CreateDelegationTokenResponse => CreateDelegationTokenResponseDataJsonConverter.write(res.data(), version)
      case res: CreatePartitionsResponse => CreatePartitionsResponseDataJsonConverter.write(res.data(), version)
      case res: CreateTopicsResponse => CreateTopicsResponseDataJsonConverter.write(res.data(), version)
      case res: DeleteAclsResponse => DeleteAclsResponseDataJsonConverter.write(res.data(), version)
      case res: DeleteGroupsResponse => DeleteGroupsResponseDataJsonConverter.write(res.data, version)
      case res: DeleteRecordsResponse => DeleteRecordsResponseDataJsonConverter.write(res.data(), version)
      case res: DeleteTopicsResponse => DeleteTopicsResponseDataJsonConverter.write(res.data(), version)
      case res: DescribeAclsResponse => DescribeAclsResponseDataJsonConverter.write(res.data(), version)
      case res: DescribeClientQuotasResponse => DescribeClientQuotasResponseDataJsonConverter.write(res.data(), version)
      case res: DescribeConfigsResponse => DescribeConfigsResponseDataJsonConverter.write(res.data(), version)
      case res: DescribeDelegationTokenResponse => DescribeDelegationTokenResponseDataJsonConverter.write(res.data(), version)
      case res: DescribeGroupsResponse => DescribeGroupsResponseDataJsonConverter.write(res.data(), version)
      case res: DescribeLogDirsResponse => DescribeLogDirsResponseDataJsonConverter.write(res.data(), version)
      case res: DescribeQuorumResponse => DescribeQuorumResponseDataJsonConverter.write(res.data, version)
      case res: DescribeUserScramCredentialsResponse => DescribeUserScramCredentialsResponseDataJsonConverter.write(res.data(), version)
      case res: ElectLeadersResponse => ElectLeadersResponseDataJsonConverter.write(res.data(), version)
      case res: EndTxnResponse => EndTxnResponseDataJsonConverter.write(res.data, version)
      case res: EndQuorumEpochResponse => EndQuorumEpochResponseDataJsonConverter.write(res.data, version)
      case res: ExpireDelegationTokenResponse => ExpireDelegationTokenResponseDataJsonConverter.write(res.data(), version)
      case res: FetchResponse[_] => FetchResponseDataJsonConverter.write(res.data(), version)
      case res: FindCoordinatorResponse => FindCoordinatorResponseDataJsonConverter.write(res.data(), version)
      case res: HeartbeatResponse => HeartbeatResponseDataJsonConverter.write(res.data(), version)
      case res: IncrementalAlterConfigsResponse => IncrementalAlterConfigsResponseDataJsonConverter.write(res.data(), version)
      case res: InitProducerIdResponse => InitProducerIdResponseDataJsonConverter.write(res.data, version)
      case res: JoinGroupResponse => JoinGroupResponseDataJsonConverter.write(res.data(), version)
      case res: LeaderAndIsrResponse => LeaderAndIsrResponseDataJsonConverter.write(res.data(), version)
      case res: LeaveGroupResponse => LeaveGroupResponseDataJsonConverter.write(res.data, version)
      case res: ListGroupsResponse => ListGroupsResponseDataJsonConverter.write(res.data(), version)
      case res: ListOffsetResponse => ListOffsetResponseDataJsonConverter.write(res.data(), version)
      case res: ListPartitionReassignmentsResponse => ListPartitionReassignmentsResponseDataJsonConverter.write(res.data(), version)
      case res: MetadataResponse => MetadataResponseDataJsonConverter.write(res.data(), version)
      case res: OffsetCommitResponse => OffsetCommitResponseDataJsonConverter.write(res.data(), version)
      case res: OffsetDeleteResponse => OffsetDeleteResponseDataJsonConverter.write(res.data, version)
      case res: OffsetFetchResponse => OffsetFetchResponseDataJsonConverter.write(res.data, version)
      case res: OffsetsForLeaderEpochResponse => offsetsForLeaderEpochResponseNode(res, version)
      case res: ProduceResponse => produceResponseNode(res, version)
      case res: RenewDelegationTokenResponse => RenewDelegationTokenResponseDataJsonConverter.write(res.data(), version)
      case res: SaslAuthenticateResponse => SaslAuthenticateResponseDataJsonConverter.write(res.data(), version)
      case res: SaslHandshakeResponse => SaslHandshakeResponseDataJsonConverter.write(res.data(), version)
      case res: StopReplicaResponse => StopReplicaResponseDataJsonConverter.write(res.data(), version)
      case res: SyncGroupResponse => SyncGroupResponseDataJsonConverter.write(res.data, version)
      case res: TxnOffsetCommitResponse => TxnOffsetCommitResponseDataJsonConverter.write(res.data, version)
      case res: UpdateFeaturesResponse => UpdateFeaturesResponseDataJsonConverter.write(res.data(), version)
      case res: UpdateMetadataResponse => UpdateMetadataResponseDataJsonConverter.write(res.data(), version)
      case res: WriteTxnMarkersResponse => WriteTxnMarkersResponseDataJsonConverter.write(res.data, version)
      case res: VoteResponse => VoteResponseDataJsonConverter.write(res.data, version)
    }
  }

  def requestHeaderNode(header: RequestHeader): JsonNode = {
    val node = RequestHeaderDataJsonConverter.write(header.data(), header.headerVersion()).asInstanceOf[ObjectNode]
    node.set("requestApiKey", new TextNode(header.apiKey.toString))
    node
  }

  def requestDescMetrics(header: RequestHeader, res: Response, req: AbstractRequest,
                  context: RequestContext, session: Session, verbose: Boolean,
                  totalTimeMs: Double, requestQueueTimeMs: Double, apiLocalTimeMs: Double,
                  apiRemoteTimeMs: Double, apiThrottleTimeMs: Long, responseQueueTimeMs: Double,
                  responseSendTimeMs: Double, temporaryMemoryBytes: Long,
                  messageConversionsTimeMs: Double): JsonNode = {
    val node = new ObjectNode(JsonNodeFactory.instance)
    node.set("requestHeader", requestHeaderNode(header))
    node.set("request", request(req, verbose))
    node.set("response", res.responseLog.getOrElse(new TextNode("")))
    node.set("connection", new TextNode(context.connectionId))
    node.set("totalTime", new DoubleNode(totalTimeMs))
    node.set("requestQueueTime", new DoubleNode(requestQueueTimeMs))
    node.set("localTime", new DoubleNode(apiLocalTimeMs))
    node.set("remoteTime", new DoubleNode(apiRemoteTimeMs))
    node.set("throttleTime", new LongNode(apiThrottleTimeMs))
    node.set("responseQueueTime", new DoubleNode(responseQueueTimeMs))
    node.set("sendTime", new DoubleNode(responseSendTimeMs))
    node.set("securityProtocol", new TextNode(context.securityProtocol.toString))
    node.set("principal", new TextNode(session.principal.toString))
    node.set("listener", new TextNode(context.listenerName.value))
    node.set("clientInformation", new TextNode(context.clientInformation.toString))
    if (temporaryMemoryBytes > 0)
      node.set("temporaryMemoryBytes", new LongNode(temporaryMemoryBytes))
    if (messageConversionsTimeMs > 0)
      node.set("messageConversionsTime", new DoubleNode(messageConversionsTimeMs))
    node
  }

  def requestDesc(header: RequestHeader, req: AbstractRequest, verbose: Boolean): JsonNode = {
    val node = new ObjectNode(JsonNodeFactory.instance)
    node.set("requestHeader", requestHeaderNode(header))
    node.set("request", request(req, verbose))
    node
  }

  /**
   * Temporary until switch to use the generated schemas.
   */
  def offsetsForLeaderEpochRequestNode(request: OffsetsForLeaderEpochRequest, version: Short): JsonNode = {
    val node = new ObjectNode(JsonNodeFactory.instance)
    if (version >= 3) {
      node.set("replicaId", new IntNode(request.replicaId))
    }
    val topicsToPartitionEpochs = CollectionUtils.groupPartitionDataByTopic(request.epochsByTopicPartition)
    val topicsArray = new ArrayNode(JsonNodeFactory.instance)
    for (topicToEpochs <- topicsToPartitionEpochs.entrySet.asScala) {
      val topicsData = new ObjectNode(JsonNodeFactory.instance)
      topicsData.set("name", new TextNode(topicToEpochs.getKey))
      val partitionsArray = new ArrayNode(JsonNodeFactory.instance)
      for (partitionEpoch <- topicToEpochs.getValue.entrySet.asScala) {
        val partitionData = partitionEpoch.getValue
        val partitionNode = new ObjectNode(JsonNodeFactory.instance)
        partitionNode.set("partitionIndex", new IntNode(partitionEpoch.getKey))
        partitionNode.set("leaderEpoch", new IntNode(partitionData.leaderEpoch))
        if (version >= 2) {
          val leaderEpoch = partitionData.currentLeaderEpoch
          partitionNode.set("currentLeaderEpoch", new IntNode(leaderEpoch.orElse(RecordBatch.NO_PARTITION_LEADER_EPOCH)))
        }
        partitionsArray.add(partitionNode)
      }
      topicsData.set("partitions", partitionsArray)
      topicsArray.add(topicsData)
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
    if (verbose) {
      val partSizes = new ArrayNode(JsonNodeFactory.instance)
      for (partSize <- request.partitionSizes().entrySet.asScala) {
        val part = new ObjectNode(JsonNodeFactory.instance)
        val topic = partSize.getKey
        part.set("partition", new TextNode(topic.toString))
        part.set("size", new IntNode(partSize.getValue))
        partSizes.add(part)
      }
      node.set("partitionSizes", partSizes)
    } else {
      node.set("partitionSizes", new IntNode(request.partitionSizes.size))
    }
    node
  }

  /**
   * Temporary until switch to use the generated schemas.
   */
  def offsetsForLeaderEpochResponseNode(response: OffsetsForLeaderEpochResponse, version: Short): JsonNode = {
    val node = new ObjectNode(JsonNodeFactory.instance)
    if (version >= 2) {
      node.set("throttleTimeMs", new IntNode(response.throttleTimeMs))
    }
    val endOffsetsByTopic = CollectionUtils.groupPartitionDataByTopic(response.responses)
    val topicsArray = new ArrayNode(JsonNodeFactory.instance)
    for (topicToPartitionEpochs <- endOffsetsByTopic.entrySet.asScala) {
      val topicData = new ObjectNode(JsonNodeFactory.instance)
      topicData.set("name", new TextNode(topicToPartitionEpochs.getKey))
      val partitionsArray = new ArrayNode(JsonNodeFactory.instance)
      for (partitionEndOffset <- topicToPartitionEpochs.getValue.entrySet.asScala) {
        val epochEndOffset = partitionEndOffset.getValue
        val partitionData = new ObjectNode(JsonNodeFactory.instance)
        partitionData.set("errorCode", new ShortNode(epochEndOffset.error.code))
        partitionData.set("partitionIndex", new IntNode(partitionEndOffset.getKey))
        if (version >= 1) partitionData.set("leaderEpoch", new IntNode(epochEndOffset.leaderEpoch))
        partitionData.set("endOffset", new LongNode(epochEndOffset.endOffset))
        partitionsArray.add(partitionData)
      }
      topicData.set("partitions", partitionsArray)
      topicsArray.add(topicData)
    }
    node.set("topics", topicsArray)
    node
  }

  /**
   * Temporary until switch to use the generated schemas.
   */
  def produceResponseNode(response: ProduceResponse, version: Short): JsonNode = {
    val node = new ObjectNode(JsonNodeFactory.instance)
    val responseByTopic = CollectionUtils.groupPartitionDataByTopic(response.responses)
    val responsesArray = new ArrayNode(JsonNodeFactory.instance)
    for (entry <- responseByTopic.entrySet.asScala) {
      val topicData = new ObjectNode(JsonNodeFactory.instance)
      topicData.set("name", new TextNode(entry.getKey))
      val partitionsArray = new ArrayNode(JsonNodeFactory.instance)
      for (partitionEntry <- entry.getValue.entrySet.asScala) {
        val part = partitionEntry.getValue
        val partitionData = new ObjectNode(JsonNodeFactory.instance)
        partitionData.set("partitionIndex", new IntNode(partitionEntry.getKey))
        var errorCode = part.error.code
        if (errorCode == Errors.KAFKA_STORAGE_ERROR.code && version <= 3) {
          errorCode = Errors.NOT_LEADER_OR_FOLLOWER.code
        }
        partitionData.set("errorCode", new ShortNode(errorCode))
        partitionData.set("baseOffset", new LongNode(part.baseOffset))
        if (version >= 2) {
          partitionData.set("logAppendTimeMs", new LongNode(part.logAppendTime))
        }
        if (version >= 5) {
          partitionData.set("logStartOffset", new LongNode(part.logStartOffset))
        }
        if (version >= 8) {
          val recordErrorsArray = new ArrayNode(JsonNodeFactory.instance)
          for (indexAndMessage <- part.recordErrors.asScala) {
            val indexAndMessageData = new ObjectNode(JsonNodeFactory.instance)
            indexAndMessageData.set("batchIndex", new IntNode(indexAndMessage.batchIndex))
            if (indexAndMessage.message == null) indexAndMessageData.set("batchIndexErrorMessage", NullNode.instance)
            else indexAndMessageData.set("batchIndexErrorMessage", new TextNode(indexAndMessage.message))
            recordErrorsArray.add(indexAndMessageData)
          }
          partitionData.set("recordErrors", recordErrorsArray)
          if (part.errorMessage == null) partitionData.set("errorMessage", NullNode.instance)
          else partitionData.set("errorMessage", new TextNode(part.errorMessage))
        }
        partitionsArray.add(partitionData)
      }
      topicData.set("partitions", partitionsArray)
      responsesArray.add(topicData)
    }
    node.set("responses", responsesArray)
    if (version >= 1) {
      node.set("throttleTimeMs", new IntNode(response.throttleTimeMs))
    }
    node
  }
}
