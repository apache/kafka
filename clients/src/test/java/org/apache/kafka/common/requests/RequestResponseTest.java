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

package org.apache.kafka.common.requests;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ProtoUtils;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.common.record.Record;
import org.junit.Test;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class RequestResponseTest {

    @Test
    public void testSerialization() throws Exception {
        List<AbstractRequestResponse> requestResponseList = Arrays.asList(
                createRequestHeader(),
                createResponseHeader(),
                createGroupCoordinatorRequest(),
                createGroupCoordinatorRequest().getErrorResponse(0, new UnknownServerException()),
                createGroupCoordinatorResponse(),
                createControlledShutdownRequest(),
                createControlledShutdownResponse(),
                createControlledShutdownRequest().getErrorResponse(1, new UnknownServerException()),
                createFetchRequest(),
                createFetchRequest().getErrorResponse(1, new UnknownServerException()),
                createFetchResponse(),
                createHeartBeatRequest(),
                createHeartBeatRequest().getErrorResponse(0, new UnknownServerException()),
                createHeartBeatResponse(),
                createJoinGroupRequest(),
                createJoinGroupRequest().getErrorResponse(0, new UnknownServerException()),
                createJoinGroupResponse(),
                createLeaveGroupRequest(),
                createLeaveGroupRequest().getErrorResponse(0, new UnknownServerException()),
                createLeaveGroupResponse(),
                createListGroupsRequest(),
                createListGroupsRequest().getErrorResponse(0, new UnknownServerException()),
                createListGroupsResponse(),
                createDescribeGroupRequest(),
                createDescribeGroupRequest().getErrorResponse(0, new UnknownServerException()),
                createDescribeGroupResponse(),
                createListOffsetRequest(),
                createListOffsetRequest().getErrorResponse(0, new UnknownServerException()),
                createListOffsetResponse(),
                MetadataRequest.allTopics(),
                createMetadataRequest(Arrays.asList("topic1")),
                createMetadataRequest(Arrays.asList("topic1")).getErrorResponse(1, new UnknownServerException()),
                createMetadataResponse(1),
                createOffsetCommitRequest(2),
                createOffsetCommitRequest(2).getErrorResponse(2, new UnknownServerException()),
                createOffsetCommitResponse(),
                createOffsetFetchRequest(),
                createOffsetFetchRequest().getErrorResponse(0, new UnknownServerException()),
                createOffsetFetchResponse(),
                createProduceRequest(),
                createProduceRequest().getErrorResponse(2, new UnknownServerException()),
                createProduceResponse(),
                createStopReplicaRequest(),
                createStopReplicaRequest().getErrorResponse(0, new UnknownServerException()),
                createStopReplicaResponse(),
                createUpdateMetadataRequest(2, "rack1"),
                createUpdateMetadataRequest(2, null),
                createUpdateMetadataRequest(2, "rack1").getErrorResponse(2, new UnknownServerException()),
                createUpdateMetadataResponse(),
                createLeaderAndIsrRequest(),
                createLeaderAndIsrRequest().getErrorResponse(0, new UnknownServerException()),
                createLeaderAndIsrResponse(),
                createSaslHandshakeRequest(),
                createSaslHandshakeRequest().getErrorResponse(0, new UnknownServerException()),
                createSaslHandshakeResponse(),
                createApiVersionRequest(),
                createApiVersionRequest().getErrorResponse(0, new UnknownServerException()),
                createApiVersionResponse()
        );

        for (AbstractRequestResponse req : requestResponseList)
            checkSerialization(req, null);

        createMetadataResponse(0);
        createMetadataRequest(Arrays.asList("topic1")).getErrorResponse(0, new UnknownServerException());
        checkSerialization(createFetchRequest().getErrorResponse(0, new UnknownServerException()), 0);
        checkSerialization(createOffsetCommitRequest(0), 0);
        checkSerialization(createOffsetCommitRequest(0).getErrorResponse(0, new UnknownServerException()), 0);
        checkSerialization(createOffsetCommitRequest(1), 1);
        checkSerialization(createOffsetCommitRequest(1).getErrorResponse(1, new UnknownServerException()), 1);
        checkSerialization(createUpdateMetadataRequest(0, null), 0);
        checkSerialization(createUpdateMetadataRequest(0, null).getErrorResponse(0, new UnknownServerException()), 0);
        checkSerialization(createUpdateMetadataRequest(1, null), 1);
        checkSerialization(createUpdateMetadataRequest(1, "rack1"), 1);
        checkSerialization(createUpdateMetadataRequest(1, null).getErrorResponse(1, new UnknownServerException()), 1);
    }

    private void checkSerialization(AbstractRequestResponse req, Integer version) throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(req.sizeOf());
        req.writeTo(buffer);
        buffer.rewind();
        AbstractRequestResponse deserialized;
        if (version == null) {
            Method deserializer = req.getClass().getDeclaredMethod("parse", ByteBuffer.class);
            deserialized = (AbstractRequestResponse) deserializer.invoke(null, buffer);
        } else {
            Method deserializer = req.getClass().getDeclaredMethod("parse", ByteBuffer.class, Integer.TYPE);
            deserialized = (AbstractRequestResponse) deserializer.invoke(null, buffer, version);
        }
        assertEquals("The original and deserialized of " + req.getClass().getSimpleName() + " should be the same.", req, deserialized);
        assertEquals("The original and deserialized of " + req.getClass().getSimpleName() + " should have the same hashcode.",
                req.hashCode(), deserialized.hashCode());
    }

    @Test
    public void produceResponseVersionTest() {
        Map<TopicPartition, ProduceResponse.PartitionResponse> responseData = new HashMap<>();
        responseData.put(new TopicPartition("test", 0), new ProduceResponse.PartitionResponse(Errors.NONE.code(), 10000, Record.NO_TIMESTAMP));
        ProduceResponse v0Response = new ProduceResponse(responseData);
        ProduceResponse v1Response = new ProduceResponse(responseData, 10, 1);
        ProduceResponse v2Response = new ProduceResponse(responseData, 10, 2);
        assertEquals("Throttle time must be zero", 0, v0Response.getThrottleTime());
        assertEquals("Throttle time must be 10", 10, v1Response.getThrottleTime());
        assertEquals("Throttle time must be 10", 10, v2Response.getThrottleTime());
        assertEquals("Should use schema version 0", ProtoUtils.responseSchema(ApiKeys.PRODUCE.id, 0), v0Response.toStruct().schema());
        assertEquals("Should use schema version 1", ProtoUtils.responseSchema(ApiKeys.PRODUCE.id, 1), v1Response.toStruct().schema());
        assertEquals("Should use schema version 2", ProtoUtils.responseSchema(ApiKeys.PRODUCE.id, 2), v2Response.toStruct().schema());
        assertEquals("Response data does not match", responseData, v0Response.responses());
        assertEquals("Response data does not match", responseData, v1Response.responses());
        assertEquals("Response data does not match", responseData, v2Response.responses());
    }

    @Test
    public void fetchResponseVersionTest() {
        Map<TopicPartition, FetchResponse.PartitionData> responseData = new HashMap<>();
        responseData.put(new TopicPartition("test", 0), new FetchResponse.PartitionData(Errors.NONE.code(), 1000000, ByteBuffer.allocate(10)));

        FetchResponse v0Response = new FetchResponse(responseData);
        FetchResponse v1Response = new FetchResponse(responseData, 10);
        assertEquals("Throttle time must be zero", 0, v0Response.getThrottleTime());
        assertEquals("Throttle time must be 10", 10, v1Response.getThrottleTime());
        assertEquals("Should use schema version 0", ProtoUtils.responseSchema(ApiKeys.FETCH.id, 0), v0Response.toStruct().schema());
        assertEquals("Should use schema version 1", ProtoUtils.responseSchema(ApiKeys.FETCH.id, 1), v1Response.toStruct().schema());
        assertEquals("Response data does not match", responseData, v0Response.responseData());
        assertEquals("Response data does not match", responseData, v1Response.responseData());
    }

    @Test
    public void testControlledShutdownResponse() {
        ControlledShutdownResponse response = createControlledShutdownResponse();
        ByteBuffer buffer = ByteBuffer.allocate(response.sizeOf());
        response.writeTo(buffer);
        buffer.rewind();
        ControlledShutdownResponse deserialized = ControlledShutdownResponse.parse(buffer);
        assertEquals(response.errorCode(), deserialized.errorCode());
        assertEquals(response.partitionsRemaining(), deserialized.partitionsRemaining());
    }

    @Test
    public void testRequestHeaderWithNullClientId() {
        RequestHeader header = new RequestHeader((short) 10, (short) 1, null, 10);
        ByteBuffer buffer = ByteBuffer.allocate(header.sizeOf());
        header.writeTo(buffer);
        buffer.rewind();
        RequestHeader deserialized = RequestHeader.parse(buffer);
        assertEquals(header.apiKey(), deserialized.apiKey());
        assertEquals(header.apiVersion(), deserialized.apiVersion());
        assertEquals(header.correlationId(), deserialized.correlationId());
        assertEquals("", deserialized.clientId()); // null is defaulted to ""
    }

    private AbstractRequestResponse createRequestHeader() {
        return new RequestHeader((short) 10, (short) 1, "", 10);
    }

    private AbstractRequestResponse createResponseHeader() {
        return new ResponseHeader(10);
    }

    private AbstractRequest createGroupCoordinatorRequest() {
        return new GroupCoordinatorRequest("test-group");
    }

    private AbstractRequestResponse createGroupCoordinatorResponse() {
        return new GroupCoordinatorResponse(Errors.NONE.code(), new Node(10, "host1", 2014));
    }

    private AbstractRequest createFetchRequest() {
        Map<TopicPartition, FetchRequest.PartitionData> fetchData = new HashMap<>();
        fetchData.put(new TopicPartition("test1", 0), new FetchRequest.PartitionData(100, 1000000));
        fetchData.put(new TopicPartition("test2", 0), new FetchRequest.PartitionData(200, 1000000));
        return new FetchRequest(-1, 100, 100000, fetchData);
    }

    private AbstractRequestResponse createFetchResponse() {
        Map<TopicPartition, FetchResponse.PartitionData> responseData = new HashMap<>();
        responseData.put(new TopicPartition("test", 0), new FetchResponse.PartitionData(Errors.NONE.code(), 1000000, ByteBuffer.allocate(10)));
        return new FetchResponse(responseData, 0);
    }

    private AbstractRequest createHeartBeatRequest() {
        return new HeartbeatRequest("group1", 1, "consumer1");
    }

    private AbstractRequestResponse createHeartBeatResponse() {
        return new HeartbeatResponse(Errors.NONE.code());
    }

    private AbstractRequest createJoinGroupRequest() {
        ByteBuffer metadata = ByteBuffer.wrap(new byte[] {});
        List<JoinGroupRequest.ProtocolMetadata> protocols = new ArrayList<>();
        protocols.add(new JoinGroupRequest.ProtocolMetadata("consumer-range", metadata));
        return new JoinGroupRequest("group1", 30000, "consumer1", "consumer", protocols);
    }

    private AbstractRequestResponse createJoinGroupResponse() {
        Map<String, ByteBuffer> members = new HashMap<>();
        members.put("consumer1", ByteBuffer.wrap(new byte[]{}));
        members.put("consumer2", ByteBuffer.wrap(new byte[]{}));
        return new JoinGroupResponse(Errors.NONE.code(), 1, "range", "consumer1", "leader", members);
    }

    private AbstractRequest createListGroupsRequest() {
        return new ListGroupsRequest();
    }

    private AbstractRequestResponse createListGroupsResponse() {
        List<ListGroupsResponse.Group> groups = Arrays.asList(new ListGroupsResponse.Group("test-group", "consumer"));
        return new ListGroupsResponse(Errors.NONE.code(), groups);
    }

    private AbstractRequest createDescribeGroupRequest() {
        return new DescribeGroupsRequest(Collections.singletonList("test-group"));
    }

    private AbstractRequestResponse createDescribeGroupResponse() {
        String clientId = "consumer-1";
        String clientHost = "localhost";
        ByteBuffer empty = ByteBuffer.allocate(0);
        DescribeGroupsResponse.GroupMember member = new DescribeGroupsResponse.GroupMember("memberId",
                clientId, clientHost, empty, empty);
        DescribeGroupsResponse.GroupMetadata metadata = new DescribeGroupsResponse.GroupMetadata(Errors.NONE.code(),
                "STABLE", "consumer", "roundrobin", Arrays.asList(member));
        return new DescribeGroupsResponse(Collections.singletonMap("test-group", metadata));
    }

    private AbstractRequest createLeaveGroupRequest() {
        return new LeaveGroupRequest("group1", "consumer1");
    }

    private AbstractRequestResponse createLeaveGroupResponse() {
        return new LeaveGroupResponse(Errors.NONE.code());
    }

    private AbstractRequest createListOffsetRequest() {
        Map<TopicPartition, ListOffsetRequest.PartitionData> offsetData = new HashMap<>();
        offsetData.put(new TopicPartition("test", 0), new ListOffsetRequest.PartitionData(1000000L, 10));
        return new ListOffsetRequest(-1, offsetData);
    }

    private AbstractRequestResponse createListOffsetResponse() {
        Map<TopicPartition, ListOffsetResponse.PartitionData> responseData = new HashMap<>();
        responseData.put(new TopicPartition("test", 0), new ListOffsetResponse.PartitionData(Errors.NONE.code(), Arrays.asList(100L)));
        return new ListOffsetResponse(responseData);
    }

    private AbstractRequest createMetadataRequest(List<String> topics) {
        return new MetadataRequest(topics);
    }

    private AbstractRequestResponse createMetadataResponse(int version) {
        Node node = new Node(1, "host1", 1001);
        List<Node> replicas = Arrays.asList(node);
        List<Node> isr = Arrays.asList(node);

        List<MetadataResponse.TopicMetadata> allTopicMetadata = new ArrayList<>();
        allTopicMetadata.add(new MetadataResponse.TopicMetadata(Errors.NONE, "__consumer_offsets", true,
                Arrays.asList(new MetadataResponse.PartitionMetadata(Errors.NONE, 1, node, replicas, isr))));
        allTopicMetadata.add(new MetadataResponse.TopicMetadata(Errors.LEADER_NOT_AVAILABLE, "topic2", false,
                Collections.<MetadataResponse.PartitionMetadata>emptyList()));

        return new MetadataResponse(Arrays.asList(node), MetadataResponse.NO_CONTROLLER_ID, allTopicMetadata, version);
    }

    private AbstractRequest createOffsetCommitRequest(int version) {
        Map<TopicPartition, OffsetCommitRequest.PartitionData> commitData = new HashMap<>();
        commitData.put(new TopicPartition("test", 0), new OffsetCommitRequest.PartitionData(100, ""));
        commitData.put(new TopicPartition("test", 1), new OffsetCommitRequest.PartitionData(200, null));
        if (version == 0) {
            return new OffsetCommitRequest("group1", commitData);
        } else if (version == 1) {
            return new OffsetCommitRequest("group1", 100, "consumer1", commitData);
        } else if (version == 2) {
            return new OffsetCommitRequest("group1", 100, "consumer1", 1000000, commitData);
        }
        throw new IllegalArgumentException("Unknown offset commit request version " + version);
    }

    private AbstractRequestResponse createOffsetCommitResponse() {
        Map<TopicPartition, Short> responseData = new HashMap<>();
        responseData.put(new TopicPartition("test", 0), Errors.NONE.code());
        return new OffsetCommitResponse(responseData);
    }

    private AbstractRequest createOffsetFetchRequest() {
        return new OffsetFetchRequest("group1", Arrays.asList(new TopicPartition("test11", 1)));
    }

    private AbstractRequestResponse createOffsetFetchResponse() {
        Map<TopicPartition, OffsetFetchResponse.PartitionData> responseData = new HashMap<>();
        responseData.put(new TopicPartition("test", 0), new OffsetFetchResponse.PartitionData(100L, "", Errors.NONE.code()));
        responseData.put(new TopicPartition("test", 1), new OffsetFetchResponse.PartitionData(100L, null, Errors.NONE.code()));
        return new OffsetFetchResponse(responseData);
    }

    private AbstractRequest createProduceRequest() {
        Map<TopicPartition, ByteBuffer> produceData = new HashMap<>();
        produceData.put(new TopicPartition("test", 0), ByteBuffer.allocate(10));
        return new ProduceRequest((short) 1, 5000, produceData);
    }

    private AbstractRequestResponse createProduceResponse() {
        Map<TopicPartition, ProduceResponse.PartitionResponse> responseData = new HashMap<>();
        responseData.put(new TopicPartition("test", 0), new ProduceResponse.PartitionResponse(Errors.NONE.code(), 10000, Record.NO_TIMESTAMP));
        return new ProduceResponse(responseData, 0);
    }

    private AbstractRequest createStopReplicaRequest() {
        Set<TopicPartition> partitions = new HashSet<>(Arrays.asList(new TopicPartition("test", 0)));
        return new StopReplicaRequest(0, 1, true, partitions);
    }

    private AbstractRequestResponse createStopReplicaResponse() {
        Map<TopicPartition, Short> responses = new HashMap<>();
        responses.put(new TopicPartition("test", 0), Errors.NONE.code());
        return new StopReplicaResponse(Errors.NONE.code(), responses);
    }

    private AbstractRequest createControlledShutdownRequest() {
        return new ControlledShutdownRequest(10);
    }

    private ControlledShutdownResponse createControlledShutdownResponse() {
        HashSet<TopicPartition> topicPartitions = new HashSet<>(Arrays.asList(
                new TopicPartition("test2", 5),
                new TopicPartition("test1", 10)
        ));
        return new ControlledShutdownResponse(Errors.NONE.code(), topicPartitions);
    }

    private AbstractRequest createLeaderAndIsrRequest() {
        Map<TopicPartition, LeaderAndIsrRequest.PartitionState> partitionStates = new HashMap<>();
        List<Integer> isr = Arrays.asList(1, 2);
        List<Integer> replicas = Arrays.asList(1, 2, 3, 4);
        partitionStates.put(new TopicPartition("topic5", 105),
                new LeaderAndIsrRequest.PartitionState(0, 2, 1, new ArrayList<>(isr), 2, new HashSet<>(replicas)));
        partitionStates.put(new TopicPartition("topic5", 1),
                new LeaderAndIsrRequest.PartitionState(1, 1, 1, new ArrayList<>(isr), 2, new HashSet<>(replicas)));
        partitionStates.put(new TopicPartition("topic20", 1),
                new LeaderAndIsrRequest.PartitionState(1, 0, 1, new ArrayList<>(isr), 2, new HashSet<>(replicas)));

        Set<Node> leaders = new HashSet<>(Arrays.asList(
                new Node(0, "test0", 1223),
                new Node(1, "test1", 1223)
        ));

        return new LeaderAndIsrRequest(1, 10, partitionStates, leaders);
    }

    private AbstractRequestResponse createLeaderAndIsrResponse() {
        Map<TopicPartition, Short> responses = new HashMap<>();
        responses.put(new TopicPartition("test", 0), Errors.NONE.code());
        return new LeaderAndIsrResponse(Errors.NONE.code(), responses);
    }

    @SuppressWarnings("deprecation")
    private AbstractRequest createUpdateMetadataRequest(int version, String rack) {
        Map<TopicPartition, UpdateMetadataRequest.PartitionState> partitionStates = new HashMap<>();
        List<Integer> isr = Arrays.asList(1, 2);
        List<Integer> replicas = Arrays.asList(1, 2, 3, 4);
        partitionStates.put(new TopicPartition("topic5", 105),
                new UpdateMetadataRequest.PartitionState(0, 2, 1, new ArrayList<>(isr), 2, new HashSet<>(replicas)));
        partitionStates.put(new TopicPartition("topic5", 1),
                new UpdateMetadataRequest.PartitionState(1, 1, 1, new ArrayList<>(isr), 2, new HashSet<>(replicas)));
        partitionStates.put(new TopicPartition("topic20", 1),
                new UpdateMetadataRequest.PartitionState(1, 0, 1, new ArrayList<>(isr), 2, new HashSet<>(replicas)));

        if (version == 0) {
            Set<Node> liveBrokers = new HashSet<>(Arrays.asList(
                    new Node(0, "host1", 1223),
                    new Node(1, "host2", 1234)
            ));

            return new UpdateMetadataRequest(1, 10, liveBrokers, partitionStates);
        } else {
            Map<SecurityProtocol, UpdateMetadataRequest.EndPoint> endPoints1 = new HashMap<>();
            endPoints1.put(SecurityProtocol.PLAINTEXT, new UpdateMetadataRequest.EndPoint("host1", 1223));

            Map<SecurityProtocol, UpdateMetadataRequest.EndPoint> endPoints2 = new HashMap<>();
            endPoints2.put(SecurityProtocol.PLAINTEXT, new UpdateMetadataRequest.EndPoint("host1", 1244));
            endPoints2.put(SecurityProtocol.SSL, new UpdateMetadataRequest.EndPoint("host2", 1234));

            Set<UpdateMetadataRequest.Broker> liveBrokers = new HashSet<>(Arrays.asList(new UpdateMetadataRequest.Broker(0, endPoints1, rack),
                    new UpdateMetadataRequest.Broker(1, endPoints2, rack)
            ));
            return new UpdateMetadataRequest(version, 1, 10, partitionStates, liveBrokers);
        }
    }

    private AbstractRequestResponse createUpdateMetadataResponse() {
        return new UpdateMetadataResponse(Errors.NONE.code());
    }

    private AbstractRequest createSaslHandshakeRequest() {
        return new SaslHandshakeRequest("PLAIN");
    }

    private AbstractRequestResponse createSaslHandshakeResponse() {
        return new SaslHandshakeResponse(Errors.NONE.code(), Collections.singletonList("GSSAPI"));
    }

    private AbstractRequest createApiVersionRequest() {
        return new ApiVersionsRequest();
    }

    private AbstractRequestResponse createApiVersionResponse() {
        List<ApiVersionsResponse.ApiVersion> apiVersions = Arrays.asList(new ApiVersionsResponse.ApiVersion((short) 0, (short) 0, (short) 2));
        return new ApiVersionsResponse(Errors.NONE.code(), apiVersions);
    }
}