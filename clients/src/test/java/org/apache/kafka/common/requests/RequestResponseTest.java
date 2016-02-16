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

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ProtoUtils;
import org.apache.kafka.common.protocol.SecurityProtocol;
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
                createFetchRequest().getErrorResponse(0, new UnknownServerException()),
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
                createMetadataRequest(),
                createMetadataRequest().getErrorResponse(0, new UnknownServerException()),
                createMetadataResponse(),
                createOffsetCommitRequest(),
                createOffsetCommitRequest().getErrorResponse(0, new UnknownServerException()),
                createOffsetCommitResponse(),
                createOffsetFetchRequest(),
                createOffsetFetchRequest().getErrorResponse(0, new UnknownServerException()),
                createOffsetFetchResponse(),
                createProduceRequest(),
                createProduceRequest().getErrorResponse(0, new UnknownServerException()),
                createProduceResponse(),
                createStopReplicaRequest(),
                createStopReplicaRequest().getErrorResponse(0, new UnknownServerException()),
                createStopReplicaResponse(),
                createUpdateMetadataRequest(1),
                createUpdateMetadataRequest(1).getErrorResponse(1, new UnknownServerException()),
                createUpdateMetadataResponse(),
                createLeaderAndIsrRequest(),
                createLeaderAndIsrRequest().getErrorResponse(0, new UnknownServerException()),
                createLeaderAndIsrResponse()
        );

        for (AbstractRequestResponse req : requestResponseList)
            checkSerialization(req, null);

        checkSerialization(createUpdateMetadataRequest(0), 0);
        checkSerialization(createUpdateMetadataRequest(0).getErrorResponse(0, new UnknownServerException()), 0);
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
        Map<TopicPartition, ProduceResponse.PartitionResponse> responseData = new HashMap<TopicPartition, ProduceResponse.PartitionResponse>();
        responseData.put(new TopicPartition("test", 0), new ProduceResponse.PartitionResponse(Errors.NONE.code(), 10000));

        ProduceResponse v0Response = new ProduceResponse(responseData);
        ProduceResponse v1Response = new ProduceResponse(responseData, 10);
        assertEquals("Throttle time must be zero", 0, v0Response.getThrottleTime());
        assertEquals("Throttle time must be 10", 10, v1Response.getThrottleTime());
        assertEquals("Should use schema version 0", ProtoUtils.responseSchema(ApiKeys.PRODUCE.id, 0), v0Response.toStruct().schema());
        assertEquals("Should use schema version 1", ProtoUtils.responseSchema(ApiKeys.PRODUCE.id, 1), v1Response.toStruct().schema());
        assertEquals("Response data does not match", responseData, v0Response.responses());
        assertEquals("Response data does not match", responseData, v1Response.responses());
    }

    @Test
    public void fetchResponseVersionTest() {
        Map<TopicPartition, FetchResponse.PartitionData> responseData = new HashMap<TopicPartition, FetchResponse.PartitionData>();
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
        Map<TopicPartition, FetchRequest.PartitionData> fetchData = new HashMap<TopicPartition, FetchRequest.PartitionData>();
        fetchData.put(new TopicPartition("test1", 0), new FetchRequest.PartitionData(100, 1000000));
        fetchData.put(new TopicPartition("test2", 0), new FetchRequest.PartitionData(200, 1000000));
        return new FetchRequest(-1, 100, 100000, fetchData);
    }

    private AbstractRequestResponse createFetchResponse() {
        Map<TopicPartition, FetchResponse.PartitionData> responseData = new HashMap<TopicPartition, FetchResponse.PartitionData>();
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
        Map<TopicPartition, ListOffsetRequest.PartitionData> offsetData = new HashMap<TopicPartition, ListOffsetRequest.PartitionData>();
        offsetData.put(new TopicPartition("test", 0), new ListOffsetRequest.PartitionData(1000000L, 10));
        return new ListOffsetRequest(-1, offsetData);
    }

    private AbstractRequestResponse createListOffsetResponse() {
        Map<TopicPartition, ListOffsetResponse.PartitionData> responseData = new HashMap<TopicPartition, ListOffsetResponse.PartitionData>();
        responseData.put(new TopicPartition("test", 0), new ListOffsetResponse.PartitionData(Errors.NONE.code(), Arrays.asList(100L)));
        return new ListOffsetResponse(responseData);
    }

    private AbstractRequest createMetadataRequest() {
        return new MetadataRequest(Arrays.asList("topic1"));
    }

    private AbstractRequestResponse createMetadataResponse() {
        Node node = new Node(1, "host1", 1001);
        Node[] replicas = new Node[1];
        replicas[0] = node;
        Node[] isr = new Node[1];
        isr[0] = node;
        Cluster cluster = new Cluster(Arrays.asList(node), Arrays.asList(new PartitionInfo("topic1", 1, node, replicas, isr)),
                Collections.<String>emptySet());

        Map<String, Errors> errors = new HashMap<String, Errors>();
        errors.put("topic2", Errors.LEADER_NOT_AVAILABLE);
        return new MetadataResponse(cluster, errors);
    }

    private AbstractRequest createOffsetCommitRequest() {
        Map<TopicPartition, OffsetCommitRequest.PartitionData> commitData = new HashMap<TopicPartition, OffsetCommitRequest.PartitionData>();
        commitData.put(new TopicPartition("test", 0), new OffsetCommitRequest.PartitionData(100, ""));
        return new OffsetCommitRequest("group1", 100, "consumer1", 1000000, commitData);
    }

    private AbstractRequestResponse createOffsetCommitResponse() {
        Map<TopicPartition, Short> responseData = new HashMap<TopicPartition, Short>();
        responseData.put(new TopicPartition("test", 0), Errors.NONE.code());
        return new OffsetCommitResponse(responseData);
    }

    private AbstractRequest createOffsetFetchRequest() {
        return new OffsetFetchRequest("group1", Arrays.asList(new TopicPartition("test11", 1)));
    }

    private AbstractRequestResponse createOffsetFetchResponse() {
        Map<TopicPartition, OffsetFetchResponse.PartitionData> responseData = new HashMap<TopicPartition, OffsetFetchResponse.PartitionData>();
        responseData.put(new TopicPartition("test", 0), new OffsetFetchResponse.PartitionData(100L, "", Errors.NONE.code()));
        return new OffsetFetchResponse(responseData);
    }

    private AbstractRequest createProduceRequest() {
        Map<TopicPartition, ByteBuffer> produceData = new HashMap<TopicPartition, ByteBuffer>();
        produceData.put(new TopicPartition("test", 0), ByteBuffer.allocate(10));
        return new ProduceRequest((short) 1, 5000, produceData);
    }

    private AbstractRequestResponse createProduceResponse() {
        Map<TopicPartition, ProduceResponse.PartitionResponse> responseData = new HashMap<TopicPartition, ProduceResponse.PartitionResponse>();
        responseData.put(new TopicPartition("test", 0), new ProduceResponse.PartitionResponse(Errors.NONE.code(), 10000));
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

        Set<LeaderAndIsrRequest.EndPoint> leaders = new HashSet<>(Arrays.asList(
                new LeaderAndIsrRequest.EndPoint(0, "test0", 1223),
                new LeaderAndIsrRequest.EndPoint(1, "test1", 1223)
        ));

        return new LeaderAndIsrRequest(1, 10, partitionStates, leaders);
    }

    private AbstractRequestResponse createLeaderAndIsrResponse() {
        Map<TopicPartition, Short> responses = new HashMap<>();
        responses.put(new TopicPartition("test", 0), Errors.NONE.code());
        return new LeaderAndIsrResponse(Errors.NONE.code(), responses);
    }

    private AbstractRequest createUpdateMetadataRequest(int version) {
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
            Set<UpdateMetadataRequest.BrokerEndPoint> liveBrokers = new HashSet<>(Arrays.asList(
                    new UpdateMetadataRequest.BrokerEndPoint(0, "host1", 1223),
                    new UpdateMetadataRequest.BrokerEndPoint(1, "host2", 1234)
            ));

            return new UpdateMetadataRequest(1, 10, liveBrokers, partitionStates);
        } else {
            Map<SecurityProtocol, UpdateMetadataRequest.EndPoint> endPoints1 = new HashMap<>();
            endPoints1.put(SecurityProtocol.PLAINTEXT, new UpdateMetadataRequest.EndPoint("host1", 1223));

            Map<SecurityProtocol, UpdateMetadataRequest.EndPoint> endPoints2 = new HashMap<>();
            endPoints2.put(SecurityProtocol.PLAINTEXT, new UpdateMetadataRequest.EndPoint("host1", 1244));
            endPoints2.put(SecurityProtocol.SSL, new UpdateMetadataRequest.EndPoint("host2", 1234));

            Set<UpdateMetadataRequest.Broker> liveBrokers = new HashSet<>(Arrays.asList(new UpdateMetadataRequest.Broker(0, endPoints1),
                    new UpdateMetadataRequest.Broker(1, endPoints2)
            ));

            return new UpdateMetadataRequest(1, 10, partitionStates, liveBrokers);
        }
    }

    private AbstractRequestResponse createUpdateMetadataResponse() {
        return new UpdateMetadataResponse(Errors.NONE.code());
    }


}
