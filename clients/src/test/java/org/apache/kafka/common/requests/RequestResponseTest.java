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
import org.apache.kafka.common.errors.NotCoordinatorForGroupException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ProtoUtils;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RequestResponseTest {

    @Test
    public void testSerialization() throws Exception {
        checkSerialization(createRequestHeader(), null);
        checkSerialization(createResponseHeader(), null);
        checkSerialization(createGroupCoordinatorRequest());
        checkSerialization(createGroupCoordinatorRequest().getErrorResponse(new UnknownServerException()), null);
        checkSerialization(createGroupCoordinatorResponse(), null);
        checkSerialization(createControlledShutdownRequest());
        checkSerialization(createControlledShutdownResponse(), null);
        checkSerialization(createControlledShutdownRequest().getErrorResponse(new UnknownServerException()), null);
        checkSerialization(createFetchRequest(3), 3);
        checkSerialization(createFetchRequest(3).getErrorResponse(new UnknownServerException()), 3);
        checkSerialization(createFetchResponse(), null);
        checkSerialization(createHeartBeatRequest());
        checkSerialization(createHeartBeatRequest().getErrorResponse(new UnknownServerException()), null);
        checkSerialization(createHeartBeatResponse(), null);
        checkSerialization(createJoinGroupRequest(1), 1);
        checkSerialization(createJoinGroupRequest(0).getErrorResponse(new UnknownServerException()), 0);
        checkSerialization(createJoinGroupRequest(1).getErrorResponse(new UnknownServerException()), 1);
        checkSerialization(createJoinGroupResponse(), null);
        checkSerialization(createLeaveGroupRequest());
        checkSerialization(createLeaveGroupRequest().getErrorResponse(new UnknownServerException()), null);
        checkSerialization(createLeaveGroupResponse(), null);
        checkSerialization(createListGroupsRequest());
        checkSerialization(createListGroupsRequest().getErrorResponse(new UnknownServerException()), null);
        checkSerialization(createListGroupsResponse(), null);
        checkSerialization(createDescribeGroupRequest());
        checkSerialization(createDescribeGroupRequest().getErrorResponse(new UnknownServerException()), null);
        checkSerialization(createDescribeGroupResponse(), null);
        checkSerialization(createListOffsetRequest(1), 1);
        checkSerialization(createListOffsetRequest(1).getErrorResponse(new UnknownServerException()), 1);
        checkSerialization(createListOffsetResponse(1), 1);
        checkSerialization(MetadataRequest.allTopics((short) 2), 2);
        checkSerialization(createMetadataRequest(1, Arrays.asList("topic1")), 1);
        checkSerialization(createMetadataRequest(1, Arrays.asList("topic1")).getErrorResponse(new UnknownServerException()), 1);
        checkSerialization(createMetadataResponse(2), 2);
        checkSerialization(createMetadataRequest(2, Arrays.asList("topic1")).getErrorResponse(new UnknownServerException()), 2);
        checkSerialization(createOffsetCommitRequest(2), 2);
        checkSerialization(createOffsetCommitRequest(2).getErrorResponse(new UnknownServerException()), 2);
        checkSerialization(createOffsetCommitResponse(), null);
        checkSerialization(OffsetFetchRequest.forAllPartitions("group1"));
        checkSerialization(OffsetFetchRequest.forAllPartitions("group1").getErrorResponse(new NotCoordinatorForGroupException()), 2);
        checkSerialization(createOffsetFetchRequest(0));
        checkSerialization(createOffsetFetchRequest(1));
        checkSerialization(createOffsetFetchRequest(2));
        checkSerialization(OffsetFetchRequest.forAllPartitions("group1"));
        checkSerialization(createOffsetFetchRequest(0).getErrorResponse(new UnknownServerException()), 0);
        checkSerialization(createOffsetFetchRequest(1).getErrorResponse(new UnknownServerException()), 1);
        checkSerialization(createOffsetFetchRequest(2).getErrorResponse(new UnknownServerException()), 2);
        checkSerialization(createOffsetFetchResponse(), null);
        checkSerialization(createProduceRequest());
        checkSerialization(createProduceRequest().getErrorResponse(new UnknownServerException()), null);
        checkSerialization(createProduceResponse(), null);
        checkSerialization(createStopReplicaRequest(true));
        checkSerialization(createStopReplicaRequest(false));
        checkSerialization(createStopReplicaRequest(true).getErrorResponse(new UnknownServerException()), null);
        checkSerialization(createStopReplicaResponse(), null);
        checkSerialization(createLeaderAndIsrRequest());
        checkSerialization(createLeaderAndIsrRequest().getErrorResponse(new UnknownServerException()), null);
        checkSerialization(createLeaderAndIsrResponse(), null);
        checkSerialization(createSaslHandshakeRequest());
        checkSerialization(createSaslHandshakeRequest().getErrorResponse(new UnknownServerException()), null);
        checkSerialization(createSaslHandshakeResponse(), null);
        checkSerialization(createApiVersionRequest());
        checkSerialization(createApiVersionRequest().getErrorResponse(new UnknownServerException()), null);
        checkSerialization(createApiVersionResponse(), null);
        checkSerialization(createCreateTopicRequest(0), 0);
        checkSerialization(createCreateTopicRequest(0).getErrorResponse(new UnknownServerException()), 0);
        checkSerialization(createCreateTopicResponse(0), 0);
        checkSerialization(createCreateTopicRequest(1), 1);
        checkSerialization(createCreateTopicRequest(1).getErrorResponse(new UnknownServerException()), 1);
        checkSerialization(createCreateTopicResponse(1), 1);
        checkSerialization(createDeleteTopicsRequest());
        checkSerialization(createDeleteTopicsRequest().getErrorResponse(new UnknownServerException()), null);
        checkSerialization(createDeleteTopicsResponse(), null);
        checkOlderFetchVersions();
        checkSerialization(createMetadataResponse(0), 0);
        checkSerialization(createMetadataResponse(1), 1);
        checkSerialization(createMetadataRequest(1, Arrays.asList("topic1")).getErrorResponse(new UnknownServerException()), 1);
        checkSerialization(createOffsetCommitRequest(0), 0);
        checkSerialization(createOffsetCommitRequest(0).getErrorResponse(new UnknownServerException()), 0);
        checkSerialization(createOffsetCommitRequest(1), 1);
        checkSerialization(createOffsetCommitRequest(1).getErrorResponse(new UnknownServerException()), 1);
        checkSerialization(createJoinGroupRequest(0), 0);
        checkSerialization(createUpdateMetadataRequest(0, null), 0);
        checkSerialization(createUpdateMetadataRequest(0, null).getErrorResponse(new UnknownServerException()), 0);
        checkSerialization(createUpdateMetadataRequest(1, null), 1);
        checkSerialization(createUpdateMetadataRequest(1, "rack1"), 1);
        checkSerialization(createUpdateMetadataRequest(1, null).getErrorResponse(new UnknownServerException()), 1);
        checkSerialization(createUpdateMetadataRequest(2, "rack1"), 2);
        checkSerialization(createUpdateMetadataRequest(2, null), 2);
        checkSerialization(createUpdateMetadataRequest(2, "rack1").getErrorResponse(new UnknownServerException()), 2);
        checkSerialization(createUpdateMetadataRequest(3, "rack1"));
        checkSerialization(createUpdateMetadataRequest(3, null));
        checkSerialization(createUpdateMetadataRequest(3, "rack1").getErrorResponse(new UnknownServerException()), 3);
        checkSerialization(createUpdateMetadataResponse(), null);
        checkSerialization(createListOffsetRequest(0), 0);
        checkSerialization(createListOffsetRequest(0).getErrorResponse(new UnknownServerException()), 0);
        checkSerialization(createListOffsetResponse(0), 0);
    }

    private void checkOlderFetchVersions() throws Exception {
        int latestVersion = ProtoUtils.latestVersion(ApiKeys.FETCH.id);
        for (int i = 0; i < latestVersion; ++i) {
            checkSerialization(createFetchRequest(i).getErrorResponse(new UnknownServerException()), i);
            checkSerialization(createFetchRequest(i), i);
        }
    }

    private void checkSerialization(AbstractRequest req) throws Exception {
        checkSerialization(req, Integer.valueOf(req.version()));
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
        assertEquals("The original and deserialized of " + req.getClass().getSimpleName() +
                "(version " + version + ") should be the same.", req, deserialized);
        assertEquals("The original and deserialized of " + req.getClass().getSimpleName() + " should have the same hashcode.",
                req.hashCode(), deserialized.hashCode());
    }

    @Test
    public void produceResponseVersionTest() {
        Map<TopicPartition, ProduceResponse.PartitionResponse> responseData = new HashMap<>();
        responseData.put(new TopicPartition("test", 0), new ProduceResponse.PartitionResponse(Errors.NONE,
                10000, Record.NO_TIMESTAMP));
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
        LinkedHashMap<TopicPartition, FetchResponse.PartitionData> responseData = new LinkedHashMap<>();

        MemoryRecords records = MemoryRecords.readableRecords(ByteBuffer.allocate(10));
        responseData.put(new TopicPartition("test", 0), new FetchResponse.PartitionData(Errors.NONE.code(), 1000000, records));

        FetchResponse v0Response = new FetchResponse(0, responseData, 0);
        FetchResponse v1Response = new FetchResponse(1, responseData, 10);
        assertEquals("Throttle time must be zero", 0, v0Response.getThrottleTime());
        assertEquals("Throttle time must be 10", 10, v1Response.getThrottleTime());
        assertEquals("Should use schema version 0", ProtoUtils.responseSchema(ApiKeys.FETCH.id, 0), v0Response.toStruct().schema());
        assertEquals("Should use schema version 1", ProtoUtils.responseSchema(ApiKeys.FETCH.id, 1), v1Response.toStruct().schema());
        assertEquals("Response data does not match", responseData, v0Response.responseData());
        assertEquals("Response data does not match", responseData, v1Response.responseData());
    }

    @Test
    public void verifyFetchResponseFullWrite() throws Exception {
        FetchResponse fetchResponse = createFetchResponse();
        RequestHeader header = new RequestHeader(ApiKeys.FETCH.id, ProtoUtils.latestVersion(ApiKeys.FETCH.id),
                "client", 15);

        Send send = fetchResponse.toSend("1", header);
        ByteBufferChannel channel = new ByteBufferChannel(send.size());
        send.writeTo(channel);
        channel.close();

        ByteBuffer buf = channel.buf;

        // read the size
        int size = buf.getInt();
        assertTrue(size > 0);

        // read the header
        ResponseHeader responseHeader = ResponseHeader.parse(channel.buf);
        assertEquals(header.correlationId(), responseHeader.correlationId());

        // read the body
        Struct responseBody = ProtoUtils.responseSchema(ApiKeys.FETCH.id, header.apiVersion()).read(buf);
        FetchResponse parsedResponse = new FetchResponse(responseBody);
        assertEquals(parsedResponse, fetchResponse);

        assertEquals(size, responseHeader.sizeOf() + parsedResponse.sizeOf());
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

    @Test(expected = UnsupportedVersionException.class)
    public void testCreateTopicRequestV0FailsIfValidateOnly() {
        createCreateTopicRequest(0, true);
    }

    private RequestHeader createRequestHeader() {
        return new RequestHeader((short) 10, (short) 1, "", 10);
    }

    private ResponseHeader createResponseHeader() {
        return new ResponseHeader(10);
    }

    private GroupCoordinatorRequest createGroupCoordinatorRequest() {
        return new GroupCoordinatorRequest.Builder("test-group").build();
    }

    private GroupCoordinatorResponse createGroupCoordinatorResponse() {
        return new GroupCoordinatorResponse(Errors.NONE.code(), new Node(10, "host1", 2014));
    }

    private FetchRequest createFetchRequest(int version) {
        LinkedHashMap<TopicPartition, FetchRequest.PartitionData> fetchData = new LinkedHashMap<>();
        fetchData.put(new TopicPartition("test1", 0), new FetchRequest.PartitionData(100, 1000000));
        fetchData.put(new TopicPartition("test2", 0), new FetchRequest.PartitionData(200, 1000000));
        return new FetchRequest.Builder(100, 100000, fetchData).setMaxBytes(1000).
                setVersion((short) version).build();
    }

    private FetchResponse createFetchResponse() {
        LinkedHashMap<TopicPartition, FetchResponse.PartitionData> responseData = new LinkedHashMap<>();
        MemoryRecords records = MemoryRecords.readableRecords(ByteBuffer.allocate(10));
        responseData.put(new TopicPartition("test", 0), new FetchResponse.PartitionData(Errors.NONE.code(), 1000000, records));
        return new FetchResponse(responseData, 25);
    }

    private HeartbeatRequest createHeartBeatRequest() {
        return new HeartbeatRequest.Builder("group1", 1, "consumer1").build();
    }

    private HeartbeatResponse createHeartBeatResponse() {
        return new HeartbeatResponse(Errors.NONE.code());
    }

    @SuppressWarnings("deprecation")
    private JoinGroupRequest createJoinGroupRequest(int version) {
        ByteBuffer metadata = ByteBuffer.wrap(new byte[] {});
        List<JoinGroupRequest.ProtocolMetadata> protocols = new ArrayList<>();
        protocols.add(new JoinGroupRequest.ProtocolMetadata("consumer-range", metadata));
        if (version == 0) {
            return new JoinGroupRequest.Builder("group1", 30000, "consumer1", "consumer", protocols).
                    setVersion((short) version).build();
        } else {
            return new JoinGroupRequest.Builder("group1", 10000, "consumer1", "consumer", protocols).
                    setRebalanceTimeout(60000).build();
        }
    }

    private JoinGroupResponse createJoinGroupResponse() {
        Map<String, ByteBuffer> members = new HashMap<>();
        members.put("consumer1", ByteBuffer.wrap(new byte[]{}));
        members.put("consumer2", ByteBuffer.wrap(new byte[]{}));
        return new JoinGroupResponse(Errors.NONE.code(), 1, "range", "consumer1", "leader", members);
    }

    private ListGroupsRequest createListGroupsRequest() {
        return new ListGroupsRequest.Builder().build();
    }

    private ListGroupsResponse createListGroupsResponse() {
        List<ListGroupsResponse.Group> groups = Arrays.asList(new ListGroupsResponse.Group("test-group", "consumer"));
        return new ListGroupsResponse(Errors.NONE.code(), groups);
    }

    private DescribeGroupsRequest createDescribeGroupRequest() {
        return new DescribeGroupsRequest.Builder(singletonList("test-group")).build();
    }

    private DescribeGroupsResponse createDescribeGroupResponse() {
        String clientId = "consumer-1";
        String clientHost = "localhost";
        ByteBuffer empty = ByteBuffer.allocate(0);
        DescribeGroupsResponse.GroupMember member = new DescribeGroupsResponse.GroupMember("memberId",
                clientId, clientHost, empty, empty);
        DescribeGroupsResponse.GroupMetadata metadata = new DescribeGroupsResponse.GroupMetadata(Errors.NONE.code(),
                "STABLE", "consumer", "roundrobin", Arrays.asList(member));
        return new DescribeGroupsResponse(Collections.singletonMap("test-group", metadata));
    }

    private LeaveGroupRequest createLeaveGroupRequest() {
        return new LeaveGroupRequest.Builder("group1", "consumer1").build();
    }

    private LeaveGroupResponse createLeaveGroupResponse() {
        return new LeaveGroupResponse(Errors.NONE.code());
    }

    @SuppressWarnings("deprecation")
    private ListOffsetRequest createListOffsetRequest(int version) {
        if (version == 0) {
            Map<TopicPartition, ListOffsetRequest.PartitionData> offsetData = Collections.singletonMap(
                    new TopicPartition("test", 0),
                    new ListOffsetRequest.PartitionData(1000000L, 10));
            return new ListOffsetRequest.Builder().setOffsetData(offsetData).setVersion((short) version).build();
        } else if (version == 1) {
            Map<TopicPartition, Long> offsetData = Collections.singletonMap(
                    new TopicPartition("test", 0), 1000000L);
            return new ListOffsetRequest.Builder().setTargetTimes(offsetData).setVersion((short) version).build();
        } else {
            throw new IllegalArgumentException("Illegal ListOffsetRequest version " + version);
        }
    }

    @SuppressWarnings("deprecation")
    private ListOffsetResponse createListOffsetResponse(int version) {
        if (version == 0) {
            Map<TopicPartition, ListOffsetResponse.PartitionData> responseData = new HashMap<>();
            responseData.put(new TopicPartition("test", 0), new ListOffsetResponse.PartitionData(Errors.NONE.code(), Arrays.asList(100L)));
            return new ListOffsetResponse(responseData);
        } else if (version == 1) {
            Map<TopicPartition, ListOffsetResponse.PartitionData> responseData = new HashMap<>();
            responseData.put(new TopicPartition("test", 0), new ListOffsetResponse.PartitionData(Errors.NONE.code(), 10000L, 100L));
            return new ListOffsetResponse(responseData, 1);
        } else {
            throw new IllegalArgumentException("Illegal ListOffsetResponse version " + version);
        }
    }

    private MetadataRequest createMetadataRequest(int version, List<String> topics) {
        return new MetadataRequest.Builder(topics).
                setVersion((short) version).
                build();
    }

    private MetadataResponse createMetadataResponse(int version) {
        Node node = new Node(1, "host1", 1001);
        List<Node> replicas = Arrays.asList(node);
        List<Node> isr = Arrays.asList(node);

        List<MetadataResponse.TopicMetadata> allTopicMetadata = new ArrayList<>();
        allTopicMetadata.add(new MetadataResponse.TopicMetadata(Errors.NONE, "__consumer_offsets", true,
                Arrays.asList(new MetadataResponse.PartitionMetadata(Errors.NONE, 1, node, replicas, isr))));
        allTopicMetadata.add(new MetadataResponse.TopicMetadata(Errors.LEADER_NOT_AVAILABLE, "topic2", false,
                Collections.<MetadataResponse.PartitionMetadata>emptyList()));

        return new MetadataResponse(Arrays.asList(node), null, MetadataResponse.NO_CONTROLLER_ID, allTopicMetadata, version);
    }

    private OffsetCommitRequest createOffsetCommitRequest(int version) {
        Map<TopicPartition, OffsetCommitRequest.PartitionData> commitData = new HashMap<>();
        commitData.put(new TopicPartition("test", 0), new OffsetCommitRequest.PartitionData(100, ""));
        commitData.put(new TopicPartition("test", 1), new OffsetCommitRequest.PartitionData(200, null));
        return new OffsetCommitRequest.Builder("group1", commitData)
                .setGenerationId(100)
                .setMemberId("consumer1")
                .setRetentionTime(1000000)
                .setVersion((short) version)
                .build();
    }

    private OffsetCommitResponse createOffsetCommitResponse() {
        Map<TopicPartition, Short> responseData = new HashMap<>();
        responseData.put(new TopicPartition("test", 0), Errors.NONE.code());
        return new OffsetCommitResponse(responseData);
    }

    private OffsetFetchRequest createOffsetFetchRequest(int version) {
        return new OffsetFetchRequest.Builder("group1", singletonList(new TopicPartition("test11", 1)))
                .setVersion((short) version)
                .build();
    }

    private OffsetFetchResponse createOffsetFetchResponse() {
        Map<TopicPartition, OffsetFetchResponse.PartitionData> responseData = new HashMap<>();
        responseData.put(new TopicPartition("test", 0), new OffsetFetchResponse.PartitionData(100L, "", Errors.NONE));
        responseData.put(new TopicPartition("test", 1), new OffsetFetchResponse.PartitionData(100L, null, Errors.NONE));
        return new OffsetFetchResponse(Errors.NONE, responseData);
    }

    private ProduceRequest createProduceRequest() {
        Map<TopicPartition, MemoryRecords> produceData = new HashMap<>();
        produceData.put(new TopicPartition("test", 0), MemoryRecords.readableRecords(ByteBuffer.allocate(10)));
        return new ProduceRequest.Builder((short) 1, 5000, produceData).build();
    }

    private ProduceResponse createProduceResponse() {
        Map<TopicPartition, ProduceResponse.PartitionResponse> responseData = new HashMap<>();
        responseData.put(new TopicPartition("test", 0), new ProduceResponse.PartitionResponse(Errors.NONE,
                10000, Record.NO_TIMESTAMP));
        return new ProduceResponse(responseData, 0);
    }

    private StopReplicaRequest createStopReplicaRequest(boolean deletePartitions) {
        Set<TopicPartition> partitions = new HashSet<>(Arrays.asList(new TopicPartition("test", 0)));
        return new StopReplicaRequest.Builder(0, 1, deletePartitions, partitions).build();
    }

    private StopReplicaResponse createStopReplicaResponse() {
        Map<TopicPartition, Short> responses = new HashMap<>();
        responses.put(new TopicPartition("test", 0), Errors.NONE.code());
        return new StopReplicaResponse(Errors.NONE.code(), responses);
    }

    private ControlledShutdownRequest createControlledShutdownRequest() {
        return new ControlledShutdownRequest.Builder(10).build();
    }

    private ControlledShutdownResponse createControlledShutdownResponse() {
        HashSet<TopicPartition> topicPartitions = new HashSet<>(Arrays.asList(
                new TopicPartition("test2", 5),
                new TopicPartition("test1", 10)
        ));
        return new ControlledShutdownResponse(Errors.NONE.code(), topicPartitions);
    }

    private LeaderAndIsrRequest createLeaderAndIsrRequest() {
        Map<TopicPartition, PartitionState> partitionStates = new HashMap<>();
        List<Integer> isr = Arrays.asList(1, 2);
        List<Integer> replicas = Arrays.asList(1, 2, 3, 4);
        partitionStates.put(new TopicPartition("topic5", 105),
                new PartitionState(0, 2, 1, new ArrayList<>(isr), 2, new HashSet<>(replicas)));
        partitionStates.put(new TopicPartition("topic5", 1),
                new PartitionState(1, 1, 1, new ArrayList<>(isr), 2, new HashSet<>(replicas)));
        partitionStates.put(new TopicPartition("topic20", 1),
                new PartitionState(1, 0, 1, new ArrayList<>(isr), 2, new HashSet<>(replicas)));

        Set<Node> leaders = new HashSet<>(Arrays.asList(
                new Node(0, "test0", 1223),
                new Node(1, "test1", 1223)
        ));

        return new LeaderAndIsrRequest.Builder(1, 10, partitionStates, leaders).build();
    }

    private LeaderAndIsrResponse createLeaderAndIsrResponse() {
        Map<TopicPartition, Short> responses = new HashMap<>();
        responses.put(new TopicPartition("test", 0), Errors.NONE.code());
        return new LeaderAndIsrResponse(Errors.NONE.code(), responses);
    }

    private UpdateMetadataRequest createUpdateMetadataRequest(int version, String rack) {
        Map<TopicPartition, PartitionState> partitionStates = new HashMap<>();
        List<Integer> isr = Arrays.asList(1, 2);
        List<Integer> replicas = Arrays.asList(1, 2, 3, 4);
        partitionStates.put(new TopicPartition("topic5", 105),
                new PartitionState(0, 2, 1, new ArrayList<>(isr), 2, new HashSet<>(replicas)));
        partitionStates.put(new TopicPartition("topic5", 1),
                new PartitionState(1, 1, 1, new ArrayList<>(isr), 2, new HashSet<>(replicas)));
        partitionStates.put(new TopicPartition("topic20", 1),
                new PartitionState(1, 0, 1, new ArrayList<>(isr), 2, new HashSet<>(replicas)));

        SecurityProtocol plaintext = SecurityProtocol.PLAINTEXT;
        List<UpdateMetadataRequest.EndPoint> endPoints1 = new ArrayList<>();
        endPoints1.add(new UpdateMetadataRequest.EndPoint("host1", 1223, plaintext,
                ListenerName.forSecurityProtocol(plaintext)));

        List<UpdateMetadataRequest.EndPoint> endPoints2 = new ArrayList<>();
        endPoints2.add(new UpdateMetadataRequest.EndPoint("host1", 1244, plaintext,
                ListenerName.forSecurityProtocol(plaintext)));
        if (version > 0) {
            SecurityProtocol ssl = SecurityProtocol.SSL;
            endPoints2.add(new UpdateMetadataRequest.EndPoint("host2", 1234, ssl,
                    ListenerName.forSecurityProtocol(ssl)));
            endPoints2.add(new UpdateMetadataRequest.EndPoint("host2", 1334, ssl,
                    new ListenerName("CLIENT")));
        }

        Set<UpdateMetadataRequest.Broker> liveBrokers = new HashSet<>(Arrays.asList(
                new UpdateMetadataRequest.Broker(0, endPoints1, rack),
                new UpdateMetadataRequest.Broker(1, endPoints2, rack)
        ));
        return new UpdateMetadataRequest.Builder(1, 10, partitionStates, liveBrokers).
                setVersion((short) version).build();
    }

    private UpdateMetadataResponse createUpdateMetadataResponse() {
        return new UpdateMetadataResponse(Errors.NONE.code());
    }

    private SaslHandshakeRequest createSaslHandshakeRequest() {
        return new SaslHandshakeRequest("PLAIN");
    }

    private SaslHandshakeResponse createSaslHandshakeResponse() {
        return new SaslHandshakeResponse(Errors.NONE.code(), singletonList("GSSAPI"));
    }

    private ApiVersionsRequest createApiVersionRequest() {
        return new ApiVersionsRequest.Builder().build();
    }

    private ApiVersionsResponse createApiVersionResponse() {
        List<ApiVersionsResponse.ApiVersion> apiVersions = Arrays.asList(new ApiVersionsResponse.ApiVersion((short) 0, (short) 0, (short) 2));
        return new ApiVersionsResponse(Errors.NONE.code(), apiVersions);
    }

    private CreateTopicsRequest createCreateTopicRequest(int version) {
        return createCreateTopicRequest(version, version >= 1);
    }

    private CreateTopicsRequest createCreateTopicRequest(int version, boolean validateOnly) {
        CreateTopicsRequest.TopicDetails request1 = new CreateTopicsRequest.TopicDetails(3, (short) 5);

        Map<Integer, List<Integer>> replicaAssignments = new HashMap<>();
        replicaAssignments.put(1, Arrays.asList(1, 2, 3));
        replicaAssignments.put(2, Arrays.asList(2, 3, 4));

        Map<String, String> configs = new HashMap<>();
        configs.put("config1", "value1");

        CreateTopicsRequest.TopicDetails request2 = new CreateTopicsRequest.TopicDetails(replicaAssignments, configs);

        Map<String, CreateTopicsRequest.TopicDetails> request = new HashMap<>();
        request.put("my_t1", request1);
        request.put("my_t2", request2);
        return new CreateTopicsRequest.Builder(request, 0, validateOnly).setVersion((short) version).build();
    }

    private CreateTopicsResponse createCreateTopicResponse(int version) {
        Map<String, CreateTopicsResponse.Error> errors = new HashMap<>();
        errors.put("t1", new CreateTopicsResponse.Error(Errors.INVALID_TOPIC_EXCEPTION, null));
        errors.put("t2", new CreateTopicsResponse.Error(Errors.LEADER_NOT_AVAILABLE, "Leader with id 5 is not available."));
        return new CreateTopicsResponse(errors, (short) version);
    }

    private DeleteTopicsRequest createDeleteTopicsRequest() {
        return new DeleteTopicsRequest.Builder(new HashSet<>(Arrays.asList("my_t1", "my_t2")), 10000).
                build();
    }

    private DeleteTopicsResponse createDeleteTopicsResponse() {
        Map<String, Errors> errors = new HashMap<>();
        errors.put("t1", Errors.INVALID_TOPIC_EXCEPTION);
        errors.put("t2", Errors.TOPIC_AUTHORIZATION_FAILED);
        return new DeleteTopicsResponse(errors);
    }

    private static class ByteBufferChannel implements GatheringByteChannel {
        private final ByteBuffer buf;
        private boolean closed = false;

        private ByteBufferChannel(long size) {
            if (size > Integer.MAX_VALUE)
                throw new IllegalArgumentException("size should be not be greater than Integer.MAX_VALUE");
            this.buf = ByteBuffer.allocate((int) size);
        }

        @Override
        public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
            int position = buf.position();
            for (int i = 0; i < length; i++) {
                ByteBuffer src = srcs[i].duplicate();
                if (i == 0)
                    src.position(offset);
                buf.put(src);
            }
            return buf.position() - position;
        }

        @Override
        public long write(ByteBuffer[] srcs) throws IOException {
            return write(srcs, 0, srcs.length);
        }

        @Override
        public int write(ByteBuffer src) throws IOException {
            int position = buf.position();
            buf.put(src);
            return buf.position() - position;
        }

        @Override
        public boolean isOpen() {
            return !closed;
        }

        @Override
        public void close() throws IOException {
            buf.flip();
            closed = true;
        }
    }
}
