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
import org.apache.kafka.common.protocol.Errors;
import org.junit.Test;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class RequestResponseTest {

    @Test
    public void testSerialization() throws Exception {
        List<AbstractRequestResponse> requestResponseList = Arrays.asList(
                createRequestHeader(),
                createResponseHeader(),
                createConsumerMetadataRequest(),
                createConsumerMetadataRequest().getErrorResponse(new UnknownServerException()),
                createConsumerMetadataResponse(),
                createFetchRequest(),
                createFetchRequest().getErrorResponse(new UnknownServerException()),
                createFetchResponse(),
                createHeartBeatRequest(),
                createHeartBeatRequest().getErrorResponse(new UnknownServerException()),
                createHeartBeatResponse(),
                createJoinGroupRequest(),
                createJoinGroupRequest().getErrorResponse(new UnknownServerException()),
                createJoinGroupResponse(),
                createListOffsetRequest(),
                createListOffsetRequest().getErrorResponse(new UnknownServerException()),
                createListOffsetResponse(),
                createMetadataRequest(),
                createMetadataRequest().getErrorResponse(new UnknownServerException()),
                createMetadataResponse(),
                createOffsetCommitRequest(),
                createOffsetCommitRequest().getErrorResponse(new UnknownServerException()),
                createOffsetCommitResponse(),
                createOffsetFetchRequest(),
                createOffsetFetchRequest().getErrorResponse(new UnknownServerException()),
                createOffsetFetchResponse(),
                createProduceRequest(),
                createProduceRequest().getErrorResponse(new UnknownServerException()),
                createProduceResponse());

        for (AbstractRequestResponse req: requestResponseList) {
            ByteBuffer buffer = ByteBuffer.allocate(req.sizeOf());
            req.writeTo(buffer);
            buffer.rewind();
            Method deserializer = req.getClass().getDeclaredMethod("parse", ByteBuffer.class);
            AbstractRequestResponse deserialized = (AbstractRequestResponse) deserializer.invoke(null, buffer);
            assertEquals("The original and deserialized of " + req.getClass().getSimpleName() + " should be the same.", req, deserialized);
            assertEquals("The original and deserialized of " + req.getClass().getSimpleName() + " should have the same hashcode.",
                    req.hashCode(), deserialized.hashCode());
        }
    }

    private AbstractRequestResponse createRequestHeader() {
        return new RequestHeader((short) 10, (short) 1, "", 10);
    }

    private AbstractRequestResponse createResponseHeader() {
        return new ResponseHeader(10);
    }

    private AbstractRequest createConsumerMetadataRequest() {
        return new ConsumerMetadataRequest("test-group");
    }

    private AbstractRequestResponse createConsumerMetadataResponse() {
        return new ConsumerMetadataResponse((short) 1, new Node(10, "host1", 2014));
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
        return new FetchResponse(responseData);
    }

    private AbstractRequest createHeartBeatRequest() {
        return new HeartbeatRequest("group1", 1, "consumer1");
    }

    private AbstractRequestResponse createHeartBeatResponse() {
        return new HeartbeatResponse(Errors.NONE.code());
    }

    private AbstractRequest createJoinGroupRequest() {
        return new JoinGroupRequest("group1", 30000, Arrays.asList("topic1"), "consumer1", "strategy1");
    }

    private AbstractRequestResponse createJoinGroupResponse() {
        return new JoinGroupResponse(Errors.NONE.code(), 1, "consumer1", Arrays.asList(new TopicPartition("test11", 1), new TopicPartition("test2", 1)));
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
        Cluster cluster = new Cluster(Arrays.asList(node), Arrays.asList(new PartitionInfo("topic1", 1, node, replicas, isr)));
        return new MetadataResponse(cluster);
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
        return new ProduceResponse(responseData);
    }
}
