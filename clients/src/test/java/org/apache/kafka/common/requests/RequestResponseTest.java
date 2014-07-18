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
    public void testSerialization() throws Exception{
        List<AbstractRequestResponse> requestList = Arrays.asList(
                createRequestHeader(),
                createResponseHeader(),
                createConsumerMetadataRequest(),
                createConsumerMetadataResponse(),
                createFetchRequest(),
                createFetchResponse(),
                createHeartBeatRequest(),
                createHeartBeatResponse(),
                createJoinGroupRequest(),
                createJoinGroupResponse(),
                createListOffsetRequest(),
                createListOffsetResponse(),
                createMetadataRequest(),
                createMetadataResponse(),
                createOffsetCommitRequest(),
                createOffsetCommitResponse(),
                createOffsetFetchRequest(),
                createOffsetFetchResponse(),
                createProduceRequest(),
                createProduceResponse());

        for (AbstractRequestResponse req: requestList) {
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
        return new RequestHeader((short)10, (short)1, "", 10);
    }

    private AbstractRequestResponse createResponseHeader() {
        return new ResponseHeader(10);
    }

    private AbstractRequestResponse createConsumerMetadataRequest() {
        return new ConsumerMetadataRequest("test-group");
    }

    private AbstractRequestResponse createConsumerMetadataResponse() {
        return new ConsumerMetadataResponse((short)1, new Node(10, "host1", 2014));
    }

    private AbstractRequestResponse createFetchRequest() {
        Map<TopicPartition, FetchRequest.PartitionData> fetchData = new HashMap<TopicPartition, FetchRequest.PartitionData>();
        fetchData.put(new TopicPartition("test1", 0), new FetchRequest.PartitionData(100, 1000000));
        fetchData.put(new TopicPartition("test2", 0), new FetchRequest.PartitionData(200, 1000000));
        return new FetchRequest(-1, 100, 100000, fetchData);
    }

    private AbstractRequestResponse createFetchResponse() {
        Map<TopicPartition, FetchResponse.PartitionData> responseData = new HashMap<TopicPartition, FetchResponse.PartitionData>();
        responseData.put(new TopicPartition("test", 0), new FetchResponse.PartitionData((short)0, 1000000, ByteBuffer.allocate(10)));
        return new FetchResponse(responseData);
    }

    private AbstractRequestResponse createHeartBeatRequest() {
        return new HeartbeatRequest("group1", 1, "consumer1");
    }

    private AbstractRequestResponse createHeartBeatResponse() {
        return new HeartbeatResponse((short)0);
    }

    private AbstractRequestResponse createJoinGroupRequest() {
        return new JoinGroupRequest("group1", 30000, Arrays.asList("topic1"), "consumer1", "strategy1");
    }

    private AbstractRequestResponse createJoinGroupResponse() {
        return new JoinGroupResponse((short)0, 1, "consumer1", Arrays.asList(new TopicPartition("test11", 1), new TopicPartition("test2", 1)));
    }

    private AbstractRequestResponse createListOffsetRequest() {
        Map<TopicPartition, ListOffsetRequest.PartitionData> offsetData = new HashMap<TopicPartition, ListOffsetRequest.PartitionData>();
        offsetData.put(new TopicPartition("test", 0), new ListOffsetRequest.PartitionData(1000000L, 10));
        return new ListOffsetRequest(-1, offsetData);
    }

    private AbstractRequestResponse createListOffsetResponse() {
        Map<TopicPartition, ListOffsetResponse.PartitionData> responseData = new HashMap<TopicPartition, ListOffsetResponse.PartitionData>();
        responseData.put(new TopicPartition("test", 0), new ListOffsetResponse.PartitionData((short)0, Arrays.asList(100L)));
        return new ListOffsetResponse(responseData);
    }

    private AbstractRequestResponse createMetadataRequest() {
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

    private AbstractRequestResponse createOffsetCommitRequest() {
        Map<TopicPartition, OffsetCommitRequest.PartitionData> commitData = new HashMap<TopicPartition, OffsetCommitRequest.PartitionData>();
        commitData.put(new TopicPartition("test", 0), new OffsetCommitRequest.PartitionData(100, 1000000, ""));
        return new OffsetCommitRequest("group1", 100, "consumer1", commitData);
    }

    private AbstractRequestResponse createOffsetCommitResponse() {
        Map<TopicPartition, Short> responseData = new HashMap<TopicPartition, Short>();
        responseData.put(new TopicPartition("test", 0), (short)0);
        return new OffsetCommitResponse(responseData);
    }

    private AbstractRequestResponse createOffsetFetchRequest() {
        return new OffsetFetchRequest("group1", Arrays.asList(new TopicPartition("test11", 1)));
    }

    private AbstractRequestResponse createOffsetFetchResponse() {
        Map<TopicPartition, OffsetFetchResponse.PartitionData> responseData = new HashMap<TopicPartition, OffsetFetchResponse.PartitionData>();
        responseData.put(new TopicPartition("test", 0), new OffsetFetchResponse.PartitionData(100L, "", (short)0));
        return new OffsetFetchResponse(responseData);
    }

    private AbstractRequestResponse createProduceRequest() {
        Map<TopicPartition, ByteBuffer> produceData = new HashMap<TopicPartition, ByteBuffer>();
        produceData.put(new TopicPartition("test", 0), ByteBuffer.allocate(10));
        return new ProduceRequest((short)0, 5000, produceData);
    }

    private AbstractRequestResponse createProduceResponse() {
        Map<TopicPartition, ProduceResponse.PartitionResponse> responseData = new HashMap<TopicPartition, ProduceResponse.PartitionResponse>();
        responseData.put(new TopicPartition("test", 0), new ProduceResponse.PartitionResponse((short) 0, 10000));
        return new ProduceResponse(responseData);
    }
}
