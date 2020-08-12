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
package org.apache.kafka.raft;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.FetchSnapshotRequestData;
import org.apache.kafka.common.message.FetchSnapshotResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.FetchSnapshotRequest;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.snapshot.SnapshotWriter;
import org.apache.kafka.snapshot.RawSnapshotReader;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

final public class KafkaRaftClientSnapshotTest {
    @Test
    public void testMissingFetchSnapshotRequest() throws Exception {
        int localId = 0;
        int epoch = 2;
        Set<Integer> voters = Utils.mkSet(localId, localId + 1);

        RaftClientTestContext context = RaftClientTestContext.initializeAsLeader(localId, voters, epoch);

        context.deliverRequest(fetchSnapshotRequest(context.metadataPartition, new OffsetAndEpoch(0, 0), Integer.MAX_VALUE, 0));

        context.client.poll();

        FetchSnapshotResponseData.PartitionSnapshot response =  context.assertSentFetchSnapshotResponse(context.metadataPartition).get();
        assertEquals(Errors.SNAPSHOT_NOT_FOUND, Errors.forCode(response.errorCode()));
    }

    @Test
    public void testUnknownFetchSnapshotRequest() throws Exception {
        int localId = 0;
        Set<Integer> voters = Utils.mkSet(localId, localId + 1);
        int epoch = 2;
        TopicPartition topicPartition = new TopicPartition("unknown", 0);

        RaftClientTestContext context = RaftClientTestContext.initializeAsLeader(localId, voters, epoch);

        context.deliverRequest(fetchSnapshotRequest(topicPartition, new OffsetAndEpoch(0, 0), Integer.MAX_VALUE, 0));

        context.client.poll();

        FetchSnapshotResponseData.PartitionSnapshot response =  context.assertSentFetchSnapshotResponse(topicPartition).get();
        assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION, Errors.forCode(response.errorCode()));
    }

    @Test
    public void testFetchSnapshotRequestAsLeader() throws Exception {
        int localId = 0;
        Set<Integer> voters = Utils.mkSet(localId, localId + 1);
        int epoch = 2;
        OffsetAndEpoch snapshotId = new OffsetAndEpoch(0, 0);
        List<String> records = Arrays.asList("foo", "bar");

        RaftClientTestContext context = RaftClientTestContext.initializeAsLeader(localId, voters, epoch);

        try (SnapshotWriter<String> snapshot = context.client.createSnapshot(snapshotId)) {
            snapshot.append(records);
            snapshot.freeze();
        }

        try (RawSnapshotReader snapshot = context.log.readSnapshot(snapshotId).get()) {
            context.deliverRequest(fetchSnapshotRequest(context.metadataPartition, snapshotId, Integer.MAX_VALUE, 0));

            context.client.poll();

            FetchSnapshotResponseData.PartitionSnapshot response =  context
                .assertSentFetchSnapshotResponse(context.metadataPartition)
                .get();

            assertEquals(Errors.NONE, Errors.forCode(response.errorCode()));
            assertEquals(snapshot.sizeInBytes(), response.size());
            assertEquals(0, response.position());
            assertEquals(snapshot.sizeInBytes(), response.bytes().remaining());

            ByteBuffer buffer = ByteBuffer.allocate((int) snapshot.sizeInBytes());
            snapshot.read(buffer, 0);
            buffer.flip();

            assertEquals(buffer.slice(), response.bytes());
        }
    }

    @Test
    public void testPartialFetchSnapshotRequestAsLeader() throws Exception {
        int localId = 0;
        Set<Integer> voters = Utils.mkSet(localId, localId + 1);
        int epoch = 2;
        OffsetAndEpoch snapshotId = new OffsetAndEpoch(0, 0);
        List<String> records = Arrays.asList("foo", "bar");

        RaftClientTestContext context = RaftClientTestContext.initializeAsLeader(localId, voters, epoch);

        try (SnapshotWriter<String> snapshot = context.client.createSnapshot(snapshotId)) {
            snapshot.append(records);
            snapshot.freeze();
        }

        try (RawSnapshotReader snapshot = context.log.readSnapshot(snapshotId).get()) {
            // Fetch half of the snapshot
            context.deliverRequest(fetchSnapshotRequest(context.metadataPartition, snapshotId, (int) snapshot.sizeInBytes() / 2, 0));

            context.client.poll();

            FetchSnapshotResponseData.PartitionSnapshot response = context.assertSentFetchSnapshotResponse(context.metadataPartition).get();

            assertEquals(Errors.NONE, Errors.forCode(response.errorCode()));
            assertEquals(snapshot.sizeInBytes(), response.size());
            assertEquals(0, response.position());
            assertEquals(snapshot.sizeInBytes() / 2, response.bytes().remaining());

            ByteBuffer snapshotBuffer = ByteBuffer.allocate((int) snapshot.sizeInBytes());
            snapshot.read(snapshotBuffer, 0);
            snapshotBuffer.flip();

            ByteBuffer responseBuffer = ByteBuffer.allocate((int) snapshot.sizeInBytes());
            responseBuffer.put(response.bytes());

            ByteBuffer expectedBytes = snapshotBuffer.duplicate();
            expectedBytes.limit((int) snapshot.sizeInBytes() / 2);

            assertEquals(expectedBytes, responseBuffer.duplicate().flip());

            // Fetch the remainder of the snapshot
            context.deliverRequest(
                fetchSnapshotRequest(context.metadataPartition, snapshotId, Integer.MAX_VALUE, responseBuffer.position())
            );

            context.client.poll();

            response = context.assertSentFetchSnapshotResponse(context.metadataPartition).get();
            assertEquals(Errors.NONE, Errors.forCode(response.errorCode()));
            assertEquals(snapshot.sizeInBytes(), response.size());
            assertEquals(responseBuffer.position(), response.position());
            assertEquals(snapshot.sizeInBytes() - (snapshot.sizeInBytes() / 2), response.bytes().remaining());

            responseBuffer.put(response.bytes());
            assertEquals(snapshotBuffer, responseBuffer.flip());
        }
    }

    @Test
    public void testFetchSnapshotRequestAsFollower() throws IOException {
        int localId = 0;
        int leaderId = localId + 1;
        Set<Integer> voters = Utils.mkSet(localId, leaderId);
        int epoch = 2;
        OffsetAndEpoch snapshotId = new OffsetAndEpoch(0, 0);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withElectedLeader(epoch, leaderId)
            .build();

        context.deliverRequest(fetchSnapshotRequest(context.metadataPartition, snapshotId, Integer.MAX_VALUE, 0));

        context.client.poll();

        FetchSnapshotResponseData.PartitionSnapshot response =  context.assertSentFetchSnapshotResponse(context.metadataPartition).get();
        assertEquals(Errors.NONE, Errors.forCode(response.errorCode()));
        assertEquals(epoch, response.currentLeader().leaderEpoch());
        assertEquals(leaderId, response.currentLeader().leaderId());
    }

    private static FetchSnapshotRequestData fetchSnapshotRequest(
        TopicPartition topicPartition,
        OffsetAndEpoch offsetAndEpoch,
        int maxBytes,
        long position
    ) {
        FetchSnapshotRequestData.SnapshotId snapshotId = new FetchSnapshotRequestData.SnapshotId()
            .setEndOffset(offsetAndEpoch.offset)
            .setEpoch(offsetAndEpoch.epoch);
        return FetchSnapshotRequest.singleton(topicPartition, snapshotId, maxBytes, position);
    }
}
