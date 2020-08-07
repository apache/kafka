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

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.apache.kafka.raft.KafkaRaftClientTest.METADATA_PARTITION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SimpleKeyValueStoreTest {

    private int epoch = 2;

    private KafkaRaftClient setupSingleNodeRaftManager() throws IOException {
        int localId = 1;
        int electionTimeoutMs = 1000;
        int electionJitterMs = 50;
        int fetchTimeoutMs = 5000;
        int retryBackoffMs = 100;
        int requestTimeoutMs = 5000;
        int fetchMaxWaitMs = 500;
        Set<Integer> voters = Collections.singleton(localId);
        QuorumStateStore store = new MockQuorumStateStore();

        MockTime time = new MockTime();
        MockFuturePurgatory<Long> fetchPurgatory = new MockFuturePurgatory<>(time);
        MockFuturePurgatory<Long> appendPurgatory = new MockFuturePurgatory<>(time);

        store.writeElectionState(ElectionState.withElectedLeader(epoch, localId, Collections.singleton(localId)));

        ReplicatedLog log = new MockLog(METADATA_PARTITION);
        NetworkChannel channel = new MockNetworkChannel();
        LogContext logContext = new LogContext();
        QuorumState quorum = new QuorumState(localId, voters, store, logContext);

        List<InetSocketAddress> bootstrapServers = voters.stream()
            .map(id -> new InetSocketAddress("localhost", 9990 + id))
            .collect(Collectors.toList());

        return new KafkaRaftClient(channel, log, quorum, time, new Metrics(time), fetchPurgatory, appendPurgatory,
            new InetSocketAddress("localhost", 9990 + localId), bootstrapServers,
            electionTimeoutMs, electionJitterMs, fetchTimeoutMs, retryBackoffMs, requestTimeoutMs,
            fetchMaxWaitMs, logContext, new Random());
    }

    @Test
    public void testPutAndGet() throws Exception {
        KafkaRaftClient client = setupSingleNodeRaftManager();
        SimpleKeyValueStore<Integer, Integer> store = new SimpleKeyValueStore<>(
            new Serdes.IntegerSerde(), new Serdes.IntegerSerde());
        client.initialize(store);

        CompletableFuture<OffsetAndEpoch> future = store.put(0, 1);
        client.poll();

        assertTrue(future.isDone());
        // The control record takes up one offset.
        assertEquals(new OffsetAndEpoch(1L, epoch), future.get());
        assertEquals(1, store.get(0).intValue());
    }
}
