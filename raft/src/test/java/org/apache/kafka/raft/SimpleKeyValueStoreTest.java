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

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SimpleKeyValueStoreTest {

    private KafkaRaftClient setupSingleNodeRaftManager() {
        int localId = 1;
        int electionTimeoutMs = 1000;
        int electionJitterMs = 50;
        int fetchTimeoutMs = 5000;
        int retryBackoffMs = 100;
        int requestTimeoutMs = 5000;
        Set<Integer> voters = Collections.singleton(localId);
        QuorumStateStore store = new MockQuorumStateStore();
        Time time = new MockTime();
        ReplicatedLog log = new MockLog();
        NetworkChannel channel = new MockNetworkChannel();
        LogContext logContext = new LogContext();
        QuorumState quorum = new QuorumState(localId, voters, store, logContext);

        List<InetSocketAddress> bootstrapServers = voters.stream()
            .map(id -> new InetSocketAddress("localhost", 9990 + id))
            .collect(Collectors.toList());

        return new KafkaRaftClient(channel, log, quorum, time,
            new InetSocketAddress("localhost", 9990 + localId), bootstrapServers,
            electionTimeoutMs, electionJitterMs, fetchTimeoutMs, retryBackoffMs, requestTimeoutMs,
            logContext, new Random());
    }

    @Test
    public void testPutAndGet() throws Exception {
        KafkaRaftClient manager = setupSingleNodeRaftManager();
        manager.initialize(new NoOpStateMachine());
        SimpleKeyValueStore<Integer, Integer> store = new SimpleKeyValueStore<>(manager,
            new Serdes.IntegerSerde(), new Serdes.IntegerSerde());
        store.initialize();

        CompletableFuture<OffsetAndEpoch> future = store.put(0, 1);
        manager.poll();

        assertTrue(future.isDone());
        // The control record takes up one offset.
        assertEquals(new OffsetAndEpoch(2L, 1), future.get());
        assertEquals(1, store.get(0).intValue());
    }
}
