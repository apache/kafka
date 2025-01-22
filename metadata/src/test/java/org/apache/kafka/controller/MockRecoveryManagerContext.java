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
package org.apache.kafka.controller;

import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.TopicIdPartition;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MockRecoveryManagerContext {
    static final String MOCK_LISTENER_NAME = "mock-listener";
    private static final long TIMEOUT_MS = 3000;
    private static final int NODE_ID = 0;
    static final String MOCK_HOST = "localhost";
    static final int MOCK_PORT = 9092;

    static class MockQueueEntry {
        final String tag;
        final int insertCount;
        final Supplier<ControllerResult<Void>> op;

        MockQueueEntry(String tag, int insertCount, Supplier<ControllerResult<Void>> op) {
            this.tag = tag;
            this.insertCount = insertCount;
            this.op = op;
        }

        void check(String expectedTag, int expectedInsertionCount, Class<? extends Supplier<ControllerResult<Void>>> expectedOp) {
            assertEquals(expectedTag, tag);
            assertEquals(expectedInsertionCount, insertCount);
            assertEquals(expectedOp, op.getClass());
        }
    }

    static class MockQueueAccessor implements RecoveryManager.QueueAccessor {
        LinkedList<MockQueueEntry> deferred = new LinkedList<>();
        LinkedList<MockQueueEntry> immediate = new LinkedList<>();
        private int insertCount = 0;

        @Override
        public void scheduleDeferred(String tag, long timeFromNowNs, Supplier<ControllerResult<Void>> op) {
            deferred.addLast(new MockQueueEntry(tag, insertCount, op));
            insertCount++;
        }

        @Override
        public void enqueueWriteOp(String name, Supplier<ControllerResult<Void>> op) {
            immediate.addLast(new MockQueueEntry(name, insertCount, op));
            insertCount++;
        }

        public void emptyQueues() {
            deferred.clear();
            immediate.clear();
        }
    }

    static class MockElectionRequest {
        final List<TopicIdPartition> topicPartitions;
        final LogLengthInfoStore store;
        final List<ApiMessageAndVersion> records;

        MockElectionRequest(List<TopicIdPartition> topicPartitions, LogLengthInfoStore store, List<ApiMessageAndVersion> records) {
            this.topicPartitions = topicPartitions;
            this.store = store;
            this.records = records;
        }
    }

    static class MockReplicationFacade implements RecoveryManager.ReplicationFacade {
        List<ApiError> nextElectionResults = new ArrayList<>();
        List<MockElectionRequest> electionRequests = new ArrayList<>();

        @Override
        public List<ApiError> electLeadersWithLogInfo(List<TopicIdPartition> readyPartitions, LogLengthInfoStore store, List<ApiMessageAndVersion> records) {
            assertEquals(nextElectionResults.size(), readyPartitions.size());
            electionRequests.add(new MockElectionRequest(readyPartitions, store, records));
            return nextElectionResults;
        }
    }

    static class MockRequestThread implements RecoveryFetcher.Sender {
        public List<RecoveryRequestThread.RequestAndReceiver> work = new ArrayList<>();

        @Override
        public void enqueueRequest(RecoveryFetcher.Receiver receiver, RecoveryFetcher.Request request) {
            work.add(new RecoveryRequestThread.RequestAndReceiver(receiver, request));
        }

        public void emptyQueues() {
            work.clear();
        }
    }

    MockQueueAccessor queueAccessor;
    MockReplicationFacade replication;
    MockRequestThread requestThread;

    MockRecoveryManagerContext() {
        queueAccessor = new MockQueueAccessor();
        replication = new MockReplicationFacade();
        requestThread = new MockRequestThread();
    }

    RecoveryManager.Builder createBuilder() {
        return new RecoveryManager.Builder().
                setQueueAccessor(this.queueAccessor).
                setReplicationControlManager(this.replication).
                setRecoveryFetcherSender(this.requestThread).
                setTime(new MockTime()).
                setEnabled(true).
                setTimeout(TIMEOUT_MS).
                setInterbrokerListenerName(MOCK_LISTENER_NAME).
                setNodeId(NODE_ID);
    }

    RecoveryManager createRecoveryManager() {
        return createBuilder().build();
    }
}
