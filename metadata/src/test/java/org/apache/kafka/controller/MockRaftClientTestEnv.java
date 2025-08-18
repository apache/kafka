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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.controller.MockRaftClient.LeaderChangeBatch;
import org.apache.kafka.controller.MockRaftClient.LocalRecordBatch;
import org.apache.kafka.controller.MockRaftClient.SharedLogData;
import org.apache.kafka.raft.LeaderAndEpoch;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.KRaftVersion;
import org.apache.kafka.snapshot.RawSnapshotReader;
import org.apache.kafka.test.TestUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class MockRaftClientTestEnv implements AutoCloseable {
    private static final Logger log =
        LoggerFactory.getLogger(MockRaftClientTestEnv.class);

    private final String clusterId;

    /**
     * The first error we encountered during this test, or the empty string if we have
     * not encountered any.
     */
    final AtomicReference<String> firstError = new AtomicReference<>(null);

    /**
     * The test directory, which we will delete once the test is over.
     */
    private final File dir;

    /**
     * The shared data for our MockRaftClient instances.
     */
    private final SharedLogData shared;

    /**
     * A list of raft clients.
     */
    private final List<MockRaftClient> raftClients;

    public static class Builder {
        private final int numNodes;
        private Optional<RawSnapshotReader> snapshotReader = Optional.empty();
        private Consumer<SharedLogData> sharedLogDataInitializer = __ -> { };
        private KRaftVersion lastKRaftVersion = KRaftVersion.KRAFT_VERSION_0;

        public Builder(int numNodes) {
            this.numNodes = numNodes;
        }

        public Builder setSnapshotReader(RawSnapshotReader snapshotReader) {
            this.snapshotReader = Optional.of(snapshotReader);
            return this;
        }

        public Builder setSharedLogDataInitializer(Consumer<SharedLogData> sharedLogDataInitializer) {
            this.sharedLogDataInitializer = sharedLogDataInitializer;
            return this;
        }

        /**
         * Used to mock the latest KRaft version that would be returned from RaftClient.kraftVersion()
         */
        public Builder setLastKRaftVersion(KRaftVersion kraftVersion) {
            this.lastKRaftVersion = kraftVersion;
            return this;
        }

        public MockRaftClientTestEnv build() {
            return new MockRaftClientTestEnv(
                numNodes,
                snapshotReader,
                sharedLogDataInitializer,
                lastKRaftVersion);
        }

        public MockRaftClientTestEnv buildWithMockListeners() {
            MockRaftClientTestEnv env = build();
            try {
                for (MockRaftClient raftClient : env.raftClients) {
                    raftClient.register(new MockRaftClientListener(raftClient.nodeId().getAsInt()));
                }
            } catch (Exception e) {
                try {
                    env.close();
                } catch (Exception t) {
                    log.error("Error while closing new log environment", t);
                }
                throw e;
            }
            return env;
        }
    }

    private MockRaftClientTestEnv(
        int numNodes,
        Optional<RawSnapshotReader> snapshotReader,
        Consumer<SharedLogData> sharedLogDataInitializer,
        KRaftVersion lastKRaftVersion
    ) {
        clusterId = Uuid.randomUuid().toString();
        dir = TestUtils.tempDirectory();
        shared = new SharedLogData(snapshotReader);
        sharedLogDataInitializer.accept(shared);
        List<MockRaftClient> newRaftClients = new ArrayList<>(numNodes);
        try {
            for (int nodeId = 0; nodeId < numNodes; nodeId++) {
                newRaftClients.add(new MockRaftClient(
                    new LogContext(String.format("[MockRaftClient %d] ", nodeId)),
                    nodeId,
                    shared,
                    String.format("MockRaftClient-%d_", nodeId),
                    lastKRaftVersion));
            }
        } catch (Throwable t) {
            for (MockRaftClient raftClient : newRaftClients) {
                raftClient.close();
            }
            throw t;
        }
        this.raftClients = newRaftClients;
    }

    /**
     * Return all records in the log as a list.
     */
    public List<ApiMessageAndVersion> allRecords() {
        return shared.allRecords();
    }

    /**
     * Append some records to the log. This method is meant to be called before the
     * controllers are started, to simulate a pre-existing metadata log.
     *
     * @param records   The records to be appended. Will be added in a single batch.
     */
    public void appendInitialRecords(List<ApiMessageAndVersion> records) {
        int initialLeaderEpoch = 1;
        shared.append(new LeaderChangeBatch(new LeaderAndEpoch(OptionalInt.empty(), initialLeaderEpoch + 1)));
        shared.append(new LocalRecordBatch(initialLeaderEpoch + 1, 0, records));
        shared.append(new LeaderChangeBatch(new LeaderAndEpoch(OptionalInt.of(0), initialLeaderEpoch + 2)));
    }

    public String clusterId() {
        return clusterId;
    }

    LeaderAndEpoch waitForLeader() throws InterruptedException {
        AtomicReference<LeaderAndEpoch> value = new AtomicReference<>(null);
        TestUtils.retryOnExceptionWithTimeout(20000, 3, () -> {
            LeaderAndEpoch result = null;
            for (MockRaftClient raftClient : raftClients) {
                LeaderAndEpoch leader = raftClient.leaderAndEpoch();
                int nodeId = raftClient.nodeId().getAsInt();
                if (leader.isLeader(nodeId)) {
                    if (result != null) {
                        throw new RuntimeException("node " + nodeId +
                            " thinks it's the leader, but so does " + result.leaderId());
                    }
                    result = leader;
                }
            }
            if (result == null) {
                throw new RuntimeException("No leader found.");
            }
            value.set(result);
        });
        return value.get();
    }

    public List<MockRaftClient> raftClients() {
        return raftClients;
    }

    public Optional<MockRaftClient> activeRaftClient() {
        OptionalInt leader = shared.leaderAndEpoch().leaderId();
        if (leader.isPresent()) {
            return Optional.of(raftClients.get(leader.getAsInt()));
        } else {
            return Optional.empty();
        }
    }

    public LeaderAndEpoch leaderAndEpoch() {
        return shared.leaderAndEpoch();
    }

    @Override
    public void close() throws InterruptedException {
        try {
            for (MockRaftClient raftClient : raftClients) {
                raftClient.beginShutdown();
            }
            for (MockRaftClient raftClient : raftClients) {
                raftClient.close();
            }
            Utils.delete(dir);
        } catch (IOException e) {
            log.error("Error deleting {}", dir.getAbsolutePath(), e);
        }
    }
}
