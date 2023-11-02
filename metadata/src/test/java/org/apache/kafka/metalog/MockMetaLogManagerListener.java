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

package org.apache.kafka.metalog;

import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.raft.Batch;
import org.apache.kafka.raft.BatchReader;
import org.apache.kafka.raft.LeaderAndEpoch;
import org.apache.kafka.raft.RaftClient;
import org.apache.kafka.snapshot.SnapshotReader;
import org.apache.kafka.server.common.ApiMessageAndVersion;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;

public class MockMetaLogManagerListener implements RaftClient.Listener<ApiMessageAndVersion> {
    public static final String COMMIT = "COMMIT";
    public static final String LAST_COMMITTED_OFFSET = "LAST_COMMITTED_OFFSET";
    public static final String NEW_LEADER = "NEW_LEADER";
    public static final String RENOUNCE = "RENOUNCE";
    public static final String SHUTDOWN = "SHUTDOWN";
    public static final String SNAPSHOT = "SNAPSHOT";

    private final int nodeId;
    private final List<String> serializedEvents = new ArrayList<>();
    private LeaderAndEpoch leaderAndEpoch = new LeaderAndEpoch(OptionalInt.empty(), 0);

    public MockMetaLogManagerListener(int nodeId) {
        this.nodeId = nodeId;
    }

    @Override
    public synchronized void handleCommit(BatchReader<ApiMessageAndVersion> reader) {
        try {
            while (reader.hasNext()) {
                Batch<ApiMessageAndVersion> batch = reader.next();
                long lastCommittedOffset = batch.lastOffset();

                for (ApiMessageAndVersion messageAndVersion : batch.records()) {
                    ApiMessage message = messageAndVersion.message();
                    StringBuilder bld = new StringBuilder();
                    bld.append(COMMIT).append(" ").append(message.toString());
                    serializedEvents.add(bld.toString());
                }
                StringBuilder bld = new StringBuilder();
                bld.append(LAST_COMMITTED_OFFSET).append(" ").append(lastCommittedOffset);
                serializedEvents.add(bld.toString());
            }
        } finally {
            reader.close();
        }
    }

    @Override
    public synchronized void handleLoadSnapshot(SnapshotReader<ApiMessageAndVersion> reader) {
        long lastCommittedOffset = reader.lastContainedLogOffset();
        try {
            while (reader.hasNext()) {
                Batch<ApiMessageAndVersion> batch = reader.next();

                for (ApiMessageAndVersion messageAndVersion : batch.records()) {
                    ApiMessage message = messageAndVersion.message();
                    StringBuilder bld = new StringBuilder();
                    bld.append(SNAPSHOT).append(" ").append(message.toString());
                    serializedEvents.add(bld.toString());
                }
                StringBuilder bld = new StringBuilder();
                bld.append(LAST_COMMITTED_OFFSET).append(" ").append(lastCommittedOffset);
                serializedEvents.add(bld.toString());
            }
        } finally {
            reader.close();
        }
    }

    @Override
    public synchronized void handleLeaderChange(LeaderAndEpoch newLeaderAndEpoch) {
        LeaderAndEpoch oldLeaderAndEpoch = this.leaderAndEpoch;
        this.leaderAndEpoch = newLeaderAndEpoch;

        if (newLeaderAndEpoch.isLeader(nodeId)) {
            StringBuilder bld = new StringBuilder();
            bld.append(NEW_LEADER).append(" ").
                append(nodeId).append(" ").append(newLeaderAndEpoch.epoch());
            serializedEvents.add(bld.toString());
        } else if (oldLeaderAndEpoch.isLeader(nodeId)) {
            StringBuilder bld = new StringBuilder();
            bld.append(RENOUNCE).append(" ").append(newLeaderAndEpoch.epoch());
            serializedEvents.add(bld.toString());
        }
    }

    @Override
    public void beginShutdown() {
        StringBuilder bld = new StringBuilder();
        bld.append(SHUTDOWN);
        synchronized (this) {
            serializedEvents.add(bld.toString());
        }
    }

    public synchronized List<String> serializedEvents() {
        return new ArrayList<>(serializedEvents);
    }
}
