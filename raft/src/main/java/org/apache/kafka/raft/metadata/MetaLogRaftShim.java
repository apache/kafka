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
package org.apache.kafka.raft.metadata;

import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.metadata.ApiMessageAndVersion;
import org.apache.kafka.metalog.MetaLogLeader;
import org.apache.kafka.metalog.MetaLogListener;
import org.apache.kafka.metalog.MetaLogManager;
import org.apache.kafka.raft.BatchReader;
import org.apache.kafka.raft.LeaderAndEpoch;
import org.apache.kafka.raft.RaftClient;

import java.util.List;
import java.util.stream.Collectors;

/**
 * For now, we rely on a shim to translate from `RaftClient` to `MetaLogManager`.
 * Once we check in to trunk, we can drop `RaftClient` and implement `MetaLogManager`
 * directly.
 */
public class MetaLogRaftShim implements MetaLogManager {
    private final RaftClient<ApiMessageAndVersion> client;
    private final int nodeId;

    public MetaLogRaftShim(RaftClient<ApiMessageAndVersion> client, int nodeId) {
        this.client = client;
        this.nodeId = nodeId;
    }

    @Override
    public void initialize() {
        // NO-OP - The RaftClient is initialized externally
    }

    @Override
    public void register(MetaLogListener listener) {
        client.register(new ListenerShim(listener));
    }

    @Override
    public long scheduleAtomicWrite(long epoch, List<ApiMessageAndVersion> batch) {
        return write(epoch, batch, true);
    }

    @Override
    public long scheduleWrite(long epoch, List<ApiMessageAndVersion> batch) {
        return write(epoch, batch, false);
    }

    private long write(long epoch, List<ApiMessageAndVersion> batch, boolean isAtomic) {
        final Long result;
        if (isAtomic) {
            result = client.scheduleAtomicAppend((int) epoch, batch);
        } else {
            result = client.scheduleAppend((int) epoch, batch);
        }

        if (result == null) {
            throw new IllegalArgumentException(
                String.format(
                    "Unable to alloate a buffer for the schedule write operation: epoch %s, batch %s)",
                    epoch,
                    batch
                )
            );
        } else {
            return result;
        }
    }

    @Override
    public void renounce(long epoch) {
        throw new UnsupportedOperationException();
    }

    @Override
    public MetaLogLeader leader() {
        LeaderAndEpoch leaderAndEpoch = client.leaderAndEpoch();
        return new MetaLogLeader(leaderAndEpoch.leaderId.orElse(-1), leaderAndEpoch.epoch);
    }

    @Override
    public int nodeId() {
        return nodeId;
    }

    private class ListenerShim implements RaftClient.Listener<ApiMessageAndVersion> {
        private final MetaLogListener listener;

        private ListenerShim(MetaLogListener listener) {
            this.listener = listener;
        }

        @Override
        public void handleCommit(BatchReader<ApiMessageAndVersion> reader) {
            try {
                // TODO: The `BatchReader` might need to read from disk if this is
                // not a leader. We want to move this IO to the state machine so that
                // it does not block Raft replication
                while (reader.hasNext()) {
                    BatchReader.Batch<ApiMessageAndVersion> batch = reader.next();
                    List<ApiMessage> records = batch.records().stream()
                        .map(ApiMessageAndVersion::message)
                        .collect(Collectors.toList());
                    listener.handleCommits(batch.lastOffset(), records);
                }
            } finally {
                reader.close();
            }
        }

        @Override
        public void handleClaim(int epoch) {
            listener.handleNewLeader(new MetaLogLeader(nodeId, epoch));
        }

        @Override
        public void handleResign(int epoch) {
            listener.handleRenounce(epoch);
        }

        @Override
        public String toString() {
            return "ListenerShim(" +
                    "listener=" + listener +
                    ')';
        }
    }

}
