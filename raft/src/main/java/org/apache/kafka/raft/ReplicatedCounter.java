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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.OptionalInt;

public class ReplicatedCounter implements RaftClient.Listener<Integer> {
    private final int localBrokerId;
    private final Logger log;
    private final RaftClient<Integer> client;

    private int committed;
    private int uncommitted;
    private LeaderAndEpoch currentLeaderAndEpoch = new LeaderAndEpoch(OptionalInt.empty(), 0);

    public ReplicatedCounter(
        int localBrokerId,
        RaftClient<Integer> client,
        LogContext logContext
    ) {
        this.localBrokerId = localBrokerId;
        this.client = client;
        this.log = logContext.logger(ReplicatedCounter.class);
    }

    public synchronized void poll() {
        // Check for leader changes
        LeaderAndEpoch latestLeaderAndEpoch = client.currentLeaderAndEpoch();
        if (!currentLeaderAndEpoch.equals(latestLeaderAndEpoch)) {
            this.committed = 0;
            this.uncommitted = 0;
            this.currentLeaderAndEpoch = latestLeaderAndEpoch;
        }
    }

    public synchronized boolean isWritable() {
        // We only accept appends if we are the leader and we have caught up to a position
        // within the current leader epoch
        return localBrokerId == currentLeaderAndEpoch.leaderId.orElse(-1);
    }

    public synchronized void increment() {
        if (!isWritable())
            throw new KafkaException("Counter is not currently writable");
        uncommitted += 1;
        Long offset = client.scheduleAppend(currentLeaderAndEpoch.epoch, Collections.singletonList(uncommitted));
        if (offset != null) {
            log.debug("Scheduled append of record {} with epoch {} at offset {}",
                uncommitted, currentLeaderAndEpoch.epoch, offset);
        }
    }

    @Override
    public void handleCommit(int epoch, long lastOffset, List<Integer> records) {
        log.debug("Received commit of records {} with epoch {} at last offset {}",
            records, epoch, lastOffset);
        this.committed = records.get(records.size() - 1);
    }

}
