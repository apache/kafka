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

import java.util.Optional;

import static java.util.Collections.singletonList;

public class ReplicatedCounter implements RaftClient.Listener<Integer> {
    private final int nodeId;
    private final Logger log;
    private final RaftClient<Integer> client;

    private int committed;
    private int uncommitted;
    private Optional<Integer> claimedEpoch;

    public ReplicatedCounter(
        int nodeId,
        RaftClient<Integer> client,
        LogContext logContext
    ) {
        this.nodeId = nodeId;
        this.client = client;
        this.log = logContext.logger(ReplicatedCounter.class);

        this.committed = 0;
        this.uncommitted = 0;
        this.claimedEpoch = Optional.empty();
    }

    public synchronized boolean isWritable() {
        return claimedEpoch.isPresent();
    }

    public synchronized void increment() {
        if (!claimedEpoch.isPresent()) {
            throw new KafkaException("Counter is not currently writable");
        }

        int epoch = claimedEpoch.get();
        uncommitted += 1;
        Long offset = client.scheduleAppend(epoch, singletonList(uncommitted));
        if (offset != null) {
            log.debug("Scheduled append of record {} with epoch {} at offset {}",
                uncommitted, epoch, offset);
        }
    }

    @Override
    public synchronized void handleCommit(BatchReader<Integer> reader) {
        try {
            int initialValue = this.committed;
            while (reader.hasNext()) {
                BatchReader.Batch<Integer> batch = reader.next();
                log.debug("Handle commit of batch with records {} at base offset {}",
                    batch.records(), batch.baseOffset());
                for (Integer value : batch.records()) {
                    if (value != this.committed + 1) {
                        throw new AssertionError("Expected next committed value to be " +
                            (this.committed + 1) + ", but instead found " + value + " on node " + nodeId);
                    }
                    this.committed = value;
                }
            }
            log.debug("Counter incremented from {} to {}", initialValue, committed);
        } finally {
            reader.close();
        }
    }

    @Override
    public synchronized void handleClaim(int epoch) {
        log.debug("Counter uncommitted value initialized to {} after claiming leadership in epoch {}",
            committed, epoch);
        this.uncommitted = committed;
        this.claimedEpoch = Optional.of(epoch);
    }

    @Override
    public synchronized void handleResign() {
        log.debug("Counter uncommitted value reset after resigning leadership");
        this.uncommitted = -1;
        this.claimedEpoch = Optional.empty();
    }

}
