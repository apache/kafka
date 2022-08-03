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

package org.apache.kafka.shell;

import org.apache.kafka.raft.BatchReader;
import org.apache.kafka.raft.LeaderAndEpoch;
import org.apache.kafka.raft.RaftClient;
import org.apache.kafka.snapshot.SnapshotReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;


/**
 * A RaftClient Listener which tracks when we have caught up with the high water mark of the quorum.
 */
public class TrackingListener<T> implements RaftClient.Listener<T> {
    private static final Logger log = LoggerFactory.getLogger(TrackingListener.class);
    private final CompletableFuture<Void> caughtUpFuture;
    private final Supplier<OptionalLong> highWaterMarkSupplier;
    private final RaftClient.Listener<T> underlying;

    public TrackingListener(
        CompletableFuture<Void> caughtUpFuture,
        Supplier<OptionalLong> highWaterMarkSupplier,
        RaftClient.Listener<T> underlying
    ) {
        this.caughtUpFuture = caughtUpFuture;
        this.highWaterMarkSupplier = highWaterMarkSupplier;
        this.underlying = underlying;
    }

    @Override
    public void handleCommit(BatchReader<T> reader) {
        // Copy the BatchReader so that we can determine its final offset.
        TrackingBatchReader<T> trackingReader = new TrackingBatchReader<>(reader);
        if (log.isTraceEnabled()) {
            log.trace("handleCommit(trackingReader.lastOffset={}, trackingReader.baseOffset={})",
                trackingReader.lastOffset(), trackingReader.baseOffset());
        }
        underlying.handleCommit(trackingReader);
        checkIfCaughtUp(trackingReader.lastOffset().getAsLong() + 1);
    }

    @Override
    public void handleSnapshot(SnapshotReader<T> reader) {
        if (log.isTraceEnabled()) {
            log.trace("handleSnapshot(reader.lastContainedLogOffset={})", reader.lastContainedLogOffset());
        }
        underlying.handleSnapshot(reader);
        checkIfCaughtUp(reader.lastContainedLogOffset() + 1);
    }

    @Override
    public void handleLeaderChange(LeaderAndEpoch leader) {
        if (log.isTraceEnabled()) {
            log.trace("handleLeaderChange(leader={})", leader);
        }
        underlying.handleLeaderChange(leader);
    }

    @Override
    public void beginShutdown() {
        if (log.isTraceEnabled()) {
            log.trace("beginShutdown");
        }
        underlying.beginShutdown();
    }

    private void checkIfCaughtUp(long offset) {
        if (!caughtUpFuture.isDone()) {
            OptionalLong highWaterMarkOption = highWaterMarkSupplier.get();
            if (highWaterMarkOption.isPresent()) {
                long highWatermark = highWaterMarkOption.getAsLong();
                if (offset >= highWatermark) {
                    log.debug("Offset {} has caught up with high water mark {}.", offset, highWatermark);
                    caughtUpFuture.complete(null);
                } else {
                    log.debug("Offset {} has not yet caught up with high water mark {}.", offset, highWatermark);
                }
            } else {
                log.debug("High water mark is not known for RaftClient.");
            }
        }
    }
}
