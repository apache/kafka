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
package org.apache.kafka.tiered.storage.actions;

import org.apache.kafka.tiered.storage.TieredStorageTestAction;
import org.apache.kafka.tiered.storage.TieredStorageTestContext;
import kafka.log.UnifiedLog;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.storage.internals.epoch.LeaderEpochFileCache;
import org.apache.kafka.storage.internals.log.EpochEntry;
import org.apache.kafka.test.TestUtils;
import scala.Option;

import java.io.PrintStream;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

public final class ExpectLeaderEpochCheckpointAction implements TieredStorageTestAction {

    private final Integer brokerId;
    private final TopicPartition partition;
    private final Integer beginEpoch;
    private final Long startOffset;

    public ExpectLeaderEpochCheckpointAction(Integer brokerId,
                                             TopicPartition partition,
                                             Integer beginEpoch,
                                             Long startOffset) {
        this.brokerId = brokerId;
        this.partition = partition;
        this.beginEpoch = beginEpoch;
        this.startOffset = startOffset;
    }

    @Override
    public void doExecute(TieredStorageTestContext context) throws InterruptedException, ExecutionException {
        AtomicReference<EpochEntry> earliestEntryOpt = new AtomicReference<>();
        TestUtils.waitForCondition(() -> {
            EpochEntry earliestEntry = null;
            Optional<UnifiedLog> log = context.log(brokerId, partition);
            if (log.isPresent()) {
                Option<LeaderEpochFileCache> leaderEpochCache = log.get().leaderEpochCache();
                if (leaderEpochCache.isDefined()) {
                    earliestEntry = leaderEpochCache.get().earliestEntry().orElse(null);
                }
            }
            earliestEntryOpt.set(earliestEntry);
            return earliestEntry != null && beginEpoch == earliestEntry.epoch
                    && startOffset == earliestEntry.startOffset;
        }, 2000L, "leader-epoch-checkpoint begin-epoch: " + beginEpoch + " and start-offset: "
                + startOffset + " doesn't match with actual: " + earliestEntryOpt.get());
    }

    @Override
    public void describe(PrintStream output) {
        output.printf("expect-leader-epoch-checkpoint broker-id: %d, partition: %s, beginEpoch: %d, startOffset: %d%n",
                brokerId, partition, beginEpoch, startOffset);
    }
}
