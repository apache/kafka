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
package org.apache.kafka.trogdor.workload;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kafka.common.utils.Time;

import java.util.Optional;

/**
 * A transactions generator where we commit a transaction every N milliseconds
 */
public class TimeIntervalTransactionsGenerator implements TransactionGenerator {

    private final Time time;
    private final int intervalMs;

    private Optional<Long> lastCommitMs = Optional.empty();

    @JsonCreator
    public TimeIntervalTransactionsGenerator(@JsonProperty("transactionIntervalMs") int intervalMs) {
        this(intervalMs, Time.SYSTEM);
    }

    TimeIntervalTransactionsGenerator(@JsonProperty("transactionIntervalMs") int intervalMs,
                                      Time time) {
        if (intervalMs < 1) {
            throw new IllegalArgumentException("Cannot have a negative interval");
        }
        this.time = time;
        this.intervalMs = intervalMs;
    }

    @JsonProperty
    public int transactionIntervalMs() {
        return intervalMs;
    }

    @Override
    public synchronized TransactionAction nextAction() {
        if (!lastCommitMs.isPresent()) {
            lastCommitMs = Optional.of(time.milliseconds());
            return TransactionAction.BEGIN_TRANSACTION;
        }
        if (time.milliseconds() - lastCommitMs.get() >= intervalMs) {
            lastCommitMs = Optional.empty();
            return TransactionAction.COMMIT_TRANSACTION;
        }

        return TransactionAction.NO_OP;
    }
}
