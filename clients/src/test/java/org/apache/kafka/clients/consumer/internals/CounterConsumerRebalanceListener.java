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

package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

public class CounterConsumerRebalanceListener implements ConsumerRebalanceListener {

    private final AtomicInteger revokedCounter = new AtomicInteger();
    private final AtomicInteger assignedCounter = new AtomicInteger();
    private final AtomicInteger lostCounter = new AtomicInteger();

    private final Optional<RuntimeException> revokedError;
    private final Optional<RuntimeException> assignedError;
    private final Optional<RuntimeException> lostError;

    public CounterConsumerRebalanceListener() {
        this(Optional.empty(), Optional.empty(), Optional.empty());
    }

    public CounterConsumerRebalanceListener(Optional<RuntimeException> revokedError,
                                            Optional<RuntimeException> assignedError,
                                            Optional<RuntimeException> lostError) {
        this.revokedError = revokedError;
        this.assignedError = assignedError;
        this.lostError = lostError;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        try {
            if (revokedError.isPresent())
                throw revokedError.get();
        } finally {
            revokedCounter.incrementAndGet();
        }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        try {
            if (assignedError.isPresent())
                throw assignedError.get();
        } finally {
            assignedCounter.incrementAndGet();
        }
    }

    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
        try {
            if (lostError.isPresent())
                throw lostError.get();
        } finally {
            lostCounter.incrementAndGet();
        }
    }

    public int revokedCount() {
        return revokedCounter.get();
    }

    public int assignedCount() {
        return assignedCounter.get();
    }

    public int lostCount() {
        return lostCounter.get();
    }
}