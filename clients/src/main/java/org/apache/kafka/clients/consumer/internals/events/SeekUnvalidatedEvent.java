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
package org.apache.kafka.clients.consumer.internals.events;

import org.apache.kafka.clients.consumer.internals.SubscriptionState;
import org.apache.kafka.common.TopicPartition;

import java.util.Optional;

/**
 * Event to perform {@link SubscriptionState#seekUnvalidated(TopicPartition, SubscriptionState.FetchPosition)}
 * in the background thread. This can avoid race conditions when subscription state is updated.
 */
public class SeekUnvalidatedEvent extends CompletableApplicationEvent<Void> {
    private final TopicPartition partition;
    private final long offset;
    private final Optional<Integer> offsetEpoch;

    public SeekUnvalidatedEvent(long deadlineMs, TopicPartition partition, long offset, Optional<Integer> offsetEpoch) {
        super(Type.SEEK_UNVALIDATED, deadlineMs);
        this.partition = partition;
        this.offset = offset;
        this.offsetEpoch = offsetEpoch;
    }

    public TopicPartition partition() {
        return partition;
    }

    public long offset() {
        return offset;
    }

    public Optional<Integer> offsetEpoch() {
        return offsetEpoch;
    }

    @Override
    protected String toStringBase() {
        return super.toStringBase()
                + ", partition=" + partition
                + ", offset=" + offset
                + offsetEpoch.map(integer -> ", offsetEpoch=" + integer).orElse("");
    }
}
