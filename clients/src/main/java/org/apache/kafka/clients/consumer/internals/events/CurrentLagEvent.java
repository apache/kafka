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

import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;

import java.util.Objects;
import java.util.OptionalLong;

public class CurrentLagEvent extends CompletableApplicationEvent<OptionalLong> {

    private final TopicPartition partition;

    private final IsolationLevel isolationLevel;

    public CurrentLagEvent(final TopicPartition partition, final IsolationLevel isolationLevel, final long deadlineMs) {
        super(Type.CURRENT_LAG, deadlineMs);
        this.partition = Objects.requireNonNull(partition);
        this.isolationLevel = Objects.requireNonNull(isolationLevel);
    }

    public TopicPartition partition() {
        return partition;
    }

    public IsolationLevel isolationLevel() {
        return isolationLevel;
    }

    @Override
    public String toStringBase() {
        return super.toStringBase() + ", partition=" + partition + ", isolationLevel=" + isolationLevel;
    }
}
