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
package org.apache.kafka.coordinator.common.runtime;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.timeline.SnapshotRegistry;

import java.util.Objects;

/**
 * A CoordinatorBuilder that creates a MockCoordinator.
 */
public class MockCoordinatorShardBuilder implements CoordinatorShardBuilder<MockCoordinatorShard, String> {
    private SnapshotRegistry snapshotRegistry;
    private CoordinatorTimer<Void, String> timer;
    private CoordinatorExecutor<String> executor;

    @Override
    public CoordinatorShardBuilder<MockCoordinatorShard, String> withSnapshotRegistry(
        SnapshotRegistry snapshotRegistry
    ) {
        this.snapshotRegistry = snapshotRegistry;
        return this;
    }

    @Override
    public CoordinatorShardBuilder<MockCoordinatorShard, String> withLogContext(
        LogContext logContext
    ) {
        return this;
    }

    @Override
    public CoordinatorShardBuilder<MockCoordinatorShard, String> withTime(
        Time time
    ) {
        return this;
    }

    @Override
    public CoordinatorShardBuilder<MockCoordinatorShard, String> withExecutor(
        CoordinatorExecutor<String> executor
    ) {
        this.executor = executor;
        return this;
    }

    @Override
    public CoordinatorShardBuilder<MockCoordinatorShard, String> withTimer(
        CoordinatorTimer<Void, String> timer
    ) {
        this.timer = timer;
        return this;
    }

    @Override
    public CoordinatorShardBuilder<MockCoordinatorShard, String> withCoordinatorMetrics(CoordinatorMetrics coordinatorMetrics) {
        return this;
    }

    @Override
    public CoordinatorShardBuilder<MockCoordinatorShard, String> withTopicPartition(
        TopicPartition topicPartition
    ) {
        return this;
    }

    @Override
    public MockCoordinatorShard build() {
        return new MockCoordinatorShard(
            Objects.requireNonNull(this.snapshotRegistry),
            Objects.requireNonNull(this.timer),
            Objects.requireNonNull(this.executor)
        );
    }
}
