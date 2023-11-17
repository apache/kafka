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
package org.apache.kafka.coordinator.group.runtime;

import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.timeline.SnapshotRegistry;


/**
 * A builder to build a {@link CoordinatorShard} replicated state machine.
 *
 * @param <S> The type of the coordinator.
 * @param <U> The record type.
 */
public interface CoordinatorShardBuilder<S extends CoordinatorShard<U>, U> {

    /**
     * Sets the snapshot registry used to back all the timeline
     * datastructures used by the coordinator.
     *
     * @param snapshotRegistry The registry.
     *
     * @return The builder.
     */
    CoordinatorShardBuilder<S, U> withSnapshotRegistry(
        SnapshotRegistry snapshotRegistry
    );

    /**
     * Sets the log context.
     *
     * @param logContext The log context.
     *
     * @return The builder.
     */
    CoordinatorShardBuilder<S, U> withLogContext(
        LogContext logContext
    );

    /**
     * Sets the time.
     *
     * @param time The system time.
     *
     * @return The builder.
     */
    CoordinatorShardBuilder<S, U> withTime(
        Time time
    );

    /**
     * Sets the coordinator timer.
     *
     * @param timer The coordinator timer.
     *
     * @return The builder.
     */
    CoordinatorShardBuilder<S, U> withTimer(
        CoordinatorTimer<Void, U> timer
    );

    /**
     * @return The built coordinator.
     */
    S build();
}
