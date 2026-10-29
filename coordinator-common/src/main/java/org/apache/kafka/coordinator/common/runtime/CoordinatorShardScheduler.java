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

import java.util.concurrent.CompletableFuture;

/**
 * Scheduler interface for shard-scoped components to schedule write operations
 * through the coordinator runtime without depending on the full runtime API.
 *
 * @param <U> The record type used by the coordinator.
 */
@FunctionalInterface
public interface CoordinatorShardScheduler<U> {

    /**
     * A write operation that produces records.
     *
     * @param <U> The record type used by the coordinator.
     */
    @FunctionalInterface
    interface WriteOperation<U> {
        CoordinatorResult<Void, U> generate();
    }

    /**
     * Schedules a write operation to be executed by the runtime.
     *
     * @param operationName The name of the operation for logging/debugging.
     * @param operation     The write operation to execute.
     * @return A future that completes when the operation is done.
     */
    CompletableFuture<Void> scheduleWriteOperation(
        String operationName,
        WriteOperation<U> operation
    );
}
