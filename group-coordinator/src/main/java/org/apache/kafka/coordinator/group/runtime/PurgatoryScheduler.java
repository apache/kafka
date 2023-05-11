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

import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.concurrent.CompletableFuture;

/**
 * A scheduler to add and delete operations from the purgatory.
 */
@InterfaceStability.Unstable
public interface PurgatoryScheduler {

    /**
     * Add an operation to the purgatory.
     *
     * @param key         The key to identify this operation.
     * @param deadlineMs  The deadline in milliseconds.
     * @param operation   The operation to perform.
     */
    void add(String key, long deadlineMs, CompletableFuture<Void> operation);

    /**
     * Remove an operation with the given key.
     *
     * @param key The key.
     */
    void remove(String key);

    /**
     * Shut down the scheduler.
     */
    void shutdown();
}
