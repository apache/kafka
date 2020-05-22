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
package org.apache.kafka.raft;

import java.util.concurrent.CompletableFuture;

/**
 * Simple purgatory interface which allows tracking a set of expirable futures.
 *
 * @param <T> Type completion type
 */
public interface FuturePurgatory<T> {

    /**
     * Add a future to this purgatory for tracking.
     *
     * @param future the future tracking the expected completion. A subsequent call
     *               to {@link #completeAll(Object)} will complete this future
     *               if it does not expire first.
     * @param maxWaitTimeMs the maximum time to wait for completion. If this
     *               timeout is reached, then the future will be completed exceptionally
     *               with a {@link org.apache.kafka.common.errors.TimeoutException}
     */
    void await(CompletableFuture<T> future, long maxWaitTimeMs);

    /**
     * Complete all awaiting futures. The completion callbacks will be triggered
     * from the calling thread.
     *
     * @param value the value that will be passed to {@link CompletableFuture#complete(Object)}
     *              when the futures are completed
     */
    void completeAll(T value);

    /**
     * The number of currently waiting futures.
     *
     * @return the n
     */
    int numWaiting();

}
