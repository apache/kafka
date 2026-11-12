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
package org.apache.kafka.raft.internals;

import java.util.concurrent.CompletionStage;

/**
 * Simple purgatory interface which supports waiting with expiration for a given threshold
 * to be reached. The threshold is specified through {@link #await(Comparable, long)}.
 * The returned stage can be completed in the following ways:
 *
 * 1) The stage is completed successfully if the threshold value is reached
 *    in a call to {@link #maybeComplete(Comparable, long)}.
 * 2) The stage is completed successfully if {@link #completeAll(long)} is called.
 * 3) The stage is completed exceptionally if {@link #completeAllExceptionally(Throwable)}
 *    is called.
 * 4) If none of the above happens before the expiration of the timeout passed to
 *    {@link #await(Comparable, long)}, then the stage will be completed exceptionally
 *    with a {@link org.apache.kafka.common.errors.TimeoutException}.
 *
 * Note that the stage objects should be organized in order so that completing awaiting
 * stages would stop early and not traverse all awaiting stages.
 *
 * @param <T> threshold value type
 */
public interface FuturePurgatory<T extends Comparable<T>> {
    /**
     * Create a new completion stage which is tracked by the purgatory.
     *
     * @param threshold     the minimum value that must be reached for the stage
     *                      to be successfully completed by {@link #maybeComplete(Comparable, long)}
     * @param maxWaitTimeMs the maximum time to wait for completion. If this
     *                      timeout is reached, then the stage will be completed exceptionally
     *                      with a {@link org.apache.kafka.common.errors.TimeoutException}
     *
     * @return              the stage tracking the expected completion
     */
    CompletionStage<Long> await(T threshold, long maxWaitTimeMs);

    /**
     * Complete awaiting stages whose threshold value from {@link FuturePurgatory#await} are smaller
     * than the given threshold value. The completion callbacks will be triggered from the calling thread.
     *
     * @param value         the threshold value used to determine which stages can be completed
     * @param currentTimeMs the current time in milliseconds that will be passed to
     *                      {@link CompletableFuture#complete(Object)} when the stages are completed
     */
    void maybeComplete(T value, long currentTimeMs);

    /**
     * Complete all awaiting stages successfully.
     *
     * @param currentTimeMs the current time in milliseconds that will be passed to
     *                      {@link CompletableFuture#complete(Object)} when the stages are completed
     */
    void completeAll(long currentTimeMs);

    /**
     * Complete all awaiting stages exceptionally. The completion callbacks will be
     * triggered with the passed in exception.
     *
     * @param exception     the exception that will be passed to
     *                      {@link CompletableFuture#completeExceptionally(Throwable)}
     */
    void completeAllExceptionally(Throwable exception);

    /**
     * The number of currently waiting stages.
     *
     * @return the number of waiting stages
     */
    int numWaiting();
}
