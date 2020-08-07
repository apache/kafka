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
 * Each future will be associated with a comparable value which are used to determine
 * if the future object can be completed.
 *
 * Note that the future objects should be organized in order so that completing awaiting
 * futures would stop early and not traverse all the futures
 *
 * @param <T> Type completion type
 */
public interface FuturePurgatory<T extends Comparable<T>> {

    /**
     * Add a future to this purgatory for tracking.
     *
     * @param value         the comparable value of the future object that will be used to determine
     *                      if the future can be completed or not
     * @param maxWaitTimeMs the maximum time to wait for completion. If this
     *                      timeout is reached, then the future will be completed exceptionally
     *                      with a {@link org.apache.kafka.common.errors.TimeoutException}
     *
     * @return              the future tracking the expected completion. A subsequent call
     *                      to {@link #complete(Comparable, long)} will complete this future
     *                      with the completion time in milliseconds if it does not expire first
     */
    CompletableFuture<Long> await(T value, long maxWaitTimeMs);

    /**
     * Complete awaiting futures whose associated values are larger than the given threshold value.
     * The completion callbacks will be triggered from the calling thread.
     *
     * @param value         the threshold value used to determine which futures can be completed
     * @param currentTimeMs the current time in milliseconds that will be passed to {@link CompletableFuture#complete(Object)}
     *                      when the futures are completed
     */
    void complete(T value, long currentTimeMs);

    /**
     * Complete awaiting futures whose associated values are larger than the given threshold value exceptionally.
     * The completion callbacks will be triggered with the passed in exception.
     *
     * @param exception     the current time in milliseconds that will be passed to {@link CompletableFuture#completeExceptionally(Throwable)}
     */
    void completeAllExceptionally(Throwable exception);

    /**
     * The number of currently waiting futures.
     *
     * @return the n
     */
    int numWaiting();
}
