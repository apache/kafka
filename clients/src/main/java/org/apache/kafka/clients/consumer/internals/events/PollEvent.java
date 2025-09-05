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

import java.util.concurrent.CompletableFuture;

public class PollEvent extends ApplicationEvent {

    private final long pollTimeMs;

    /**
     * A future that represents the completion of reconciliation and auto-commit
     * processing.
     * This future is completed when all commit request generation points have
     * been passed, including:
     * <ul>
     *   <li>auto-commit on rebalance</li>
     *   <li>auto-commit on the interval</li>
     * </ul>
     * Once completed, it signals that it's safe for the consumer to proceed with
     * fetching new records.
     */
    private final CompletableFuture<Void> reconcileAndAutoCommit = new CompletableFuture<>();

    public PollEvent(final long pollTimeMs) {
        super(Type.POLL);
        this.pollTimeMs = pollTimeMs;
    }

    public long pollTimeMs() {
        return pollTimeMs;
    }

    public CompletableFuture<Void> reconcileAndAutoCommit() {
        return reconcileAndAutoCommit;
    }

    public void markReconcileAndAutoCommitComplete() {
        reconcileAndAutoCommit.complete(null);
    }

    @Override
    public String toStringBase() {
        return super.toStringBase() + ", pollTimeMs=" + pollTimeMs;
    }
}