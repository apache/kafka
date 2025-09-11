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

public class CompositePollEvent extends ApplicationEvent {

    private final long deadlineMs;
    private final long pollTimeMs;
    private final Type nextStep;
    private final CompletableFuture<CompositePollResult> future;

    public CompositePollEvent(long deadlineMs, long pollTimeMs, Type nextStep) {
        super(Type.COMPOSITE_POLL);
        this.deadlineMs = deadlineMs;
        this.pollTimeMs = pollTimeMs;
        this.nextStep = nextStep;
        this.future = new CompletableFuture<>();
    }

    public long deadlineMs() {
        return deadlineMs;
    }

    public long pollTimeMs() {
        return pollTimeMs;
    }

    public Type nextStep() {
        return nextStep;
    }

    public CompletableFuture<CompositePollResult> future() {
        return future;
    }

    @Override
    protected String toStringBase() {
        return super.toStringBase() + ", deadlineMs=" + deadlineMs + ", pollTimeMs=" + pollTimeMs + ", nextStep=" + nextStep + ", future=" + future;
    }
}
