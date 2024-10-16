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
package org.apache.kafka.common.internals;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * This internal class exists because CompletableFuture exposes complete(), completeExceptionally() and
 * other methods which would allow erroneous completion by user code of a KafkaFuture returned from a
 * Kafka API to a client application.
 * @param <T> The type of the future value.
 */
public class KafkaCompletableFuture<T> extends CompletableFuture<T> {

    /**
     * Completes this future normally. For internal use by the Kafka clients, not by user code.
     * @param value the result value
     * @return {@code true} if this invocation caused this CompletableFuture
     * to transition to a completed state, else {@code false}
     */
    boolean kafkaComplete(T value) {
        return super.complete(value);
    }

    /**
     * Completes this future exceptionally. For internal use by the Kafka clients, not by user code.
     * @param throwable the exception.
     * @return {@code true} if this invocation caused this CompletableFuture
     * to transition to a completed state, else {@code false}
     */
    boolean kafkaCompleteExceptionally(Throwable throwable) {
        return super.completeExceptionally(throwable);
    }

    @Override
    public boolean complete(T value) {
        throw erroneousCompletionException();
    }

    @Override
    public boolean completeExceptionally(Throwable ex) {
        throw erroneousCompletionException();
    }

    @Override
    public void obtrudeValue(T value) {
        throw erroneousCompletionException();
    }

    @Override
    public void obtrudeException(Throwable ex) {
        throw erroneousCompletionException();
    }

    @Override
    public <U> CompletableFuture<U> newIncompleteFuture() {
        return new KafkaCompletableFuture<>();
    }

    @Override
    public CompletableFuture<T> completeAsync(Supplier<? extends T> supplier, Executor executor) {
        throw erroneousCompletionException();
    }

    @Override
    public CompletableFuture<T> completeAsync(Supplier<? extends T> supplier) {
        throw erroneousCompletionException();
    }

    @Override
    public CompletableFuture<T> completeOnTimeout(T value, long timeout, TimeUnit unit) {
        throw erroneousCompletionException();
    }

    private UnsupportedOperationException erroneousCompletionException() {
        return new UnsupportedOperationException("User code should not complete futures returned from Kafka clients");
    }
}
