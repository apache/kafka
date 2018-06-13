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

import org.apache.kafka.common.KafkaFuture;


import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A flexible future which supports call chaining and other asynchronous programming patterns.
 * This is a thin shim on top of Java 8's CompletableFuture.
 *
 * Please note that while this class offers methods similar to CompletableFuture's whenComplete and thenApply,
 * functions passed to these methods will never be called with CompletionException. If you wish to use
 * CompletableFuture semantics, use {@link #toCompletableFuture()}.
 *
 */
public class KafkaFutureImpl<T> extends KafkaFuture<T> {

    private CompletableFuture<T> completableFuture;

    public KafkaFutureImpl() {
        this(new CompletableFuture<>());
    }

    private KafkaFutureImpl(CompletableFuture<T> future) {
        this.completableFuture = future;
    }



    /**
     * Returns a new KafkaFuture that, when this future completes normally, is executed with this
     * futures's result as the argument to the supplied function.
     */
    @Override
    public <R> KafkaFuture<R> thenApply(BaseFunction<T, R> function) {
        return new KafkaFutureImpl<R>(completableFuture.thenApply(function::apply));
    }

    @Deprecated
    public <R> void copyWith(KafkaFuture<R> future, BaseFunction<R, T> function) {
        ((KafkaFutureImpl<R>) future).completableFuture.thenApply(function::apply).whenComplete((t, throwable) -> {
            if (throwable != null) {
                completableFuture.completeExceptionally(throwable);
            } else {
                completableFuture.complete(t);
            }
        });

    }

    /**
     * @See KafkaFutureImpl#thenApply(BaseFunction)
     */
    @Override
    public <R> KafkaFuture<R> thenApply(Function<T, R> function) {
        return new KafkaFutureImpl<>(completableFuture.thenApply(function::apply));
    }

    @Override
    public KafkaFuture<T> whenComplete(final BiConsumer<? super T, ? super Throwable> biConsumer) {
        KafkaFutureImpl<T> dependent = new KafkaFutureImpl<>();
        completableFuture.whenComplete((t, throwable) -> {
            try {
                if (throwable instanceof CompletionException) {
                    if (throwable.getCause() instanceof CancellationException) {
                        biConsumer.accept(null, throwable.getCause());
                        dependent.cancel(false);
                    } else {
                        biConsumer.accept(null, throwable.getCause());
                        dependent.completeExceptionally(throwable.getCause());
                    }
                } else if (throwable != null) {
                    biConsumer.accept(null, throwable);
                    dependent.completeExceptionally(throwable);
                } else {
                    biConsumer.accept(t, null);
                    dependent.complete(t);
                }
            } catch (Exception e) {
                Throwable throwableToCompleteWith;
                if (throwable == null) {
                    throwableToCompleteWith = e;
                } else if (throwable instanceof CompletionException) {
                    throwableToCompleteWith = throwable.getCause();
                } else {
                    throwableToCompleteWith = throwable;
                }
                dependent.completeExceptionally(throwableToCompleteWith);
            }
        });
        return dependent;
    }

    protected synchronized void addWaiter(BiConsumer<? super T, ? super Throwable> action) {
        completableFuture.whenComplete(action::accept);
    }

    @Override
    public boolean complete(T newValue) {
        return completableFuture.complete(newValue);
    }

    @Override
    public boolean completeExceptionally(Throwable newException) {
        return completableFuture.completeExceptionally(newException);
    }

    /**
     * If not already completed, completes this future with a CancellationException.  Dependent
     * futures that have not already completed will also complete exceptionally, with a
     * CompletionException caused by this CancellationException.
     *
     * TODO was this true?
     */
    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return completableFuture.cancel(mayInterruptIfRunning);
    }

    /**
     * Waits if necessary for this future to complete, and then returns its result.
     */
    @Override
    public T get() throws InterruptedException, ExecutionException {
        return completableFuture.get();
    }

    /**
     * Waits if necessary for at most the given time for this future to complete, and then returns
     * its result, if available.
     */
    @Override
    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException,
            TimeoutException {
        return completableFuture.get(timeout, unit);
    }

    /**
     * Returns the result value (or throws any encountered exception) if completed, else returns
     * the given valueIfAbsent.
     */
    @Override
    public T getNow(T valueIfAbsent) throws InterruptedException, ExecutionException {
        return completableFuture.getNow(valueIfAbsent);
    }

    /**
     * Returns true if this CompletableFuture was cancelled before it completed normally.
     */
    @Override
    public boolean isCancelled() {
        return completableFuture.isCancelled();
    }

    /**
     * Returns true if this CompletableFuture completed exceptionally, in any way.
     */
    @Override
    public boolean isCompletedExceptionally() {
        return completableFuture.isCompletedExceptionally();
    }

    /**
     * Returns true if completed in any fashion: normally, exceptionally, or via cancellation.
     */
    @Override
    public boolean isDone() {
        return completableFuture.isDone();
    }

    @Override
    public String toString() {
        return String.format("KafkaFuture{future=%s}", completableFuture);
    }

    @Override
    public CompletableFuture<T> toCompletableFuture() {
        return completableFuture;
    }
}
