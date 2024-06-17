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
package org.apache.kafka.common;

import org.apache.kafka.common.internals.KafkaFutureImpl;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A flexible future which supports call chaining and other asynchronous programming patterns.
 *
 * <h3>Relation to {@code CompletionStage}</h3>
 * <p>It is possible to obtain a {@code CompletionStage} from a
 * {@code KafkaFuture} instance by calling {@link #toCompletionStage()}.
 * If converting {@link KafkaFuture#whenComplete(BiConsumer)} or {@link KafkaFuture#thenApply(BaseFunction)} to
 * {@link CompletableFuture#whenComplete(java.util.function.BiConsumer)} or
 * {@link CompletableFuture#thenApply(java.util.function.Function)} be aware that the returned
 * {@code KafkaFuture} will fail with an {@code ExecutionException}, whereas a {@code CompletionStage} fails
 * with a {@code CompletionException}.
 */
public abstract class KafkaFuture<T> implements Future<T> {
    /**
     * A function which takes objects of type A and returns objects of type B.
     */
    @FunctionalInterface
    public interface BaseFunction<A, B> {
        B apply(A a);
    }

    /**
     * A function which takes objects of type A and returns objects of type B.
     *
     * @deprecated Since Kafka 3.0. Use the {@link BaseFunction} functional interface.
     */
    @Deprecated
    public static abstract class Function<A, B> implements BaseFunction<A, B> { }

    /**
     * A consumer of two different types of object.
     */
    @FunctionalInterface
    public interface BiConsumer<A, B> {
        void accept(A a, B b);
    }

    /** 
     * Returns a new KafkaFuture that is already completed with the given value.
     */
    public static <U> KafkaFuture<U> completedFuture(U value) {
        KafkaFuture<U> future = new KafkaFutureImpl<>();
        future.complete(value);
        return future;
    }

    /** 
     * Returns a new KafkaFuture that is completed when all the given futures have completed.  If
     * any future throws an exception, the returned future returns it.  If multiple futures throw
     * an exception, which one gets returned is arbitrarily chosen.
     */
    public static KafkaFuture<Void> allOf(KafkaFuture<?>... futures) {
        KafkaFutureImpl<Void> result = new KafkaFutureImpl<>();
        CompletableFuture.allOf(Arrays.stream(futures)
                .map(kafkaFuture -> {
                    // Safe since KafkaFuture's only subclass is KafkaFuture for which toCompletionStage()
                    // always return a CF.
                    return (CompletableFuture<?>) kafkaFuture.toCompletionStage();
                })
                .toArray(CompletableFuture[]::new)).whenComplete((value, ex) -> {
                    if (ex == null) {
                        result.complete(value);
                    } else {
                        // Have to unwrap the CompletionException which allOf() introduced
                        result.completeExceptionally(ex.getCause());
                    }
                });

        return result;
    }

    /**
     * Gets a {@code CompletionStage} with the same completion properties as this {@code KafkaFuture}.
     * The returned instance will complete when this future completes and in the same way
     * (with the same result or exception).
     *
     * <p>Calling {@code toCompletableFuture()} on the returned instance will yield a {@code CompletableFuture},
     * but invocation of the completion methods ({@code complete()} and other methods in the {@code complete*()}
     * and {@code obtrude*()} families) on that {@code CompletableFuture} instance will result in
     * {@code UnsupportedOperationException} being thrown. Unlike a "minimal" {@code CompletableFuture},
     * the {@code get*()} and other methods of {@code CompletableFuture} that are not inherited from
     * {@code CompletionStage} will work normally.
     *
     * <p>If you want to block on the completion of a KafkaFuture you should use
     * {@link #get()}, {@link #get(long, TimeUnit)} or {@link #getNow(Object)}, rather than calling
     * {@code .toCompletionStage().toCompletableFuture().get()} etc.
     *
     * @since Kafka 3.0
     */
    public abstract CompletionStage<T> toCompletionStage();

    /**
     * Returns a new KafkaFuture that, when this future completes normally, is executed with this
     * futures's result as the argument to the supplied function.
     *
     * The function may be invoked by the thread that calls {@code thenApply} or it may be invoked by the thread that
     * completes the future.
     */
    public abstract <R> KafkaFuture<R> thenApply(BaseFunction<T, R> function);

    /**
     * Prefer {@link KafkaFuture#thenApply(BaseFunction)} as this function is here for backwards compatibility reasons
     * and might be deprecated/removed in a future release.
     */
    public abstract <R> KafkaFuture<R> thenApply(Function<T, R> function);

    /**
     * Returns a new KafkaFuture with the same result or exception as this future, that executes the given action
     * when this future completes.
     *
     * When this future is done, the given action is invoked with the result (or null if none) and the exception
     * (or null if none) of this future as arguments.
     *
     * The returned future is completed when the action returns.
     * The supplied action should not throw an exception. However, if it does, the following rules apply:
     * if this future completed normally but the supplied action throws an exception, then the returned future completes
     * exceptionally with the supplied action's exception.
     * Or, if this future completed exceptionally and the supplied action throws an exception, then the returned future
     * completes exceptionally with this future's exception.
     *
     * The action may be invoked by the thread that calls {@code whenComplete} or it may be invoked by the thread that
     * completes the future.
     *
     * @param action the action to preform
     * @return the new future
     */
    public abstract KafkaFuture<T> whenComplete(BiConsumer<? super T, ? super Throwable> action);

    /**
     * If not already completed, sets the value returned by get() and related methods to the given
     * value.
     */
    protected abstract boolean complete(T newValue);

    /**
     * If not already completed, causes invocations of get() and related methods to throw the given
     * exception.
     */
    protected abstract boolean completeExceptionally(Throwable newException);

    /**
     * If not already completed, completes this future with a CancellationException.  Dependent
     * futures that have not already completed will also complete exceptionally, with a
     * CompletionException caused by this CancellationException.
     */
    @Override
    public abstract boolean cancel(boolean mayInterruptIfRunning);

    /**
     * Waits if necessary for this future to complete, and then returns its result.
     */
    @Override
    public abstract T get() throws InterruptedException, ExecutionException;

    /**
     * Waits if necessary for at most the given time for this future to complete, and then returns
     * its result, if available.
     */
    @Override
    public abstract T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException,
        TimeoutException;

    /**
     * Returns the result value (or throws any encountered exception) if completed, else returns
     * the given valueIfAbsent.
     */
    public abstract T getNow(T valueIfAbsent) throws InterruptedException, ExecutionException;

    /**
     * Returns true if this CompletableFuture was cancelled before it completed normally.
     */
    @Override
    public abstract boolean isCancelled();

    /**
     * Returns true if this CompletableFuture completed exceptionally, in any way.
     */
    public abstract boolean isCompletedExceptionally();

    /**
     * Returns true if completed in any fashion: normally, exceptionally, or via cancellation.
     */
    @Override
    public abstract boolean isDone();
}
