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
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A flexible future which supports call chaining and other asynchronous programming patterns.
 */
public class KafkaFutureImpl<T> extends KafkaFuture<T> {

    private final KafkaCompletableFuture<T> completableFuture;

    private final boolean isDependant;

    public KafkaFutureImpl() {
        this(false, new KafkaCompletableFuture<>());
    }

    private KafkaFutureImpl(boolean isDependant, KafkaCompletableFuture<T> completableFuture) {
        this.isDependant = isDependant;
        this.completableFuture = completableFuture;
    }

    @Override
    public CompletionStage<T> toCompletionStage() {
        return completableFuture;
    }

    /**
     * Returns a new KafkaFuture that, when this future completes normally, is executed with this
     * futures's result as the argument to the supplied function.
     */
    @Override
    public <R> KafkaFuture<R> thenApply(BaseFunction<T, R> function) {
        CompletableFuture<R> appliedFuture = completableFuture.thenApply(value -> {
            try {
                return function.apply(value);
            } catch (Throwable t) {
                if (t instanceof CompletionException) {
                    // KafkaFuture#thenApply, when the function threw CompletionException should return
                    // an ExecutionException wrapping a CompletionException wrapping the exception thrown by the
                    // function. CompletableFuture#thenApply will just return ExecutionException wrapping the
                    // exception thrown by the function, so we add an extra CompletionException here to
                    // maintain the KafkaFuture behaviour.
                    throw new CompletionException(t);
                } else {
                    throw t;
                }
            }
        });
        return new KafkaFutureImpl<>(true, toKafkaCompletableFuture(appliedFuture));
    }

    private static <U> KafkaCompletableFuture<U> toKafkaCompletableFuture(CompletableFuture<U> completableFuture) {
        if (completableFuture instanceof KafkaCompletableFuture) {
            return (KafkaCompletableFuture<U>) completableFuture;
        } else {
            final KafkaCompletableFuture<U> result = new KafkaCompletableFuture<>();
            completableFuture.whenComplete((x, y) -> {
                if (y != null) {
                    result.kafkaCompleteExceptionally(y);
                } else {
                    result.kafkaComplete(x);
                }
            });
            return result;
        }
    }

    @Override
    public KafkaFuture<T> whenComplete(final BiConsumer<? super T, ? super Throwable> biConsumer) {
        CompletableFuture<T> tCompletableFuture = completableFuture.whenComplete((java.util.function.BiConsumer<? super T, ? super Throwable>) (a, b) -> {
            try {
                biConsumer.accept(a, b);
            } catch (Throwable t) {
                if (t instanceof CompletionException) {
                    throw new CompletionException(t);
                } else {
                    throw t;
                }
            }
        });
        return new KafkaFutureImpl<>(true, toKafkaCompletableFuture(tCompletableFuture));
    }


    @Override
    public boolean complete(T newValue) {
        return completableFuture.kafkaComplete(newValue);
    }

    @Override
    public boolean completeExceptionally(Throwable newException) {
        // CompletableFuture#get() always wraps the _cause_ of a CompletionException in ExecutionException
        // (which KafkaFuture does not) so wrap CompletionException in an extra one to avoid losing the
        // first CompletionException in the exception chain.
        return completableFuture.kafkaCompleteExceptionally(
                newException instanceof CompletionException ? new CompletionException(newException) : newException);
    }

    /**
     * If not already completed, completes this future with a CancellationException.  Dependent
     * futures that have not already completed will also complete exceptionally, with a
     * CompletionException caused by this CancellationException.
     */
    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return completableFuture.cancel(mayInterruptIfRunning);
    }

    /**
     * We need to deal with differences between KafkaFuture's historic API and the API of CompletableFuture:
     * CompletableFuture#get() does not wrap CancellationException in ExecutionException (nor does KafkaFuture).
     * CompletableFuture#get() always wraps the _cause_ of a CompletionException in ExecutionException
     * (which KafkaFuture does not).
     *
     * The semantics for KafkaFuture are that all exceptional completions of the future (via #completeExceptionally()
     * or exceptions from dependants) manifest as ExecutionException, as observed via both get() and getNow().
     */
    private void maybeThrowCancellationException(Throwable cause) {
        if (cause instanceof CancellationException) {
            throw (CancellationException) cause;
        }
    }

    /**
     * Waits if necessary for this future to complete, and then returns its result.
     */
    @Override
    public T get() throws InterruptedException, ExecutionException {
        try {
            return completableFuture.get();
            // In Java 23, When a CompletableFuture is cancelled, get() will throw a CancellationException wrapping a
            // CancellationException, thus we need to unwrap it to maintain the KafkaFuture behaviour. 
            // see https://bugs.openjdk.org/browse/JDK-8331987
        } catch (ExecutionException | CancellationException e) {
            maybeThrowCancellationException(e.getCause());
            throw e;
        }
    }

    /**
     * Waits if necessary for at most the given time for this future to complete, and then returns
     * its result, if available.
     */
    @Override
    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException,
            TimeoutException {
        try {
            return completableFuture.get(timeout, unit);
            // In Java 23, When a CompletableFuture is cancelled, get() will throw a CancellationException wrapping a
            // CancellationException, thus we need to unwrap it to maintain the KafkaFuture behaviour. 
            // see https://bugs.openjdk.org/browse/JDK-8331987
        } catch (ExecutionException | CancellationException e) {
            maybeThrowCancellationException(e.getCause());
            throw e;
        }
    }

    /**
     * Returns the result value (or throws any encountered exception) if completed, else returns
     * the given valueIfAbsent.
     */
    @Override
    public T getNow(T valueIfAbsent) throws ExecutionException {
        try {
            return completableFuture.getNow(valueIfAbsent);
        } catch (CancellationException e) {
            // In Java 23, When a CompletableFuture is cancelled, getNow() will throw a CancellationException wrapping a 
            // CancellationException. whereas in Java < 23, it throws a CompletionException directly.
            // see https://bugs.openjdk.org/browse/JDK-8331987
            if (e.getCause() instanceof CancellationException) {
                throw (CancellationException) e.getCause();
            } else {
                throw e;
            }
        } catch (CompletionException e) {
            maybeThrowCancellationException(e.getCause());
            // Note, unlike CompletableFuture#get() which throws ExecutionException, CompletableFuture#getNow()
            // throws CompletionException, thus needs rewrapping to conform to KafkaFuture API,
            // where KafkaFuture#getNow() throws ExecutionException.
            throw new ExecutionException(e.getCause());
        }
    }

    /**
     * Returns true if this CompletableFuture was cancelled before it completed normally.
     */
    @Override
    public boolean isCancelled() {
        if (isDependant) {
            // Having isCancelled() for a dependent future just return
            // CompletableFuture.isCancelled() would break the historical KafkaFuture behaviour because
            // CompletableFuture#isCancelled() just checks for the exception being CancellationException
            // whereas it will be a CompletionException wrapping a CancellationException
            // due needing to compensate for CompletableFuture's CompletionException unwrapping
            // shenanigans in other methods.
            try {
                completableFuture.getNow(null);
                return false;
            } catch (Exception e) {
                return e instanceof CompletionException
                        && e.getCause() instanceof CancellationException;
            }
        } else {
            return completableFuture.isCancelled();
        }
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
        T value = null;
        Throwable exception = null;
        try {
            value = completableFuture.getNow(null);
        } catch (CancellationException e) {
            // In Java 23, When a CompletableFuture is cancelled, getNow() will throw a CancellationException wrapping a 
            // CancellationException. whereas in Java < 23, it throws a CompletionException directly.
            // see https://bugs.openjdk.org/browse/JDK-8331987
            if (e.getCause() instanceof CancellationException) {
                exception = e.getCause();
            } else { 
                exception = e;
            }
        } catch (CompletionException e) {
            exception = e.getCause();
        } catch (Exception e) {
            exception = e;
        }
        return String.format("KafkaFuture{value=%s,exception=%s,done=%b}", value, exception, exception != null || value != null);
    }
}
