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

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.common.KafkaFuture;

/**
 * A flexible future which supports call chaining and other asynchronous programming patterns.
 */
// CF#completeExceptionally() does not treat Cancellation or CompletionException specially
// On the other hand, when the callback methods of dependent futures fail, the dependant future is failed via a different code path which
// will wrap anything that's not already a CompletionException in a CompletionException (as required by the contract of CompletionStage).
// KF on dependants doesn't treat CompletionException specially, which creates a problem for implementing KF using CF.
// We would need to wrap in an additional CompletionException.

// For consumers of results there are som other wrinkles:
// CF#get() does not wrap CancellationException in EE (nor does KF)
// CF#get() always wraps the _cause_ of a CompletionException in EE (which KF does not)
// The semantics for KafkaFuture are that all exceptional completions of the future (via #completeExceptionally() or exceptions from dependants)
// manifest as ExecutionException, as observed via both get() and getNow().

public class KafkaFutureImpl<T> extends KafkaFuture<T> {

    private final CompletableFuture<T> completableFuture;
    private final boolean isDependant;

    public KafkaFutureImpl() {
        this(false, new CompletableFuture<>());
    }

    private KafkaFutureImpl(boolean isDependant, CompletableFuture<T> completableFuture) {
        this.isDependant = isDependant;
        this.completableFuture = completableFuture;
    }

    /**
     * Returns a new KafkaFuture that, when this future completes normally, is executed with this
     * futures's result as the argument to the supplied function.
     */
    @Override
    public <R> KafkaFuture<R> thenApply(BaseFunction<T, R> function) {
        return new KafkaFutureImpl<>(true, completableFuture.thenApply(value ->  {
            try {
                return function.apply(value);
            } catch (Throwable t) {
                if (t instanceof CompletionException) {
                    throw new CompletionException(t);
                } else {
                    throw t;
                }
            }
        }));
    }

    /**
     * @see KafkaFutureImpl#thenApply(BaseFunction)
     */
    @Override
    public <R> KafkaFuture<R> thenApply(Function<T, R> function) {
        return thenApply((BaseFunction<T, R>) function);
    }

    @Override
    public KafkaFuture<T> whenComplete(final BiConsumer<? super T, ? super Throwable> biConsumer) {
        return new KafkaFutureImpl<>(true, completableFuture.whenComplete((a, b) -> {
            try {
                biConsumer.accept(a, b);
            } catch (Throwable t) {
                if (t instanceof CompletionException) {
                    throw new CompletionException(t);
                } else {
                    throw t;
                }
            }
        }));
    }

    @Override
    public synchronized boolean complete(T newValue) {
        return completableFuture.complete(newValue);
    }

    @Override
    public boolean completeExceptionally(Throwable newException) {
        // CF#get() always wraps the _cause_ of a CompletionException in EE (which KF does not)
        // so wrap CompletionException in an extra one to avoid losing the first CompletionException
        // in the exception chain.
        return completableFuture.completeExceptionally(newException instanceof CompletionException ? new CompletionException(newException) : newException);
    }

    /**
     * If not already completed, completes this future with a CancellationException.  Dependent
     * futures that have not already completed will also complete exceptionally, with a
     * CompletionException caused by this CancellationException.
     */
    @Override
    public synchronized boolean cancel(boolean mayInterruptIfRunning) {
        return completableFuture.cancel(mayInterruptIfRunning);
    }

    /**
     * Waits if necessary for this future to complete, and then returns its result.
     */
    @Override
    public T get() throws InterruptedException, ExecutionException {
        try {
            return completableFuture.get();
        } catch (ExecutionException e) {
            maybeRewrapAndThrow(e.getCause());
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
        } catch (ExecutionException e) {
            maybeRewrapAndThrow(e.getCause());
            throw e;
        }
    }

    /**
     * Returns the result value (or throws any encountered exception) if completed, else returns
     * the given valueIfAbsent.
     */
    @Override
    public synchronized T getNow(T valueIfAbsent) throws InterruptedException, ExecutionException {
        try {
            return completableFuture.getNow(valueIfAbsent);
        } catch (CompletionException e) {
            maybeRewrapAndThrow(e.getCause());
            // Note, unlike CF#get() which throws ExecutionException, CF#getNow() throws CompletionException
            // thus needs rewrapping to conform to KafkaFuture API, where KF#getNow() throws ExecutionException.
            throw new ExecutionException(e.getCause());
        }
    }

    private void maybeRewrapAndThrow(Throwable cause) {
        if (cause instanceof CancellationException) {
            throw (CancellationException) cause;
        }
    }

    /**
     * Returns true if this CompletableFuture was cancelled before it completed normally.
     */
    @Override
    public synchronized boolean isCancelled() {
        if (isDependant) {
            Throwable exception;
            try {
                completableFuture.getNow(null);
                return false;
            } catch (Exception e) {
                exception = e;
            }
            return exception instanceof CompletionException
                    && exception.getCause() instanceof CancellationException;
        } else {
            return completableFuture.isCancelled();
        }
    }

    /**
     * Returns true if this CompletableFuture completed exceptionally, in any way.
     */
    @Override
    public synchronized boolean isCompletedExceptionally() {
        return completableFuture.isCompletedExceptionally();
    }

    /**
     * Returns true if completed in any fashion: normally, exceptionally, or via cancellation.
     */
    @Override
    public synchronized boolean isDone() {
        return completableFuture.isDone();
    }

    @Override
    public String toString() {
        T value = null;
        Throwable ex = null;
        try {
            value = completableFuture.getNow(null);
        } catch (CompletionException e) {
            ex = e.getCause();
        } catch (Exception e) {
            ex = e;
        }
        return String.format("KafkaFuture{value=%s,exception=%s,done=%b}", value, ex, ex != null || value != null);
    }
}
