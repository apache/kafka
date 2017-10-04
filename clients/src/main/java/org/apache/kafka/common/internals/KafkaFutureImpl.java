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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.common.KafkaFuture;

/**
 * A flexible future which supports call chaining and other asynchronous programming patterns.
 * This will eventually become a thin shim on top of Java 8's CompletableFuture.
 */
public class KafkaFutureImpl<T> extends KafkaFuture<T> {
    /**
     * A convenience method that throws the current exception, wrapping it if needed.
     *
     * In general, KafkaFuture throws CancellationException and InterruptedException directly, and
     * wraps all other exceptions in an ExecutionException.
     */
    private static void wrapAndThrow(Throwable t) throws InterruptedException, ExecutionException {
        if (t instanceof CancellationException) {
            throw (CancellationException) t;
        } else if (t instanceof InterruptedException) {
            throw (InterruptedException) t;
        } else {
            throw new ExecutionException(t);
        }
    }

    private static class Applicant<A, B> extends BiConsumer<A, Throwable> {
        private final Function<A, B> function;
        private final KafkaFutureImpl<B> future;

        Applicant(Function<A, B> function, KafkaFutureImpl<B> future) {
            this.function = function;
            this.future = future;
        }

        @Override
        public void accept(A a, Throwable exception) {
            if (exception != null) {
                future.completeExceptionally(exception);
            } else {
                try {
                    B b = function.apply(a);
                    future.complete(b);
                } catch (Throwable t) {
                    future.completeExceptionally(t);
                }
            }
        }
    }

    private static class SingleWaiter<R> extends BiConsumer<R, Throwable> {
        private R value = null;
        private Throwable exception = null;
        private boolean done = false;

        @Override
        public synchronized void accept(R newValue, Throwable newException) {
            this.value = newValue;
            this.exception = newException;
            this.done = true;
            this.notifyAll();
        }

        synchronized R await() throws InterruptedException, ExecutionException {
            while (true) {
                if (exception != null)
                    wrapAndThrow(exception);
                if (done)
                    return value;
                this.wait();
            }
        }

        R await(long timeout, TimeUnit unit)
                throws InterruptedException, ExecutionException, TimeoutException {
            long startMs = System.currentTimeMillis();
            long waitTimeMs = (unit.toMillis(timeout) > 0) ? unit.toMillis(timeout) : 1;
            long delta = 0;
            synchronized (this) {
                while (true) {
                    if (exception != null)
                        wrapAndThrow(exception);
                    if (done)
                        return value;
                    if (delta > waitTimeMs) {
                        throw new TimeoutException();
                    }
                    this.wait(waitTimeMs - delta);
                    delta = System.currentTimeMillis() - startMs;
                }
            }
        }
    }

    /**
     * True if this future is done.
     */
    private boolean done = false;

    /**
     * The value of this future, or null.  Protected by the object monitor.
     */
    private T value = null;

    /**
     * The exception associated with this future, or null.  Protected by the object monitor.
     */
    private Throwable exception = null;

    /**
     * A list of objects waiting for this future to complete (either successfully or
     * exceptionally).  Protected by the object monitor.
     */
    private List<BiConsumer<? super T, ? super Throwable>> waiters = new ArrayList<>();

    /**
     * Returns a new KafkaFuture that, when this future completes normally, is executed with this
     * futures's result as the argument to the supplied function.
     */
    @Override
    public <R> KafkaFuture<R> thenApply(Function<T, R> function) {
        KafkaFutureImpl<R> future = new KafkaFutureImpl<R>();
        addWaiter(new Applicant<>(function, future));
        return future;
    }

    @Override
    protected synchronized void addWaiter(BiConsumer<? super T, ? super Throwable> action) {
        if (exception != null) {
            action.accept(null, exception);
        } else if (done) {
            action.accept(value, null);
        } else {
            waiters.add(action);
        }
    }

    @Override
    public synchronized boolean complete(T newValue) {
        List<BiConsumer<? super T, ? super Throwable>> oldWaiters = null;
        synchronized (this) {
            if (done)
                return false;
            value = newValue;
            done = true;
            oldWaiters = waiters;
            waiters = null;
        }
        for (BiConsumer<? super T, ? super Throwable> waiter : oldWaiters) {
            waiter.accept(newValue, null);
        }
        return true;
    }

    @Override
    public boolean completeExceptionally(Throwable newException) {
        List<BiConsumer<? super T, ? super Throwable>> oldWaiters = null;
        synchronized (this) {
            if (done)
                return false;
            exception = newException;
            done = true;
            oldWaiters = waiters;
            waiters = null;
        }
        for (BiConsumer<? super T, ? super Throwable> waiter : oldWaiters) {
            waiter.accept(null, newException);
        }
        return true;
    }

    /**
     * If not already completed, completes this future with a CancellationException.  Dependent
     * futures that have not already completed will also complete exceptionally, with a
     * CompletionException caused by this CancellationException.
     */
    @Override
    public synchronized boolean cancel(boolean mayInterruptIfRunning) {
        if (completeExceptionally(new CancellationException()))
            return true;
        return exception instanceof CancellationException;
    }

    /**
     * Waits if necessary for this future to complete, and then returns its result.
     */
    @Override
    public T get() throws InterruptedException, ExecutionException {
        SingleWaiter<T> waiter = new SingleWaiter<T>();
        addWaiter(waiter);
        return waiter.await();
    }

    /**
     * Waits if necessary for at most the given time for this future to complete, and then returns
     * its result, if available.
     */
    @Override
    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException,
            TimeoutException {
        SingleWaiter<T> waiter = new SingleWaiter<T>();
        addWaiter(waiter);
        return waiter.await(timeout, unit);
    }

    /**
     * Returns the result value (or throws any encountered exception) if completed, else returns
     * the given valueIfAbsent.
     */
    @Override
    public synchronized T getNow(T valueIfAbsent) throws InterruptedException, ExecutionException {
        if (exception != null)
            wrapAndThrow(exception);
        if (done)
            return value;
        return valueIfAbsent;
    }

    /**
     * Returns true if this CompletableFuture was cancelled before it completed normally.
     */
    @Override
    public synchronized boolean isCancelled() {
        return (exception != null) && (exception instanceof CancellationException);
    }

    /**
     * Returns true if this CompletableFuture completed exceptionally, in any way.
     */
    @Override
    public synchronized boolean isCompletedExceptionally() {
        return exception != null;
    }

    /**
     * Returns true if completed in any fashion: normally, exceptionally, or via cancellation.
     */
    @Override
    public synchronized boolean isDone() {
        return done;
    }
}
