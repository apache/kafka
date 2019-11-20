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
package org.apache.kafka.connect.util;

import org.apache.kafka.connect.errors.ConnectException;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public abstract class ConvertingFutureCallback<U, T> implements Callback<U>, Future<T> {

    private final Callback<T> underlying;
    private final CountDownLatch finishedLatch;
    private volatile T result = null;
    private volatile Throwable exception = null;
    private volatile boolean cancelled = false;

    public ConvertingFutureCallback() {
        this(null);
    }

    public ConvertingFutureCallback(Callback<T> underlying) {
        this.underlying = underlying;
        this.finishedLatch = new CountDownLatch(1);
    }

    public abstract T convert(U result);

    @Override
    public void onCompletion(Throwable error, U result) {
        synchronized (this) {
            if (isDone()) {
                return;
            }
            
            if (error != null) {
                this.exception = error;
            } else {
                this.result = convert(result);
            }

            if (underlying != null)
                underlying.onCompletion(error, this.result);
            finishedLatch.countDown();
        }
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        synchronized (this) {
            if (isDone()) {
                return false;
            }
            if (mayInterruptIfRunning) {
                this.cancelled = true;
                finishedLatch.countDown();
                return true;
            }
        }
        try {
            finishedLatch.await();
        } catch (InterruptedException e) {
            throw new ConnectException("Interrupted while waiting for task to complete", e);
        }
        return false;
    }

    @Override
    public boolean isCancelled() {
        return cancelled;
    }

    @Override
    public boolean isDone() {
        return finishedLatch.getCount() == 0;
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
        finishedLatch.await();
        return result();
    }

    @Override
    public T get(long l, TimeUnit timeUnit)
            throws InterruptedException, ExecutionException, TimeoutException {
        if (!finishedLatch.await(l, timeUnit))
            throw new TimeoutException("Timed out waiting for future");
        return result();
    }

    private T result() throws ExecutionException {
        if (cancelled) {
            throw new CancellationException();
        }
        if (exception != null) {
            throw new ExecutionException(exception);
        }
        return result;
    }
}

