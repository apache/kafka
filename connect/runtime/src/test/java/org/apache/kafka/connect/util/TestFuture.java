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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class TestFuture<T> implements Future<T> {
    private volatile boolean resolved;
    private T result;
    private Throwable exception;
    private final CountDownLatch getCalledLatch;

    public TestFuture() {
        resolved = false;
        getCalledLatch = new CountDownLatch(1);
    }

    public void resolve(T val) {
        this.result = val;
        resolved = true;
        synchronized (this) {
            this.notifyAll();
        }
    }

    public void resolve(Throwable t) {
        exception = t;
        resolved = true;
        synchronized (this) {
            this.notifyAll();
        }
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        return resolved;
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
        getCalledLatch.countDown();
        while (true) {
            try {
                return get(Integer.MAX_VALUE, TimeUnit.DAYS);
            } catch (TimeoutException e) {
                // ignore
            }
        }
    }

    @Override
    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        getCalledLatch.countDown();

        synchronized (this) {
            while (!resolved) {
                this.wait(TimeUnit.MILLISECONDS.convert(timeout, unit));
            }
        }

        if (exception != null) {
            if (exception instanceof TimeoutException)
                throw (TimeoutException) exception;
            else if (exception instanceof InterruptedException)
                throw (InterruptedException) exception;
            else
                throw new ExecutionException(exception);
        }
        return result;
    }
}
