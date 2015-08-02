/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.copycat.util;

import java.util.concurrent.*;

public class FutureCallback<T> implements Callback<T>, Future<T> {

    private Callback<T> underlying;
    private CountDownLatch finishedLatch;
    private T result = null;
    private Throwable exception = null;

    public FutureCallback(Callback<T> underlying) {
        this.underlying = underlying;
        this.finishedLatch = new CountDownLatch(1);
    }

    @Override
    public void onCompletion(Throwable error, T result) {
        underlying.onCompletion(error, result);
        this.exception = error;
        this.result = result;
        finishedLatch.countDown();
    }

    @Override
    public boolean cancel(boolean b) {
        return false;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        return finishedLatch.getCount() == 0;
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
        finishedLatch.await();
        return getResult();
    }

    @Override
    public T get(long l, TimeUnit timeUnit)
            throws InterruptedException, ExecutionException, TimeoutException {
        finishedLatch.await(l, timeUnit);
        return getResult();
    }

    private T getResult() throws ExecutionException {
        if (exception != null) {
            throw new ExecutionException(exception);
        }
        return result;
    }
}
