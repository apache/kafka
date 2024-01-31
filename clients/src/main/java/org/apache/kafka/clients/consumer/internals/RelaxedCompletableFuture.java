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
package org.apache.kafka.clients.consumer.internals;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class RelaxedCompletableFuture<T> extends CompletableFuture<T> {

    private final Lock lock;
    private final Condition attemptedCondition;
    private boolean wasAttempted;

    public RelaxedCompletableFuture() {
        this.lock = new ReentrantLock();
        this.attemptedCondition = lock.newCondition();
    }

    public void attempted() {
        try {
            lock.lock();
            wasAttempted = true;
            attemptedCondition.signalAll();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
        waitForAttempt();
        return super.get();
    }

    @Override
    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        waitForAttempt();

        if (super.isDone()) {
            return super.get();
        } else {
            return super.get(timeout, unit);
        }
    }

    private void waitForAttempt() throws InterruptedException {
        try {
            lock.lock();

            if (!wasAttempted)
                attemptedCondition.await();
        } finally {
            lock.unlock();
        }
    }
}
