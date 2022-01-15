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
package org.apache.kafka.clients.producer.internals;


import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.TimeoutException;

import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public final class TransactionalRequestResult {

    private final CountDownLatch latch;
    private volatile RuntimeException error = null;
    private final String operation;

    public TransactionalRequestResult(String operation) {
        this(new CountDownLatch(1), operation);
    }

    private TransactionalRequestResult(CountDownLatch latch, String operation) {
        this.latch = latch;
        this.operation = operation;
    }

    public void fail(RuntimeException error) {
        this.error = error;
        this.latch.countDown();
    }

    public void done() {
        this.latch.countDown();
    }

    public void await() {
        boolean completed = false;
        boolean interrupted = false;

        while (!completed) {
            try {
                latch.await();
                completed = true;
            } catch (InterruptedException e) {
                interrupted = true;
                // Keep waiting until done, we have no other option for these transactional requests.
            }
        }

        if (interrupted) {
            Thread.currentThread().interrupt();
        }

        if (!isSuccessful()) {
            throw error();
        }
    }

    public void await(long timeout, TimeUnit unit) {
        try {
            boolean success = latch.await(timeout, unit);
            if (!isSuccessful()) {
                throw error();
            }
            if (!success) {
                throw new TimeoutException("Timeout expired after " + timeout + " " + unit.name().toLowerCase(Locale.ROOT) + " while awaiting " + operation);
            }
        } catch (InterruptedException e) {
            throw new InterruptException("Received interrupt while awaiting " + operation, e);
        }
    }

    public RuntimeException error() {
        return error;
    }

    public boolean isSuccessful() {
        return error == null;
    }

    public boolean isCompleted() {
        return latch.getCount() == 0L;
    }

}
