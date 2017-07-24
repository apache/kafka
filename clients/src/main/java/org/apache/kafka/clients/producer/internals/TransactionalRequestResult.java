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


import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public final class TransactionalRequestResult {
    static final TransactionalRequestResult COMPLETE = new TransactionalRequestResult(new CountDownLatch(0));

    private final CountDownLatch latch;
    private volatile RuntimeException error = null;

    public TransactionalRequestResult() {
        this(new CountDownLatch(1));
    }

    private TransactionalRequestResult(CountDownLatch latch) {
        this.latch = latch;
    }

    public void setError(RuntimeException error) {
        this.error = error;
    }

    public void done() {
        this.latch.countDown();
    }

    public void await() {
        boolean completed = false;

        while (!completed) {
            try {
                latch.await();
                completed = true;
            } catch (InterruptedException e) {
                // Keep waiting until done, we have no other option for these transactional requests.
            }
        }

        if (!isSuccessful())
            throw error();
    }

    public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
        return latch.await(timeout, unit);
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
