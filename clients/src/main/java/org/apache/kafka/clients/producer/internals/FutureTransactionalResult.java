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

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public final class FutureTransactionalResult implements Future<TransactionalRequestResult> {

    private final TransactionalRequestResult result;

    public FutureTransactionalResult(TransactionalRequestResult result) {
        this.result = result;
    }

    @Override
    public boolean isDone() {
        return this.result.isCompleted();
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    @Override
    public TransactionalRequestResult get() {
        this.result.await();
        if (!result.isSuccessful()) {
            throw result.error();
        }
        return result;
    }

    @Override
    public TransactionalRequestResult get(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException {
        boolean occurred = this.result.await(timeout, unit);
        if (!occurred) {
            throw new TimeoutException("Could not complete transactional operation within " + TimeUnit.MILLISECONDS.convert(timeout, unit) + "ms.");
        }
        return result;
    }

}
