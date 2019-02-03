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
package org.apache.kafka.connect.runtime;

import org.apache.kafka.connect.errors.ConnectException;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * A supplier for {@link Herder}s.
 */
public class HerderProvider {

    private final CountDownLatch initialized = new CountDownLatch(1);
    volatile Herder herder = null;

    public HerderProvider() {
    }

    /**
     * Create a herder provider with a herder.
     * @param herder the herder that will be supplied to threads waiting on this provider
     */
    public HerderProvider(Herder herder) {
        this.herder = herder;
        initialized.countDown();
    }

    /**
     * @return the contained herder.
     * @throws ConnectException if a herder was not available within a duration of calling this method
     */
    public Herder get() {
        try {
            // wait for herder to be initialized
            if (!initialized.await(1, TimeUnit.MINUTES)) {
                throw new ConnectException("Timed out waiting for herder to be initialized.");
            }
        } catch (InterruptedException e) {
            throw new ConnectException("Interrupted while waiting for herder to be initialized.", e);
        }
        return herder;
    }

    /**
     * @param herder set a herder, and signal to all threads waiting on get().
     */
    public void setHerder(Herder herder) {
        this.herder = herder;
        initialized.countDown();
    }

}
