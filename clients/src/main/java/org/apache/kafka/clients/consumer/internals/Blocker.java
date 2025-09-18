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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.utils.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Blocker<T> {

    private final Logger log;
    private final Lock lock;
    private final Condition condition;
    private final AtomicBoolean wokenup = new AtomicBoolean(false);
    private T value;
    private KafkaException error;

    public Blocker() {
        this.log  = LoggerFactory.getLogger(getClass());
        this.lock = new ReentrantLock();
        this.condition = lock.newCondition();
    }

    public boolean complete(T value) {
        Objects.requireNonNull(value);

        try {
            lock.lock();

            if (isSet())
                return false;

            log.debug("Setting value to {}", value);
            this.value = value;
            wokenup.set(true);
            condition.signalAll();
            return true;
        } finally {
            lock.unlock();
        }
    }

    public boolean completeExceptionally(KafkaException error) {
        Objects.requireNonNull(error);

        try {
            lock.lock();

            if (isSet())
                return false;

            log.debug("Setting exception to {}", String.valueOf(error));
            this.error = error;
            wokenup.set(true);
            condition.signalAll();
            return true;
        } finally {
            lock.unlock();
        }
    }

    private boolean isSet() {
        return error != null || value != null;
    }

    /**
     * Allows the caller to await a response from the broker for requested data. The method will block, returning only
     * under one of the following conditions:
     *
     * <ol>
     *     <li>The buffer was already woken</li>
     *     <li>The buffer was woken during the wait</li>
     *     <li>The remaining time on the {@link Timer timer} elapsed</li>
     *     <li>The thread was interrupted</li>
     * </ol>
     *
     * @param timer Timer that provides time to wait
     */
    public T await(Timer timer) {
        try {
            lock.lock();

            log.debug("At start of method, error: {}, value: {}", error, value);

            if (error != null)
                throw error;
            else if (value != null)
                return value;

            while (!wokenup.compareAndSet(true, false)) {
                // Update the timer before we head into the loop in case it took a while to get the lock.
                timer.update();

                if (timer.isExpired()) {
                    // If the thread was interrupted before we start waiting, it still counts as
                    // interrupted from the point of view of the KafkaConsumer.poll(Duration) contract.
                    // We only need to check this when we are not going to wait because waiting
                    // already checks whether the thread is interrupted.
                    if (Thread.interrupted())
                        throw error = new InterruptException("Interrupted waiting for completion");

                    break;
                }

                if (!condition.await(timer.remainingMs(), TimeUnit.MILLISECONDS)) {
                    break;
                }
            }

            log.debug("At end of method, error: {}, value: {}", error, value);

            if (error != null)
                throw error;
            else if (value != null)
                return value;

            throw error = new TimeoutException("Timed out waiting for completion");
        } catch (InterruptedException e) {
            throw new InterruptException("Interrupted waiting for completion", e);
        } finally {
            lock.unlock();
            timer.update();
        }
    }

    @Override
    public String toString() {
        return "Blocker{" +
            "value=" + value +
            ", error=" + error +
            '}';
    }
}
