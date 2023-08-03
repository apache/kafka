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
package org.apache.kafka.coordinator.group.runtime;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A concurrent event accumulator which group events per key and ensure that only one
 * event with a given key can't be processed concurrently.
 *
 * This class is threadsafe.
 *
 * @param <K> The type of the key of the event.
 * @param <T> The type of the event itself. It implements the {{@link Event}} interface.
 *
 * There are a few examples about how to use it in the unit tests.
 */
public class EventAccumulator<K, T extends EventAccumulator.Event<K>> implements AutoCloseable {

    /**
     * The interface which must be implemented by all events.
     *
     * @param <K> The type of the key of the event.
     */
    public interface Event<K> {
        K key();
    }

    /**
     * The random generator used by this class.
     */
    private final Random random;

    /**
     * The map of queues keyed by K.
     */
    private final Map<K, Queue<T>> queues;

    /**
     * The list of available keys. Keys in this list can
     * be delivered to pollers.
     */
    private final List<K> availableKeys;

    /**
     * The set of keys that are being processed.
     */
    private final Set<K> inflightKeys;

    /**
     * The lock for protecting access to the resources.
     */
    private final ReentrantLock lock;

    /**
     * The condition variable for waking up poller threads.
     */
    private final Condition condition;

    /**
     * The number of events in the accumulator.
     */
    private int size;

    /**
     * A boolean indicated whether the accumulator is closed.
     */
    private boolean closed;

    public EventAccumulator() {
        this(new Random());
    }

    public EventAccumulator(
        Random random
    ) {
        this.random = random;
        this.queues = new HashMap<>();
        this.availableKeys = new ArrayList<>();
        this.inflightKeys = new HashSet<>();
        this.closed = false;
        this.lock = new ReentrantLock();
        this.condition = lock.newCondition();
    }

    /**
     * Adds an {{@link Event}} to the queue.
     *
     * @param event An {{@link Event}}.
     */
    public void add(T event) throws RejectedExecutionException {
        lock.lock();
        try {
            if (closed) throw new RejectedExecutionException("Can't accept an event because the accumulator is closed.");

            K key = event.key();
            Queue<T> queue = queues.get(key);
            if (queue == null) {
                queue = new LinkedList<>();
                queues.put(key, queue);
                if (!inflightKeys.contains(key)) {
                    addAvailableKey(key);
                }
            }
            queue.add(event);
            size++;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns the next {{@link Event}} available. This method block indefinitely until
     * one event is ready or the accumulator is closed.
     *
     * @return The next event.
     */
    public T poll() {
        return poll(Long.MAX_VALUE, TimeUnit.SECONDS);
    }

    /**
     * Returns the next {{@link Event}} available. This method blocks for the provided
     * time and returns null of not event is available.
     *
     * @param timeout   The timeout.
     * @param unit      The timeout unit.
     * @return The next event available or null.
     */
    public T poll(long timeout, TimeUnit unit) {
        lock.lock();
        try {
            K key = randomKey();
            long nanos = unit.toNanos(timeout);
            while (key == null && !closed && nanos > 0) {
                try {
                    nanos = condition.awaitNanos(nanos);
                } catch (InterruptedException e) {
                    // Ignore.
                }
                key = randomKey();
            }

            if (key == null) return null;

            Queue<T> queue = queues.get(key);
            T event = queue.poll();

            if (queue.isEmpty()) queues.remove(key);
            inflightKeys.add(key);
            size--;

            return event;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Marks the event as processed and releases the next event
     * with the same key. This unblocks waiting threads.
     *
     * @param event The event that was processed.
     */
    public void done(T event) {
        lock.lock();
        try {
            K key = event.key();
            inflightKeys.remove(key);
            if (queues.containsKey(key)) {
                addAvailableKey(key);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns the size of the accumulator.
     */
    public int size() {
        lock.lock();
        try {
            return size;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Closes the accumulator. This unblocks all the waiting threads.
     */
    @Override
    public void close() {
        lock.lock();
        try {
            closed = true;
            condition.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Adds the key to the available keys set.
     *
     * This method must be called while holding the lock.
     */
    private void addAvailableKey(K key) {
        availableKeys.add(key);
        condition.signalAll();
    }

    /**
     * Returns the next available key. The key is selected randomly
     * from the available keys set.
     *
     * This method must be called while holding the lock.
     */
    private K randomKey() {
        if (availableKeys.isEmpty()) return null;

        int lastIndex = availableKeys.size() - 1;
        int randomIndex = random.nextInt(availableKeys.size());
        K randomKey = availableKeys.get(randomIndex);
        Collections.swap(availableKeys, randomIndex, lastIndex);
        availableKeys.remove(lastIndex);
        return randomKey;
    }
}
