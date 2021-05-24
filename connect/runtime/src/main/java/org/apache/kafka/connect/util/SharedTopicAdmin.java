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

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.connect.errors.ConnectException;

/**
 * A holder of a {@link TopicAdmin} object that is lazily and atomically created when needed by multiple callers.
 * As soon as one of the getters is called, all getters will return the same shared {@link TopicAdmin}
 * instance until this SharedAdmin is closed via {@link #close()} or {@link #close(Duration)}.
 *
 * <p>The owner of this object is responsible for ensuring that either {@link #close()} or {@link #close(Duration)}
 * is called when the {@link TopicAdmin} instance is no longer needed. Consequently, once this
 * {@link SharedTopicAdmin} instance has been closed, the {@link #get()} and {@link #topicAdmin()} methods,
 * nor any previously returned {@link TopicAdmin} instances may be used.
 *
 * <p>This class is thread-safe. It also appears as immutable to callers that obtain the {@link TopicAdmin} object,
 * until this object is closed, at which point it cannot be used anymore
 */
public class SharedTopicAdmin implements AutoCloseable, Supplier<TopicAdmin> {

    // Visible for testing
    static final Duration DEFAULT_CLOSE_DURATION = Duration.ofMillis(Long.MAX_VALUE);

    private final Map<String, Object> adminProps;
    private final AtomicReference<TopicAdmin> admin = new AtomicReference<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Function<Map<String, Object>, TopicAdmin> factory;

    public SharedTopicAdmin(Map<String, Object> adminProps) {
        this(adminProps, TopicAdmin::new);
    }

    // Visible for testing
    SharedTopicAdmin(Map<String, Object> adminProps, Function<Map<String, Object>, TopicAdmin> factory) {
        this.adminProps = Objects.requireNonNull(adminProps);
        this.factory = Objects.requireNonNull(factory);
    }

    /**
     * Get the shared {@link TopicAdmin} instance.
     *
     * @return the shared instance; never null
     * @throws ConnectException if this object has already been closed
     */
    @Override
    public TopicAdmin get() {
        return topicAdmin();
    }

    /**
     * Get the shared {@link TopicAdmin} instance.
     *
     * @return the shared instance; never null
     * @throws ConnectException if this object has already been closed
     */
    public TopicAdmin topicAdmin() {
        return admin.updateAndGet(this::createAdmin);
    }

    /**
     * Get the string containing the list of bootstrap server addresses to the Kafka broker(s) to which
     * the admin client connects.
     *
     * @return the bootstrap servers as a string; never null
     */
    public String bootstrapServers() {
        return adminProps.getOrDefault(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "<unknown>").toString();
    }

    /**
     * Close the underlying {@link TopicAdmin} instance, if one has been created, and prevent new ones from being created.
     *
     * <p>Once this method is called, the {@link #get()} and {@link #topicAdmin()} methods,
     * nor any previously returned {@link TopicAdmin} instances may be used.
     */
    @Override
    public void close() {
        close(DEFAULT_CLOSE_DURATION);
    }

    /**
     * Close the underlying {@link TopicAdmin} instance, if one has been created, and prevent new ones from being created.
     *
     * <p>Once this method is called, the {@link #get()} and {@link #topicAdmin()} methods,
     * nor any previously returned {@link TopicAdmin} instances may be used.
     *
     * @param timeout the maximum time to wait while the underlying admin client is closed; may not be null
     */
    public void close(Duration timeout) {
        Objects.requireNonNull(timeout);
        if (this.closed.compareAndSet(false, true)) {
            TopicAdmin admin = this.admin.getAndSet(null);
            if (admin != null) {
                admin.close(timeout);
            }
        }
    }

    @Override
    public String toString() {
        return "admin client for brokers at " + bootstrapServers();
    }

    /**
     * Method used to create a {@link TopicAdmin} instance. This method must be side-effect free, since it is called from within
     * the {@link AtomicReference#updateAndGet(UnaryOperator)}.
     *
     * @param existing the existing instance; may be null
     * @return the
     */
    protected TopicAdmin createAdmin(TopicAdmin existing) {
        if (closed.get()) {
            throw new ConnectException("The " + this + " has already been closed and cannot be used.");
        }
        if (existing != null) {
            return existing;
        }
        return factory.apply(adminProps);
    }
}
