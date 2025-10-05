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

import org.apache.kafka.common.utils.KafkaThread;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Base class for heartbeat threads. This class provides a mechanism to enable/disable the heartbeat thread.
 * The heartbeat thread should check whether it's enabled by calling {@link BaseHeartbeatThread#isEnabled()}
 * before sending heartbeat requests.
 */
public class BaseHeartbeatThread extends KafkaThread implements AutoCloseable {
    private final AtomicBoolean enabled = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicReference<RuntimeException> failureCause = new AtomicReference<>(null);

    public BaseHeartbeatThread(String name, boolean daemon) {
        super(name, daemon);
    }

    public void enable() {
        enabled.set(true);
    }

    public void disable() {
        enabled.set(false);
    }

    public boolean isEnabled() {
        return enabled.get();
    }

    public void setFailureCause(RuntimeException e) {
        failureCause.set(e);
    }

    public boolean isFailed() {
        return failureCause.get() != null;
    }

    public RuntimeException failureCause() {
        return failureCause.get();
    }

    public void close() {
        closed.set(true);
    }

    public boolean isClosed() {
        return closed.get();
    }
}
