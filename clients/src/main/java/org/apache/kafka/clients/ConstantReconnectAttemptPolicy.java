/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients;

import org.apache.kafka.common.config.AbstractConfig;

/**
 * A policy which used a constant delay between each reconnection attempt.
 */
public class ConstantReconnectAttemptPolicy implements ReconnectAttemptPolicy {

    private long reconnectBackoffMs;

    /**
     * Creates a new {@link ConstantReconnectAttemptPolicy} instance.
     */
    public ConstantReconnectAttemptPolicy() {

    }

    /**
     * Creates a new {@link ConstantReconnectAttemptPolicy} instance.
     */
    public ConstantReconnectAttemptPolicy(long reconnectBackoffMs) {
        setReconnectBackoffMs(reconnectBackoffMs);
    }

    @Override
    public void configure(AbstractConfig configs) {
        Long reconnectBackoffMs = configs.getLong(CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG);
        setReconnectBackoffMs(reconnectBackoffMs);
    }

    private void setReconnectBackoffMs(long reconnectBackoffMs) {
        if (reconnectBackoffMs < 0)
            throw new IllegalArgumentException(String.format("Delay must be positive - reconnect.backoff.ms %d", reconnectBackoffMs));
        this.reconnectBackoffMs = reconnectBackoffMs;
    }

    @Override
    public ReconnectAttemptScheduler newScheduler() {
        return new ConstantScheduler();
    }

    private class ConstantScheduler implements ReconnectAttemptScheduler {

        @Override
        public long nextReconnectBackoffMs() {
            return reconnectBackoffMs;
        }
    }
}


