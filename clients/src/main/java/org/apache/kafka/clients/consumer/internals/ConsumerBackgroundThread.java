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

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.GroupRebalanceConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;

import java.util.Locale;
import java.util.Optional;

public class ConsumerBackgroundThread<K,V> extends KafkaThread implements AutoCloseable {
    private static final String CONSUMER_BACKGROUND_THREAD_PREFIX = "consumer_background_thread";
    private Time time;
    private final LogContext logContext;
    private Logger log;
    private GroupRebalanceConfig groupRebalanceConfig;

    // Configurations
    private String clientId;
    private Optional<String> groupId;
    private long retryBackoffMs;
    private int heartbeatIntervalMs;
    private int requestTimeoutMs;
    private IsolationLevel isolationLevel;

    // control variables
    private volatile boolean closed = false;

    public ConsumerBackgroundThread(ConsumerConfig config) {
        super(CONSUMER_BACKGROUND_THREAD_PREFIX, true);
        this.time = Time.SYSTEM;
        configuration(config);
        this.logContext = initializeLogContext(config);
        this.log = logContext.logger(getClass());

    }

    public ConsumerBackgroundThread(ConsumerConfig config, Time time) {
        this(config);
        this.time = time;
    }

    @Override
    public void run() {
        try {
            // main loop
        } catch(Exception e) {
            // Handle Exceptions
        }
    }

    @Override
    public void close() throws Exception {
        this.closed = true;
    }

    private void configuration(ConsumerConfig config) {
        this.clientId = config.getString(CommonClientConfigs.CLIENT_ID_CONFIG);
        this.retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);
        this.heartbeatIntervalMs = config.getInt(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG);
        this.requestTimeoutMs = config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG);
        this.isolationLevel = IsolationLevel.valueOf(
                config.getString(ConsumerConfig.ISOLATION_LEVEL_CONFIG).toUpperCase(Locale.ROOT));

        this.groupRebalanceConfig = new GroupRebalanceConfig(config,
                GroupRebalanceConfig.ProtocolType.CONSUMER);
        this.groupId = Optional.ofNullable(groupRebalanceConfig.groupId);
    }

    private LogContext initializeLogContext(ConsumerConfig config) {
        return groupRebalanceConfig.groupInstanceId.map(
                        s -> new LogContext("[Consumer instanceId=" + s + ", clientId=" + clientId + ", groupId=" + groupId.orElse("null") + "] "))
                .orElseGet(
                        () -> new LogContext("[Consumer clientId=" + clientId + ", groupId=" + groupId.orElse("null") + "] "));

    }
}