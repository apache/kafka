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
package org.apache.kafka.connect.tools;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.connector.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * This connector provides support for mocking certain connector behaviors. For example,
 * this can be used to simulate connector or task failures. It works by passing a "mock mode"
 * through configuration from the system test. New mock behavior can be implemented either
 * in the connector or in the task by providing a new mode implementation.
 *
 * At the moment, this connector only supports a single task and shares configuration between
 * the connector and its tasks.
 *
 * @see MockSinkConnector
 * @see MockSourceConnector
 */
public class MockConnector extends Connector {
    public static final String MOCK_MODE_KEY = "mock_mode";
    public static final String DELAY_MS_KEY = "delay_ms";

    public static final String CONNECTOR_FAILURE = "connector-failure";
    public static final String TASK_FAILURE = "task-failure";

    public static final long DEFAULT_FAILURE_DELAY_MS = 15000;

    private static final Logger log = LoggerFactory.getLogger(MockConnector.class);

    private Map<String, String> config;
    private ScheduledExecutorService executor;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void start(Map<String, String> config) {
        this.config = config;

        if (CONNECTOR_FAILURE.equals(config.get(MOCK_MODE_KEY))) {
            // Schedule this connector to raise an exception after some delay

            String delayMsString = config.get(DELAY_MS_KEY);
            long delayMs = DEFAULT_FAILURE_DELAY_MS;
            if (delayMsString != null)
                delayMs = Long.parseLong(delayMsString);

            log.debug("Started MockConnector with failure delay of {} ms", delayMs);
            executor = Executors.newSingleThreadScheduledExecutor();
            executor.schedule(new Runnable() {
                @Override
                public void run() {
                    log.debug("Triggering connector failure");
                    context.raiseError(new RuntimeException());
                }
            }, delayMs, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public Class<? extends Task> taskClass() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        log.debug("Creating single task for MockConnector");
        return Collections.singletonList(config);
    }

    @Override
    public void stop() {
        if (executor != null) {
            executor.shutdownNow();

            try {
                if (!executor.awaitTermination(20, TimeUnit.SECONDS))
                    throw new RuntimeException("Failed timely termination of scheduler");
            } catch (InterruptedException e) {
                throw new RuntimeException("Task was interrupted during shutdown");
            }
        }
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

}
