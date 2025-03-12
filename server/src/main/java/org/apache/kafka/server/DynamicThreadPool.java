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
package org.apache.kafka.server;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.server.config.AbstractKafkaConfig;
import org.apache.kafka.server.config.ReplicationConfigs;
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.server.config.ServerLogConfigs;

import java.util.Map;
import java.util.Set;

public class DynamicThreadPool {
    public static final Set<String> RECONFIGURABLE_CONFIGS = Set.of(
        ServerConfigs.NUM_IO_THREADS_CONFIG,
        ReplicationConfigs.NUM_REPLICA_FETCHERS_CONFIG,
        ServerLogConfigs.NUM_RECOVERY_THREADS_PER_DATA_DIR_CONFIG,
        ServerConfigs.BACKGROUND_THREADS_CONFIG
    );

    private DynamicThreadPool() {}

    public static void validateReconfiguration(AbstractKafkaConfig currentConfig, AbstractKafkaConfig newConfig) {
        for (Map.Entry<String, ?> entry : newConfig.values().entrySet()) {
            String key = entry.getKey();
            if (RECONFIGURABLE_CONFIGS.contains(key)) {
                int newValue = (int) entry.getValue();
                int oldValue = getValue(currentConfig, key);

                if (newValue != oldValue) {
                    String errorMsg = String.format("Dynamic thread count update validation failed for %s=%d", key, newValue);

                    if (newValue <= 0)
                        throw new ConfigException(String.format("%s, value should be at least 1", errorMsg));
                    if (newValue < oldValue / 2)
                        throw new ConfigException(String.format("%s, value should be at least half the current value %d", errorMsg, oldValue));
                    if (newValue > oldValue * 2)
                        throw new ConfigException(String.format("%s, value should not be greater than double the current value %d", errorMsg, oldValue));
                }
            }
        }
    }

    public static int getValue(AbstractKafkaConfig config, String name) {
        return switch (name) {
            case ServerConfigs.NUM_IO_THREADS_CONFIG -> config.numIoThreads();
            case ReplicationConfigs.NUM_REPLICA_FETCHERS_CONFIG -> config.numReplicaFetchers();
            case ServerLogConfigs.NUM_RECOVERY_THREADS_PER_DATA_DIR_CONFIG -> config.numRecoveryThreadsPerDataDir();
            case ServerConfigs.BACKGROUND_THREADS_CONFIG -> config.backgroundThreads();
            default -> throw new IllegalStateException(String.format("Unexpected config %s", name));
        };
    }
}
