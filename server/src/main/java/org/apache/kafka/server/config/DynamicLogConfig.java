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
package org.apache.kafka.server.config;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.server.common.DirectoryEventHandler;
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.internals.log.LogManager;
import org.apache.kafka.storage.internals.log.UnifiedLog;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DynamicLogConfig implements BrokerReconfigurable {
    /**
     * The broker configurations pertaining to logs that are reconfigurable. This set contains
     * the names you would use when setting a static or dynamic broker configuration (not topic
     * configuration).
     */
    public static final Set<String> RECONFIGURABLE_CONFIGS = Stream.of(
            ServerTopicConfigSynonyms.TOPIC_CONFIG_SYNONYMS.values(),
            Set.of(ServerLogConfigs.CORDONED_LOG_DIRS_CONFIG))
        .flatMap(Collection::stream)
        .collect(Collectors.toUnmodifiableSet());

    private final LogManager logManager;
    private final DirectoryEventHandler directoryEventHandler;

    public DynamicLogConfig(LogManager logManager, DirectoryEventHandler directoryEventHandler) {
        this.logManager = logManager;
        this.directoryEventHandler = directoryEventHandler;
    }

    @Override
    public Set<String> reconfigurableConfigs() {
        return RECONFIGURABLE_CONFIGS;
    }

    @Override
    public void validateReconfiguration(AbstractKafkaConfig newConfig) {
        validateLogLocalRetentionMs(newConfig);
        validateLogLocalRetentionBytes(newConfig);
        validateLogRemoteCopyLagMs(newConfig);
        validateLogRemoteCopyLagBytes(newConfig);
        validateCordonedLogDirs(newConfig);
    }

    private void validateLogLocalRetentionMs(AbstractKafkaConfig config) {
        long logRetentionMs = config.logRetentionTimeMillis();
        long logLocalRetentionMs = config.getLong(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_MS_PROP);
        if (logRetentionMs != LogConfig.NO_RETENTION_LIMIT && logLocalRetentionMs != LogConfig.DEFAULT_LOCAL_RETENTION_MS) {
            if (logLocalRetentionMs == LogConfig.NO_RETENTION_LIMIT) {
                throw new ConfigException(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_MS_PROP, logLocalRetentionMs,
                    "Value must not be " + LogConfig.NO_RETENTION_LIMIT + " as " + ServerLogConfigs.LOG_RETENTION_TIME_MILLIS_CONFIG + " value is set as " + logRetentionMs + ".");
            }
            if (logLocalRetentionMs > logRetentionMs) {
                throw new ConfigException(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_MS_PROP, logLocalRetentionMs,
                    "Value must not be more than " + ServerLogConfigs.LOG_RETENTION_TIME_MILLIS_CONFIG + " property value: " + logRetentionMs);
            }
        }
    }

    private void validateLogLocalRetentionBytes(AbstractKafkaConfig config) {
        long logRetentionBytes = config.logRetentionBytes();
        long logLocalRetentionBytes = config.getLong(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_BYTES_PROP);
        if (logRetentionBytes > LogConfig.NO_RETENTION_LIMIT && logLocalRetentionBytes != LogConfig.DEFAULT_LOCAL_RETENTION_BYTES) {
            if (logLocalRetentionBytes == LogConfig.NO_RETENTION_LIMIT) {
                throw new ConfigException(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_BYTES_PROP, logLocalRetentionBytes,
                    "Value must not be " + LogConfig.NO_RETENTION_LIMIT + " as " + ServerLogConfigs.LOG_RETENTION_BYTES_CONFIG + " value is set as " + logRetentionBytes + ".");
            }
            if (logLocalRetentionBytes > logRetentionBytes) {
                throw new ConfigException(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_BYTES_PROP, logLocalRetentionBytes,
                    "Value must not be more than " + ServerLogConfigs.LOG_RETENTION_BYTES_CONFIG + " property value: " + logRetentionBytes);
            }
        }
    }

    private void validateLogRemoteCopyLagMs(AbstractKafkaConfig config) {
        long logRetentionMs = config.logRetentionTimeMillis();
        long logLocalRetentionMs = config.getLong(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_MS_PROP);
        long effectiveLocalRetentionMs = logLocalRetentionMs == LogConfig.DEFAULT_LOCAL_RETENTION_MS ? logRetentionMs : logLocalRetentionMs;
        long logRemoteCopyLagMs = config.getLong(RemoteLogManagerConfig.LOG_REMOTE_COPY_LAG_MS_PROP);
        if (logRemoteCopyLagMs > 0L && effectiveLocalRetentionMs >= 0L && logRemoteCopyLagMs > effectiveLocalRetentionMs) {
            throw new ConfigException(RemoteLogManagerConfig.LOG_REMOTE_COPY_LAG_MS_PROP, logRemoteCopyLagMs,
                "Value must not exceed " + RemoteLogManagerConfig.LOG_LOCAL_RETENTION_MS_PROP +
                    " (effective value: " + effectiveLocalRetentionMs + ")");
        }
    }

    private void validateLogRemoteCopyLagBytes(AbstractKafkaConfig config) {
        long logRetentionBytes = config.logRetentionBytes();
        long logLocalRetentionBytes = config.getLong(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_BYTES_PROP);
        long effectiveLocalRetentionBytes = logLocalRetentionBytes == LogConfig.DEFAULT_LOCAL_RETENTION_BYTES ? logRetentionBytes : logLocalRetentionBytes;
        long logRemoteCopyLagBytes = config.getLong(RemoteLogManagerConfig.LOG_REMOTE_COPY_LAG_BYTES_PROP);
        if (logRemoteCopyLagBytes > 0L && effectiveLocalRetentionBytes >= 0L && logRemoteCopyLagBytes > effectiveLocalRetentionBytes) {
            throw new ConfigException(RemoteLogManagerConfig.LOG_REMOTE_COPY_LAG_BYTES_PROP, logRemoteCopyLagBytes,
                "Value must not exceed " + RemoteLogManagerConfig.LOG_LOCAL_RETENTION_BYTES_PROP +
                    " (effective value: " + effectiveLocalRetentionBytes + ")");
        }
    }

    private void validateCordonedLogDirs(AbstractKafkaConfig config) {
        List<String> cordonedLogDirs = config.cordonedLogDirs();
        List<String> logDirs = config.logDirs();
        for (String dir : cordonedLogDirs) {
            if (!logDirs.contains(dir)) {
                throw new ConfigException(ServerLogConfigs.CORDONED_LOG_DIRS_CONFIG, cordonedLogDirs,
                    "Invalid entry in " + ServerLogConfigs.CORDONED_LOG_DIRS_CONFIG + ": " + dir + ". " +
                        "All cordoned log dirs must be entries of " + ServerLogConfigs.LOG_DIRS_CONFIG + " or " +
                        ServerLogConfigs.LOG_DIR_CONFIG + ".");
            }
        }
    }

    private void updateLogsConfig(Map<String, Object> newBrokerDefaults) {
        logManager.brokerConfigUpdated();
        for (UnifiedLog unifiedLog : logManager.allLogs()) {
            Map<String, Object> props = new HashMap<>(newBrokerDefaults);
            unifiedLog.config().originals().forEach((key, value) -> {
                if (unifiedLog.config().overriddenConfigs.contains(key)) {
                    props.put(key, value);
                }
            });
            unifiedLog.updateConfig(new LogConfig(props, unifiedLog.config().overriddenConfigs));
        }
    }

    @Override
    public void reconfigure(AbstractKafkaConfig oldConfig, AbstractKafkaConfig newConfig) {
        Map<String, Object> newBrokerDefaults = new HashMap<>(newConfig.extractLogConfigMap());
        logManager.reconfigureDefaultLogConfig(new LogConfig(newBrokerDefaults));
        updateLogsConfig(newBrokerDefaults);

        logManager.updateCordonedLogDirs(Set.copyOf(newConfig.cordonedLogDirs()));
        directoryEventHandler.handleCordoned(newConfig.cordonedLogDirs().stream()
            .flatMap(dir -> logManager.directoryId(dir).stream())
            .collect(Collectors.toSet()));
    }

    @Override
    public String toString() {
        return "DynamicLogConfig";
    }
}
