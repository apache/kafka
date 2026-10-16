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
        // For update of topic config overrides, only config names and types are validated
        // Names and types have already been validated. For consistency with topic config
        // validation, no additional validation is performed.
        validateLogLocalRetentionMs(newConfig);
        validateLogLocalRetentionBytes(newConfig);
        validateLogRemoteCopyLagMs(newConfig);
        validateLogRemoteCopyLagBytes(newConfig);
        validateCordonedLogDirs(newConfig);
    }

    private void validateLogLocalRetentionMs(AbstractKafkaConfig newConfig) {
        long logRetentionMs = newConfig.logRetentionTimeMillis();
        long logLocalRetentionMs = newConfig.remoteLogManagerConfig().logLocalRetentionMs();
        if (logRetentionMs != -1L && logLocalRetentionMs != -2L) {
            if (logLocalRetentionMs == -1L) {
                throw new ConfigException(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_MS_PROP, logLocalRetentionMs,
                    "Value must not be -1 as " + ServerLogConfigs.LOG_RETENTION_TIME_MILLIS_CONFIG + " value is set as " + logRetentionMs + ".");
            }
            if (logLocalRetentionMs > logRetentionMs) {
                throw new ConfigException(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_MS_PROP, logLocalRetentionMs,
                    "Value must not be more than " + ServerLogConfigs.LOG_RETENTION_TIME_MILLIS_CONFIG + " property value: " + logRetentionMs);
            }
        }
    }

    private void validateLogLocalRetentionBytes(AbstractKafkaConfig newConfig) {
        long logRetentionBytes = newConfig.logRetentionBytes();
        long logLocalRetentionBytes = newConfig.remoteLogManagerConfig().logLocalRetentionBytes();
        if (logRetentionBytes > -1L && logLocalRetentionBytes != -2L) {
            if (logLocalRetentionBytes == -1L) {
                throw new ConfigException(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_BYTES_PROP, logLocalRetentionBytes,
                    "Value must not be -1 as " + ServerLogConfigs.LOG_RETENTION_BYTES_CONFIG + " value is set as " + logRetentionBytes + ".");
            }
            if (logLocalRetentionBytes > logRetentionBytes) {
                throw new ConfigException(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_BYTES_PROP, logLocalRetentionBytes,
                    "Value must not be more than " + ServerLogConfigs.LOG_RETENTION_BYTES_CONFIG + " property value: " + logRetentionBytes);
            }
        }
    }

    private void validateLogRemoteCopyLagMs(AbstractKafkaConfig newConfig) {
        long logRetentionMs = newConfig.logRetentionTimeMillis();
        long logLocalRetentionMs = newConfig.remoteLogManagerConfig().logLocalRetentionMs();
        long effectiveLocalRetentionMs = logLocalRetentionMs == -2L ? logRetentionMs : logLocalRetentionMs;
        long logRemoteCopyLagMs = newConfig.remoteLogManagerConfig().logRemoteCopyLagMs();
        if (logRemoteCopyLagMs > 0L && effectiveLocalRetentionMs >= 0L && logRemoteCopyLagMs > effectiveLocalRetentionMs) {
            throw new ConfigException(RemoteLogManagerConfig.LOG_REMOTE_COPY_LAG_MS_PROP, logRemoteCopyLagMs,
                "Value must not exceed " + RemoteLogManagerConfig.LOG_LOCAL_RETENTION_MS_PROP + " (effective value: " + effectiveLocalRetentionMs + ")");
        }
    }

    private void validateLogRemoteCopyLagBytes(AbstractKafkaConfig newConfig) {
        long logRetentionBytes = newConfig.logRetentionBytes();
        long logLocalRetentionBytes = newConfig.remoteLogManagerConfig().logLocalRetentionBytes();
        long effectiveLocalRetentionBytes = logLocalRetentionBytes == -2L ? logRetentionBytes : logLocalRetentionBytes;
        long logRemoteCopyLagBytes = newConfig.remoteLogManagerConfig().logRemoteCopyLagBytes();
        if (logRemoteCopyLagBytes > 0L && effectiveLocalRetentionBytes >= 0L && logRemoteCopyLagBytes > effectiveLocalRetentionBytes) {
            throw new ConfigException(RemoteLogManagerConfig.LOG_REMOTE_COPY_LAG_BYTES_PROP, logRemoteCopyLagBytes,
                "Value must not exceed " + RemoteLogManagerConfig.LOG_LOCAL_RETENTION_BYTES_PROP + " (effective value: " + effectiveLocalRetentionBytes + ")");
        }
    }

    private void validateCordonedLogDirs(AbstractKafkaConfig newConfig) {
        List<String> logDirs = newConfig.logDirs();
        List<String> cordonedLogDirs = newConfig.cordonedLogDirs();
        cordonedLogDirs.forEach(dir -> {
            if (!logDirs.contains(dir)) {
                throw new ConfigException(ServerLogConfigs.CORDONED_LOG_DIRS_CONFIG, cordonedLogDirs,
                    "Invalid entry in " + ServerLogConfigs.CORDONED_LOG_DIRS_CONFIG + ": " + dir + ". " +
                    "All cordoned log dirs must be entries of " + ServerLogConfigs.LOG_DIRS_CONFIG + " or " + ServerLogConfigs.LOG_DIR_CONFIG + ".");
            }
        });
    }

    private void updateLogsConfig(Map<String, Object> newBrokerDefaults) {
        logManager.brokerConfigUpdated();
        logManager.allLogs().forEach(log -> {
            Map<Object, Object> props = new HashMap<>(newBrokerDefaults);
            LogConfig logConfig = log.config();
            logConfig.originals().forEach((k, v) -> {
                if (logConfig.overriddenConfigs.contains(k)) {
                    props.put(k, v);
                }
            });
            log.updateConfig(new LogConfig(props, logConfig.overriddenConfigs));
        });
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
