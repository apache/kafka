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

import org.apache.kafka.common.Reconfigurable;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.raft.KRaftConfigs;
import org.apache.kafka.server.common.DirectoryEventHandler;
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig;
import org.apache.kafka.storage.internals.log.LogManager;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

public class DynamicLogConfigTest {

    private final DynamicLogConfig dynamicLogConfig = new DynamicLogConfig(
        mock(LogManager.class),
        mock(DirectoryEventHandler.class)
    );

    @Test
    public void testValidateLogLocalRetentionMs() {
        assertInvalidConfig(
            Map.of(
                ServerLogConfigs.LOG_RETENTION_TIME_MILLIS_CONFIG, "1000",
                RemoteLogManagerConfig.LOG_LOCAL_RETENTION_MS_PROP, "-1"
            ),
            "Invalid value -1 for configuration " + RemoteLogManagerConfig.LOG_LOCAL_RETENTION_MS_PROP +
                ": Value must not be -1 as " + ServerLogConfigs.LOG_RETENTION_TIME_MILLIS_CONFIG +
                " value is set as 1000."
        );

        assertInvalidConfig(
            Map.of(
                ServerLogConfigs.LOG_RETENTION_TIME_MILLIS_CONFIG, "1000",
                RemoteLogManagerConfig.LOG_LOCAL_RETENTION_MS_PROP, "1001"
            ),
            "Invalid value 1001 for configuration " + RemoteLogManagerConfig.LOG_LOCAL_RETENTION_MS_PROP +
                ": Value must not be more than " + ServerLogConfigs.LOG_RETENTION_TIME_MILLIS_CONFIG +
                " property value: 1000"
        );
    }

    @Test
    public void testValidateLogLocalRetentionBytes() {
        assertInvalidConfig(
            Map.of(
                ServerLogConfigs.LOG_RETENTION_BYTES_CONFIG, "1000",
                RemoteLogManagerConfig.LOG_LOCAL_RETENTION_BYTES_PROP, "-1"
            ),
            "Invalid value -1 for configuration " + RemoteLogManagerConfig.LOG_LOCAL_RETENTION_BYTES_PROP +
                ": Value must not be -1 as " + ServerLogConfigs.LOG_RETENTION_BYTES_CONFIG +
                " value is set as 1000."
        );

        assertInvalidConfig(
            Map.of(
                ServerLogConfigs.LOG_RETENTION_BYTES_CONFIG, "1000",
                RemoteLogManagerConfig.LOG_LOCAL_RETENTION_BYTES_PROP, "1001"
            ),
            "Invalid value 1001 for configuration " + RemoteLogManagerConfig.LOG_LOCAL_RETENTION_BYTES_PROP +
                ": Value must not be more than " + ServerLogConfigs.LOG_RETENTION_BYTES_CONFIG +
                " property value: 1000"
        );
    }

    @Test
    public void testValidateCordonedLogDirsReportsAllInvalidEntries() {
        assertInvalidConfig(
            Map.of(
                ServerLogConfigs.LOG_DIRS_CONFIG, "/tmp/kafka-1,/tmp/kafka-2",
                ServerLogConfigs.CORDONED_LOG_DIRS_CONFIG, "/tmp/bad-1,/tmp/bad-2"
            ),
            "Invalid value [/tmp/bad-1, /tmp/bad-2] for configuration " + ServerLogConfigs.CORDONED_LOG_DIRS_CONFIG +
                ": Invalid entries in " + ServerLogConfigs.CORDONED_LOG_DIRS_CONFIG + ": [/tmp/bad-1, /tmp/bad-2]. " +
                "All cordoned log dirs must be entries of " + ServerLogConfigs.LOG_DIRS_CONFIG + " or " +
                ServerLogConfigs.LOG_DIR_CONFIG + "."
        );
    }

    private void assertInvalidConfig(Map<String, Object> overrides, String expectedMessage) {
        ConfigException exception = assertThrows(
            ConfigException.class,
            () -> dynamicLogConfig.validateReconfiguration(kafkaConfig(overrides))
        );
        assertEquals(expectedMessage, exception.getMessage());
    }

    private static AbstractKafkaConfig kafkaConfig(Map<String, Object> overrides) {
        Map<String, Object> props = new HashMap<>();
        props.put(KRaftConfigs.PROCESS_ROLES_CONFIG, "broker");
        props.put(KRaftConfigs.NODE_ID_CONFIG, "1");
        props.put(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "CONTROLLER");
        props.putAll(overrides);
        return new AbstractKafkaConfig(AbstractKafkaConfig.CONFIG_DEF, props, Map.of(), false) {
            @Override
            public void addReconfigurable(Reconfigurable reconfigurable) {
            }

            @Override
            public void removeReconfigurable(Reconfigurable reconfigurable) {
            }
        };
    }
}
