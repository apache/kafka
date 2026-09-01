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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.provider.FileConfigProvider;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class DynamicBrokerConfigTest {

    @Test
    public void testValidateConfigTypesResolvesConfigProviders() throws IOException {
        Path providerFile = Files.createTempFile("provider", ".properties");
        try {
            Files.writeString(providerFile, "token=42");
            Properties props = new Properties();
            props.setProperty(AbstractConfig.CONFIG_PROVIDERS_CONFIG, "file");
            props.setProperty(AbstractConfig.CONFIG_PROVIDERS_CONFIG + ".file.class", FileConfigProvider.class.getName());
            props.setProperty(ServerConfigs.NUM_IO_THREADS_CONFIG, "${file:" + providerFile.toAbsolutePath() + ":token}");

            assertDoesNotThrow(() -> DynamicBrokerConfig.validateConfigTypes(props));
        } finally {
            Files.deleteIfExists(providerFile);
        }
    }

    @Test
    public void testBrokerConfigSynonyms() {
        List<String> logRollTimeConfigs = List.of(ServerLogConfigs.LOG_ROLL_TIME_MILLIS_CONFIG, ServerLogConfigs.LOG_ROLL_TIME_HOURS_CONFIG);
        for (String config : logRollTimeConfigs) {
            assertEquals(logRollTimeConfigs, DynamicBrokerConfig.brokerConfigSynonyms(config, false));
        }
        List<String> logRollJitterConfigs = List.of(ServerLogConfigs.LOG_ROLL_TIME_JITTER_MILLIS_CONFIG, ServerLogConfigs.LOG_ROLL_TIME_JITTER_HOURS_CONFIG);
        for (String config : logRollJitterConfigs) {
            assertEquals(logRollJitterConfigs, DynamicBrokerConfig.brokerConfigSynonyms(config, false));
        }
        List<String> logFlushConfigs = List.of(ServerLogConfigs.LOG_FLUSH_INTERVAL_MS_CONFIG, ServerLogConfigs.LOG_FLUSH_SCHEDULER_INTERVAL_MS_CONFIG);
        assertEquals(logFlushConfigs, DynamicBrokerConfig.brokerConfigSynonyms(ServerLogConfigs.LOG_FLUSH_INTERVAL_MS_CONFIG, false));
        List<String> logRetentionConfigs = List.of(ServerLogConfigs.LOG_RETENTION_TIME_MILLIS_CONFIG, ServerLogConfigs.LOG_RETENTION_TIME_MINUTES_CONFIG, ServerLogConfigs.LOG_RETENTION_TIME_HOURS_CONFIG);
        for (String config : logRetentionConfigs) {
            assertEquals(logRetentionConfigs, DynamicBrokerConfig.brokerConfigSynonyms(config, false));
        }

        assertEquals(List.of("listener.name.secure.ssl.keystore.type", "ssl.keystore.type"),
                DynamicBrokerConfig.brokerConfigSynonyms("listener.name.secure.ssl.keystore.type", true));
        assertEquals(List.of("listener.name.sasl_ssl.plain.sasl.jaas.config", "sasl.jaas.config"),
                DynamicBrokerConfig.brokerConfigSynonyms("listener.name.sasl_ssl.plain.sasl.jaas.config", true));
        assertEquals(List.of("some.config"),
                DynamicBrokerConfig.brokerConfigSynonyms("some.config", true));

        assertEquals(List.of("listener.name.NAME.CONFIG", "CONFIG"), DynamicBrokerConfig.brokerConfigSynonyms("listener.name.NAME.CONFIG", true));
        assertEquals(List.of("listener.name.NAME.CONFIG"), DynamicBrokerConfig.brokerConfigSynonyms("listener.name.NAME.CONFIG", false));
        assertEquals(List.of("listener.name.CONFIG"), DynamicBrokerConfig.brokerConfigSynonyms("listener.name.CONFIG", true));
        assertEquals(List.of("listener.name.CONFIG"), DynamicBrokerConfig.brokerConfigSynonyms("listener.name.CONFIG", false));

        assertEquals(List.of("anything"), DynamicBrokerConfig.brokerConfigSynonyms("anything", false));
    }
}
