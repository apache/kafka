/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.coordinator.group.consumer;

import static org.apache.kafka.coordinator.group.consumer.ConsumerGroupConfig.CONSUMER_HEARTBEAT_INTERVAL_CONFIG;
import static org.apache.kafka.coordinator.group.consumer.ConsumerGroupConfig.CONSUMER_SESSION_TIMEOUT_CONFIG;
import static org.apache.kafka.coordinator.group.consumer.ConsumerGroupConfig.DEFAULT_CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS;
import static org.apache.kafka.coordinator.group.consumer.ConsumerGroupConfig.DEFAULT_CONSUMER_GROUP_SESSION_TIMEOUT_MS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.kafka.common.errors.InvalidRequestException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

public class ConsumerGroupConfigManagerTest {

    private ConsumerGroupConfigManager configManager;

    @BeforeEach
    public void setUp() {
        configManager = createConfigManager();
    }

    @AfterEach
    public void tearDown() {
        if (configManager != null) {
            configManager.close();
        }
    }

    @Test
    public void testUpdateConfigWithInvalidGroupId() {
        assertThrows(InvalidRequestException.class,
            () -> configManager.updateConsumerGroupConfig("", new Properties()));
    }

    @Test
    public void testGetNonExistentGroupConfig() {
        Optional<ConsumerGroupConfig> consumerGroupConfig = configManager.getConsumerGroupConfig("foo");
        assertFalse(consumerGroupConfig.isPresent());
    }

    @Test
    public void testUpdateConsumerGroupConfig() {
        String groupId = "foo";
        Properties props = new Properties();
        props.put(CONSUMER_SESSION_TIMEOUT_CONFIG, "20");
        configManager.updateConsumerGroupConfig(groupId, props);

        Optional<ConsumerGroupConfig> configOptional = configManager.getConsumerGroupConfig(groupId);
        assertTrue(configOptional.isPresent());

        ConsumerGroupConfig config = configOptional.get();
        assertEquals(20, config.getInt(CONSUMER_SESSION_TIMEOUT_CONFIG));
        assertEquals(DEFAULT_CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS, config.getInt(CONSUMER_HEARTBEAT_INTERVAL_CONFIG));
    }

    private ConsumerGroupConfigManager createConfigManager() {
        Map<String, String> defaultConfig = new HashMap<>();
        defaultConfig.put(CONSUMER_SESSION_TIMEOUT_CONFIG, String.valueOf(DEFAULT_CONSUMER_GROUP_SESSION_TIMEOUT_MS));
        defaultConfig.put(CONSUMER_HEARTBEAT_INTERVAL_CONFIG,
            String.valueOf(DEFAULT_CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS));
        return new ConsumerGroupConfigManager(defaultConfig);
    }
}
