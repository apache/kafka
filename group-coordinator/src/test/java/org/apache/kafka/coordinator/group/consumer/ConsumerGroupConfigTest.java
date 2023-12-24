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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ConsumerGroupConfigTest {

    @Test
    public void testFromPropsInvalid() {
        ConsumerGroupConfig.configNames().forEach(name -> {
            if (ConsumerGroupConfig.CONSUMER_SESSION_TIMEOUT_CONFIG.equals(name)) {
                assertPropertyInvalid(name, "not_a_number", "-0.1", "1.2");
            } else if (ConsumerGroupConfig.CONSUMER_HEARTBEAT_INTERVAL_CONFIG.equals(name)) {
                assertPropertyInvalid(name, "not_a_number", "-0.1", "1.2");
            } else {
                assertPropertyInvalid(name, "not_a_number", "-1");
            }
        });
    }

    private void assertPropertyInvalid(String name, Object... values) {
        for (Object value : values) {
            Properties props = new Properties();
            props.setProperty(name, value.toString());
            assertThrows(Exception.class, () -> new ConsumerGroupConfig(props));
        }
    }

    @Test
    public void testInvalidProps() {
        // Check for invalid sessionTimeoutMs, < 0
        doTestInvalidProps(-1, 10);

        // Check for invalid heartbeatIntervalMs, < 0
        doTestInvalidProps(10, -1);
    }

    private void doTestInvalidProps(int sessionTimeoutMs, int heartbeatIntervalMs) {
        Properties props = new Properties();
        props.put(ConsumerGroupConfig.CONSUMER_SESSION_TIMEOUT_CONFIG, String.valueOf(sessionTimeoutMs));
        props.put(ConsumerGroupConfig.CONSUMER_HEARTBEAT_INTERVAL_CONFIG, String.valueOf(heartbeatIntervalMs));
        assertThrows(ConfigException.class, () -> ConsumerGroupConfig.validate(props));
    }

    @Test
    public void testFromPropsWithDefaultValue() {
        Map<String, String> defaultValue = new HashMap<>();
        defaultValue.put(ConsumerGroupConfig.CONSUMER_SESSION_TIMEOUT_CONFIG, "10");
        defaultValue.put(ConsumerGroupConfig.CONSUMER_HEARTBEAT_INTERVAL_CONFIG, "10");

        Properties props = new Properties();
        props.put(ConsumerGroupConfig.CONSUMER_SESSION_TIMEOUT_CONFIG, "20");
        ConsumerGroupConfig config = ConsumerGroupConfig.fromProps(defaultValue, props);

        assertEquals(10, config.getInt(ConsumerGroupConfig.CONSUMER_HEARTBEAT_INTERVAL_CONFIG));
        assertEquals(20, config.getInt(ConsumerGroupConfig.CONSUMER_SESSION_TIMEOUT_CONFIG));
    }

    @Test
    public void testInvalidConfigName() {
        Properties props = new Properties();
        props.put(ConsumerGroupConfig.CONSUMER_SESSION_TIMEOUT_CONFIG, "10");
        props.put("invalid.config.name", "10");
        assertThrows(InvalidConfigurationException.class, () -> ConsumerGroupConfig.validate(props));
    }
}
