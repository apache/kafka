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

package org.apache.kafka.metadata.config;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

import static org.apache.kafka.metadata.config.ConfigRegistryTestConstants.ALL_KEYS;
import static org.apache.kafka.metadata.config.ConfigRegistryTestConstants.BROKER_NUM_FOOBARS;
import static org.apache.kafka.metadata.config.ConfigRegistryTestConstants.SCHEMA;
import static org.apache.kafka.metadata.config.ConfigRegistryTestConstants.STATIC_CONFIG_MAP;
import static org.apache.kafka.metadata.config.ConfigRegistryTestConstants.TOPIC_BAAZ_ENABLED;
import static org.apache.kafka.metadata.config.ConfigRegistryTestConstants.TOPIC_NUM_FOOBARS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Timeout(120)
public final class ConfigMonitorKeyTest {
    private static final Logger LOG = LoggerFactory.getLogger(ConfigMonitorKeyTest.class);

    @Test
    public void testEquals() {
        for (int i = 0; i < ALL_KEYS.size(); i++) {
            for (int j = 0; j < ALL_KEYS.size(); j++) {
                if (i == j) {
                    assertEquals(ALL_KEYS.get(i), ALL_KEYS.get(j));
                    assertEquals(ALL_KEYS.get(j), ALL_KEYS.get(i));
                } else {
                    assertNotEquals(ALL_KEYS.get(i), ALL_KEYS.get(j));
                    assertNotEquals(ALL_KEYS.get(j), ALL_KEYS.get(i));
                }
            }
        }
    }

    @Test
    public void testBrokerNumFoobarsKeyName() {
        assertEquals("num.foobars", BROKER_NUM_FOOBARS.keyName());
    }

    @Test
    public void testBrokerNumFoobarsResourceType() {
        assertEquals(ConfigResource.Type.BROKER, BROKER_NUM_FOOBARS.resourceType());
    }

    @Test
    public void testBrokerNumFoobarsToString() {
        assertEquals("ConfigMonitorKey(resourceType=BROKER, keyName=num.foobars)",
            BROKER_NUM_FOOBARS.toString());
    }

    @Test
    public void testTopicBaazEnabledToString() {
        assertEquals("ConfigMonitorKey(resourceType=TOPIC, keyName=baaz.enabled)",
            TOPIC_BAAZ_ENABLED.toString());
    }

    @Test
    public void testVerifyBrokerNumFoobarsValueType() {
        BROKER_NUM_FOOBARS.verifyExpectedValueType(SCHEMA, ConfigDef.Type.SHORT);
    }

    @Test
    public void testVerifyBrokerNumFoobarsValueTypeFails() {
        assertEquals("Unexpected value type for resource type BROKER, config keyName num.foobars; " +
                "needed BOOLEAN, got SHORT",
            assertThrows(RuntimeException.class,
                () -> BROKER_NUM_FOOBARS.verifyExpectedValueType(SCHEMA, ConfigDef.Type.BOOLEAN)).
                    getMessage());
    }

    @Test
    public void testVerifyTopicBaazEnabledValueType() {
        TOPIC_BAAZ_ENABLED.verifyExpectedValueType(SCHEMA, ConfigDef.Type.BOOLEAN);
    }

    @Test
    public void testLoadBrokerNumFoobarsStaticValue() {
        assertEquals((short) 3,
            BROKER_NUM_FOOBARS.loadStaticValue(LOG, STATIC_CONFIG_MAP, SCHEMA));
    }

    @Test
    public void testLoadTopicBaazEnabledStaticValue() {
        assertEquals(Boolean.TRUE,
            TOPIC_BAAZ_ENABLED.loadStaticValue(LOG, STATIC_CONFIG_MAP, SCHEMA));
    }

    @Test
    public void testLoadTopicBaazEnabledStaticValueEmptyConfigMap() {
        assertEquals(Boolean.FALSE,
            TOPIC_BAAZ_ENABLED.loadStaticValue(LOG, Collections.emptyMap(), SCHEMA));
    }

    @Test
    public void testLoadTopicNumFoobarsStaticValue() {
        assertEquals((short) 3,
            TOPIC_NUM_FOOBARS.loadStaticValue(LOG, STATIC_CONFIG_MAP, SCHEMA));
    }
}
