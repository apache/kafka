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
package org.apache.kafka.streams.processor.internals.assignment;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.internals.UpgradeFromValues;
import org.apache.kafka.streams.processor.assignment.AssignmentConfigs;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.EMPTY_RACK_AWARE_ASSIGNMENT_TAGS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

public class AssignorConfigurationTest {
    private final Map<String, Object> config = new HashMap<>();

    @BeforeEach
    public void setup() {
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "app.id");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy");
        config.put(StreamsConfig.InternalConfig.REFERENCE_CONTAINER_PARTITION_ASSIGNOR, mock(ReferenceContainer.class));
    }

    @Test
    public void configsShouldRejectZeroWarmups() {
        final ConfigException exception = assertThrows(
            ConfigException.class,
            () -> new AssignmentConfigs(1L, 0, 1, 1L, EMPTY_RACK_AWARE_ASSIGNMENT_TAGS)
        );

        assertThat(exception.getMessage(), containsString("Invalid value 0 for configuration max.warmup.replicas"));
    }

    @Test
    public void shouldSupportAllUpgradeFromVersionsFromCooperativeRebalancingOn() {
        boolean beforeCooperative = true;
        for (final UpgradeFromValues upgradeFrom : UpgradeFromValues.values()) {
            if (upgradeFrom.toString().equals("2.4")) {
                beforeCooperative = false;
            }

            config.put(StreamsConfig.UPGRADE_FROM_CONFIG, upgradeFrom.toString());

            if (beforeCooperative) {
                assertThrows(ConfigException.class, () -> new AssignorConfiguration(config));
            } else {
                assertDoesNotThrow(() -> new AssignorConfiguration(config));
            }
        }
    }

}
