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

import junit.framework.AssertionFailedError;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.EMPTY_RACK_AWARE_ASSIGNMENT_TAGS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;

public class AssignorConfigurationTest {
    private final Map<String, Object> config = new HashMap<>();

    @Before
    public void setup() {
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "app.id");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy");
        config.put(StreamsConfig.InternalConfig.REFERENCE_CONTAINER_PARTITION_ASSIGNOR, mock(ReferenceContainer.class));
    }

    @Test
    public void configsShouldRejectZeroWarmups() {
        final ConfigException exception = assertThrows(
            ConfigException.class,
            () -> new AssignorConfiguration.AssignmentConfigs(1L, 0, 1, 1L, EMPTY_RACK_AWARE_ASSIGNMENT_TAGS)
        );

        assertThat(exception.getMessage(), containsString("Invalid value 0 for configuration max.warmup.replicas"));
    }

    @Test
    public void rebalanceProtocolShouldSupportAllUpgradeFromVersions() {
        for (final String upgradeFrom : TestConfig.upgradeFromVersions()) {
            config.put(StreamsConfig.UPGRADE_FROM_CONFIG, upgradeFrom);
            final AssignorConfiguration assignorConfiguration = new AssignorConfiguration(config);

            try {
                assignorConfiguration.rebalanceProtocol();
            } catch (final Exception throwable) {
                throw new AssertionFailedError("Upgrade from " + upgradeFrom + " failed with " + throwable.getMessage() + "!");
            }
        }
    }

    @Test
    public void configuredMetadataVersionShouldSupportAllUpgradeFromVersions() {
        for (final String upgradeFrom : TestConfig.upgradeFromVersions()) {
            config.put(StreamsConfig.UPGRADE_FROM_CONFIG, upgradeFrom);
            final AssignorConfiguration assignorConfiguration = new AssignorConfiguration(config);

            try {
                assignorConfiguration.configuredMetadataVersion(0);
            } catch (final Exception throwable) {
                throw new AssertionFailedError("Upgrade from " + upgradeFrom + " failed with " + throwable.getMessage() + "!");
            }
        }
    }

    private static class TestValidString extends ConfigDef.ValidString {
        protected TestValidString(final ConfigDef.ValidString validString) {
            super(validString);
        }

        List<String> validStrings() {
            return validStrings;
        }
    }

    private static class TestConfig extends StreamsConfig {
        public TestConfig() {
            super(mkMap(
                mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, "app.id"),
                mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy")
            ));
        }

        private static List<String> upgradeFromVersions() {
            return new TestValidString(
                    (ConfigDef.ValidString) CONFIG.configKeys().get(StreamsConfig.UPGRADE_FROM_CONFIG).validator
            ).validStrings();
        }

    }
}
