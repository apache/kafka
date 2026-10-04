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
package org.apache.kafka.server.metrics;

import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.utils.LogCaptureAppender;

import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ClientMetricsConfigsTest {

    private static final String SUBSCRIPTION_NAME = "test-subscription";

    @Test
    public void testMatchingPatternMayContainEqualsSign() {
        Map<String, ClientMatchPattern> patterns = ClientMetricsConfigs.parseMatchingPatterns(
            SUBSCRIPTION_NAME, List.of("client_id=foo=bar")
        );

        ClientMatchPattern clientIdPattern = patterns.get(ClientMetricsConfigs.CLIENT_ID);
        assertEquals("foo=bar", clientIdPattern.pattern());
        assertTrue(clientIdPattern.matches("foo=bar"));
    }

    @Test
    public void testMatchingPatternWithoutEqualsSignIsRejected() {
        assertThrows(InvalidConfigurationException.class,
            () -> ClientMetricsConfigs.parseMatchingPatterns(SUBSCRIPTION_NAME, List.of(ClientMetricsConfigs.CLIENT_ID)));
    }

    @Test
    public void testMatchingPatternFallsBackToLegacy() {
        // A backreference is valid java.util.regex syntax but unsupported by RE2/J.
        // parseMatchingPatterns must fall back to the legacy engine for such a pattern and
        // match correctly, not reject it, while logging a warning.
        try (LogCaptureAppender appender = LogCaptureAppender.createAndRegister(ClientMetricsConfigs.class)) {
            Map<String, ClientMatchPattern> patterns = ClientMetricsConfigs.parseMatchingPatterns(
                    SUBSCRIPTION_NAME, List.of("client_software_version=(\\d+)\\.\\1")
            );

            ClientMatchPattern versionPattern = patterns.get(ClientMetricsConfigs.CLIENT_SOFTWARE_VERSION);
            assertTrue(versionPattern.matches("12.12"));
            assertFalse(versionPattern.matches("12.13"));

            List<String> warnings = appender.getMessages(Level.WARN);
            assertTrue(warnings.stream().anyMatch(message ->
                    message.contains("relies on deprecated") && message.contains("will stop being supported")));
        }
    }

    @Test
    public void testValidateRejectsNewLegacyPattern() {
        Properties props = new Properties();
        props.put(ClientMetricsConfigs.MATCH_CONFIG, "client_software_version=(\\d+)\\.\\1");

        InvalidConfigurationException exception = assertThrows(InvalidConfigurationException.class,
            () -> ClientMetricsConfigs.validate(SUBSCRIPTION_NAME, props, emptyMap()));
        assertTrue(exception.getMessage().contains("is not a valid regular expression"));
    }

    @Test
    public void testValidateAcceptsRe2CompatiblePattern() {
        Properties props = new Properties();
        props.put(ClientMetricsConfigs.MATCH_CONFIG, "client_software_version=3\\.5\\..*");

        ClientMetricsConfigs.validate(SUBSCRIPTION_NAME, props, emptyMap());
    }

    @Test
    public void testValidateAcceptsLegacyPatternWhenUnchanged() {
        // Strict RE2/J validation is skipped for already-existing patterns.
        // Passing the same config as both old and new treats match as unchanged.
        Properties props = new Properties();
        props.put(ClientMetricsConfigs.MATCH_CONFIG, "client_software_version=(\\d+)\\.\\1");

        ClientMetricsConfigs.validate(SUBSCRIPTION_NAME, props, props);
    }
}
