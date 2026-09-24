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

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ClientMetricsConfigsTest {

    @Test
    public void testParseMatchingPatterns() {
        Map<String, Pattern> patterns = ClientMetricsConfigs.parseMatchingPatterns(
            List.of("client_id=consumer-.*", "client_software_name=kafka"));
        assertEquals(2, patterns.size());
        assertEquals("consumer-.*", patterns.get("client_id").pattern());
        assertEquals("kafka", patterns.get("client_software_name").pattern());
    }

    @Test
    public void testParseMatchingPatternsEmpty() {
        assertTrue(ClientMetricsConfigs.parseMatchingPatterns(null).isEmpty());
        assertTrue(ClientMetricsConfigs.parseMatchingPatterns(List.of()).isEmpty());
    }

    @Test
    public void testParseMatchingPatternsRegexContainingEquals() {
        // KAFKA-21041: only the first '=' separates the parameter name from
        // the regular expression; further '=' characters are part of the regex.
        Map<String, Pattern> patterns = ClientMetricsConfigs.parseMatchingPatterns(
            List.of("client_id=foo=bar"));
        assertEquals(1, patterns.size());
        assertEquals("foo=bar", patterns.get("client_id").pattern());
    }

    @Test
    public void testParseMatchingPatternsInvalid() {
        // No '=' at all.
        assertThrows(InvalidConfigurationException.class,
            () -> ClientMetricsConfigs.parseMatchingPatterns(List.of("client_id")));
        // Unknown parameter name.
        assertThrows(InvalidConfigurationException.class,
            () -> ClientMetricsConfigs.parseMatchingPatterns(List.of("unknown_param=foo")));
        // Invalid regular expression.
        assertThrows(InvalidConfigurationException.class,
            () -> ClientMetricsConfigs.parseMatchingPatterns(List.of("client_id=[unclosed")));
    }
}
