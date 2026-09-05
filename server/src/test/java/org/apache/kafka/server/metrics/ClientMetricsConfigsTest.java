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
    public void testMatchingPatternMayContainEqualsSign() {
        Map<String, Pattern> patterns = ClientMetricsConfigs.parseMatchingPatterns(
            List.of("client_id=foo=bar")
        );

        Pattern clientIdPattern = patterns.get(ClientMetricsConfigs.CLIENT_ID);
        assertEquals("foo=bar", clientIdPattern.pattern());
        assertTrue(clientIdPattern.matcher("foo=bar").matches());
    }

    @Test
    public void testMatchingPatternWithoutEqualsSignIsRejected() {
        assertThrows(InvalidConfigurationException.class,
            () -> ClientMetricsConfigs.parseMatchingPatterns(List.of(ClientMetricsConfigs.CLIENT_ID)));
    }
}
