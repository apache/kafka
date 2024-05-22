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
package org.apache.kafka.connect.runtime;

import org.junit.jupiter.api.Test;

import javax.ws.rs.core.HttpHeaders;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.kafka.connect.runtime.ConfigHash.NO_HASH;
import static org.apache.kafka.connect.runtime.ConfigHash.fromConfig;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ConfigHashTest {

    private static final ConfigHash HASH_0 = new ConfigHash(0);
    private static final ConfigHash HASH_1 = new ConfigHash(9228);
    private static final ConfigHash HASH_2 = new ConfigHash(-468);
    private static final String CONNECTOR = "connector";

    @Test
    public void testHandleNullConfig() {
        ConfigHash configHash = fromConfig(null);
        assertNotNull(configHash);
        assertFalse(configHash.exists());
    }

    @Test
    public void testShouldUpdateTasks() {
        // We never expect null ConfigHash objects (should always use NO_HASH instead),
        // but should still handle them gracefully
        assertFalse(ConfigHash.shouldUpdateTasks(null, null));
        assertFalse(ConfigHash.shouldUpdateTasks(null, ConfigHash.NO_HASH));
        assertFalse(ConfigHash.shouldUpdateTasks(ConfigHash.NO_HASH, null));
        assertFalse(ConfigHash.shouldUpdateTasks(null, HASH_1));
        assertFalse(ConfigHash.shouldUpdateTasks(HASH_1, null));

        // We never update when there is no pre-existing hash
        assertFalse(ConfigHash.shouldUpdateTasks(null, NO_HASH));
        assertFalse(ConfigHash.shouldUpdateTasks(null, HASH_0));
        assertFalse(ConfigHash.shouldUpdateTasks(null, HASH_1));
        assertFalse(ConfigHash.shouldUpdateTasks(null, HASH_2));

        // We do update when hashes differ, including when there is no newer hash
        assertTrue(ConfigHash.shouldUpdateTasks(HASH_0, HASH_1));
        assertTrue(ConfigHash.shouldUpdateTasks(HASH_0, HASH_2));
        assertTrue(ConfigHash.shouldUpdateTasks(HASH_0, NO_HASH));
        assertTrue(ConfigHash.shouldUpdateTasks(HASH_1, HASH_0));
        assertTrue(ConfigHash.shouldUpdateTasks(HASH_1, HASH_2));
        assertTrue(ConfigHash.shouldUpdateTasks(HASH_1, NO_HASH));
        assertTrue(ConfigHash.shouldUpdateTasks(HASH_2, HASH_0));
        assertTrue(ConfigHash.shouldUpdateTasks(HASH_2, HASH_1));
        assertTrue(ConfigHash.shouldUpdateTasks(HASH_2, NO_HASH));
    }

    @Test
    public void testDifferentIterationOrder() {
        Map<String, String> config1 = new LinkedHashMap<>();
        config1.put("key1", "val1");
        config1.put("key2", "val2");

        Map<String, String> config2 = new LinkedHashMap<>();
        config2.put("key2", "val2");
        config2.put("key1", "val1");

        ConfigHash hash1 = fromConfig(config1);
        ConfigHash hash2 = fromConfig(config2);

        // Should still return the same value, regardless of iteration order
        assertTrue(hash1.matches(hash2));
        // And regardless of argument order
        assertTrue(hash2.matches(hash1));

        assertFalse(ConfigHash.shouldUpdateTasks(hash1, hash2));
        assertFalse(ConfigHash.shouldUpdateTasks(hash2, hash1));
    }

    @Test
    public void testForCollisions() {
        Map<String, String> config1 = new HashMap<>();
        Map<String, String> config2 = new HashMap<>();

        final int iterations = 10_000;
        int collisions = 0;
        for (int i = 0; i < iterations; i++) {
            config1.put("key" + i, "val" + i);
            config2.put("key_" + i, "val_" + i);

            if (fromConfig(config1).matches(fromConfig(config2)))
                collisions++;
        }
        assertTrue(collisions < iterations * 0.01, "Hash collision rate exceeds 1%");
    }

    @Test
    public void testHeaderParsing() {
        assertFalse(ConfigHash.fromHeaders(CONNECTOR, null).exists());
        assertFalse(ConfigHash.fromHeaders(CONNECTOR, headers(null)).exists());

        assertTrue(ConfigHash.fromHeaders(CONNECTOR, headers("0")).exists());
        assertTrue(ConfigHash.fromHeaders(CONNECTOR, headers(Integer.toString(Integer.MAX_VALUE))).exists());
        assertTrue(ConfigHash.fromHeaders(CONNECTOR, headers(Integer.toString(Integer.MIN_VALUE))).exists());
    }

    @Test
    public void testMapParsing() {

    }

    @Test
    public void testStructInsertion() {

    }

    private static HttpHeaders headers(String configHash) {
        HttpHeaders result = mock(HttpHeaders.class);
        when(result.getHeaderString(eq(ConfigHash.CONNECTOR_CONFIG_HASH_HEADER)))
                .thenReturn(configHash);
        return result;
    }

}
