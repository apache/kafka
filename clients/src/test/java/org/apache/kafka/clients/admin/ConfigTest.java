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

package org.apache.kafka.clients.admin;

import org.apache.kafka.clients.admin.ConfigEntry.ConfigType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConfigTest {
    private static final ConfigEntry E1 = new ConfigEntry("a", "b");
    private static final ConfigEntry E2 = new ConfigEntry("c", "d");
    private Config config;

    @BeforeEach
    public void setUp() {
        config = new Config(asList(E1, E2));
    }

    @Test
    public void shouldGetEntry() {
        assertEquals(E1, config.get("a"));
        assertEquals(E2, config.get("c"));
    }

    @Test
    public void shouldReturnNullOnGetUnknownEntry() {
        assertNull(config.get("unknown"));
    }

    @Test
    public void shouldGetAllEntries() {
        assertEquals(2, config.entries().size());
        assertTrue(config.entries().contains(E1));
        assertTrue(config.entries().contains(E2));
    }

    @Test
    public void shouldImplementEqualsProperly() {
        assertEquals(config, config);
        assertEquals(config, new Config(config.entries()));
        assertNotEquals(new Config(asList(E1)), config);
        assertNotEquals(config, "this");
    }

    @Test
    public void shouldImplementHashCodeProperly() {
        assertEquals(config.hashCode(), config.hashCode());
        assertEquals(config.hashCode(), new Config(config.entries()).hashCode());
        assertNotEquals(new Config(asList(E1)).hashCode(), config.hashCode());
    }

    @Test
    public void shouldImplementToStringProperly() {
        assertTrue(config.toString().contains(E1.toString()));
        assertTrue(config.toString().contains(E2.toString()));
    }

    public static ConfigEntry newConfigEntry(String name, String value, ConfigEntry.ConfigSource source, boolean isSensitive,
                                             boolean isReadOnly, List<ConfigEntry.ConfigSynonym> synonyms) {
        return new ConfigEntry(name, value, source, isSensitive, isReadOnly, synonyms, ConfigType.UNKNOWN, null);
    }
}
