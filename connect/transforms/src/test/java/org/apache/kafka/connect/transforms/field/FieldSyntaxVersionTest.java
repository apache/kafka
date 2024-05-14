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
package org.apache.kafka.connect.transforms.field;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class FieldSyntaxVersionTest {
    @Test
    void shouldAppendConfigToDef() {
        ConfigDef def = FieldSyntaxVersion.appendConfigTo(new ConfigDef());
        assertEquals(def.configKeys().size(), 1);
        final ConfigDef.ConfigKey configKey = def.configKeys().get("field.syntax.version");
        assertEquals(configKey.name, "field.syntax.version");
        assertEquals(configKey.defaultValue, "V1");
    }

    @Test
    void shouldFailWhenAppendConfigToDefAgain() {
        ConfigDef def = FieldSyntaxVersion.appendConfigTo(new ConfigDef());
        assertEquals(def.configKeys().size(), 1);
        ConfigException e = assertThrows(ConfigException.class, () -> FieldSyntaxVersion.appendConfigTo(def));
        assertEquals(e.getMessage(), "Configuration field.syntax.version is defined twice.");
    }

    @ParameterizedTest
    @CsvSource({"v1,V1", "v2,V2", "V1,V1", "V2,V2"})
    void shouldGetVersionFromConfig(String input, FieldSyntaxVersion version) {
        Map<String, String> configs = new HashMap<>();
        configs.put("field.syntax.version", input);
        AbstractConfig config = new AbstractConfig(FieldSyntaxVersion.appendConfigTo(new ConfigDef()), configs);
        assertEquals(version, FieldSyntaxVersion.fromConfig(config));
    }

    @ParameterizedTest
    @ValueSource(strings = {"v3", "V 1", "v", "V 2", "2", "1"})
    void shouldFailWhenWrongVersionIsPassed(String input) {
        Map<String, String> configs = new HashMap<>();
        configs.put("field.syntax.version", input);
        ConfigException e = assertThrows(ConfigException.class, () -> new AbstractConfig(FieldSyntaxVersion.appendConfigTo(new ConfigDef()), configs));
        assertEquals(
            "Invalid value " + input + " for configuration field.syntax.version: " +
                "String must be one of (case insensitive): V1, V2",
            e.getMessage());
    }
}
