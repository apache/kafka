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
import org.apache.kafka.metadata.KafkaConfigSchema;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.config.ConfigResource.Type.BROKER;
import static org.apache.kafka.common.config.ConfigResource.Type.TOPIC;

/**
 * Constants for use in ConfigRegistry tests.
 */
public final class ConfigRegistryTestConstants {
    final static ConfigMonitorKey BROKER_NUM_FOOBARS =
        new ConfigMonitorKey(ConfigResource.Type.BROKER, "num.foobars");

    final static ConfigMonitorKey BROKER_NUM_QUUX =
        new ConfigMonitorKey(ConfigResource.Type.BROKER, "num.quux");

    final static ConfigMonitorKey BROKER_BAAZ_ENABLED =
        new ConfigMonitorKey(ConfigResource.Type.BROKER, "baaz.enabled");

    final static ConfigMonitorKey TOPIC_NUM_FOOBARS =
        new ConfigMonitorKey(TOPIC, "num.foobars");

    final static ConfigMonitorKey TOPIC_BAAZ_ENABLED =
        new ConfigMonitorKey(TOPIC, "baaz.enabled");

    final static List<ConfigMonitorKey> ALL_KEYS = Arrays.asList(
        BROKER_NUM_FOOBARS,
        BROKER_NUM_QUUX,
        BROKER_BAAZ_ENABLED,
        TOPIC_NUM_FOOBARS,
        TOPIC_BAAZ_ENABLED);

    static final Map<ConfigResource.Type, ConfigDef> CONFIGS = new HashMap<>();

    static {
        CONFIGS.put(BROKER, new ConfigDef().
            define("num.foobars", ConfigDef.Type.SHORT, "2", ConfigDef.Importance.HIGH, "the number of foobars").
            define("num.quux", ConfigDef.Type.INT, "5", ConfigDef.Importance.HIGH, "the number of quuxes").
            define("baaz.enabled", ConfigDef.Type.BOOLEAN, "true", ConfigDef.Importance.HIGH, "if the baaz is enabled"));
        CONFIGS.put(TOPIC, new ConfigDef().
            define("num.foobars", ConfigDef.Type.SHORT, "2", ConfigDef.Importance.HIGH, "the number of foobars").
            define("baaz.enabled", ConfigDef.Type.BOOLEAN, "false", ConfigDef.Importance.HIGH, "if the baaz is enabled"));
    }

    static final KafkaConfigSchema SCHEMA = new KafkaConfigSchema(CONFIGS, Collections.emptyMap());

    static final Map<String, Object> STATIC_CONFIG_MAP;

    static {
        Map<String, Object> staticConfigMap = new HashMap<>();
        staticConfigMap.put("num.foobars", (short) 3);
        staticConfigMap.put("baaz.enabled", "true");
        STATIC_CONFIG_MAP = Collections.unmodifiableMap(staticConfigMap);
    }
}
