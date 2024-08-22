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

package org.apache.kafka.metadata;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.server.config.ConfigSynonym;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Type.BOOLEAN;
import static org.apache.kafka.common.config.ConfigDef.Type.INT;
import static org.apache.kafka.common.config.ConfigResource.Type.BROKER;
import static org.apache.kafka.common.config.ConfigResource.Type.TOPIC;

/**
 * A fake KafkaConfigSchema object that can be used in tests.
 */
public class FakeKafkaConfigSchema {
    public static final Map<ConfigResource.Type, ConfigDef> CONFIGS = new HashMap<>();

    static {
        CONFIGS.put(BROKER, new ConfigDef().
            define("unclean.leader.election.enable", BOOLEAN, "false", HIGH, "").
            define("min.insync.replicas", INT, "1", HIGH, ""));
        CONFIGS.put(TOPIC, new ConfigDef().
            define("unclean.leader.election.enable", BOOLEAN, "false", HIGH, "").
            define("min.insync.replicas", INT, "1", HIGH, ""));
    }

    public static final Map<String, List<ConfigSynonym>> SYNONYMS = new HashMap<>();

    static {
        SYNONYMS.put("unclean.leader.election.enable",
            Arrays.asList(new ConfigSynonym("unclean.leader.election.enable")));
        SYNONYMS.put("min.insync.replicas",
            Arrays.asList(new ConfigSynonym("min.insync.replicas")));
    }

    public static final KafkaConfigSchema INSTANCE = new KafkaConfigSchema(CONFIGS, SYNONYMS);
}
