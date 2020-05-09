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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.runtime.isolation.Plugins;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.connect.runtime.TopicCreationConfig.DEFAULT_TOPIC_CREATION_GROUP;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.DEFAULT_TOPIC_CREATION_PREFIX;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.PARTITIONS_CONFIG;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.REPLICATION_FACTOR_CONFIG;

public class SourceConnectorConfig extends ConnectorConfig {

    protected static final String TOPIC_CREATION_GROUP = "Topic Creation";

    public static final String TOPIC_CREATION_PREFIX = "topic.creation.";

    public static final String TOPIC_CREATION_GROUPS_CONFIG = TOPIC_CREATION_PREFIX + "groups";
    private static final String TOPIC_CREATION_GROUPS_DOC = "Groups of configurations for topics "
            + "created by source connectors";
    private static final String TOPIC_CREATION_GROUPS_DISPLAY = "Topic Creation Groups";

    private static class EnrichedSourceConnectorConfig extends AbstractConfig {
        EnrichedSourceConnectorConfig(ConfigDef configDef, Map<String, String> props) {
            super(configDef, props);
        }

        @Override
        public Object get(String key) {
            return super.get(key);
        }
    }

    private static ConfigDef config = ConnectorConfig.configDef();
    private final EnrichedSourceConnectorConfig enrichedSourceConfig;

    public static ConfigDef configDef() {
        int orderInGroup = 0;
        return new ConfigDef(config)
                .define(TOPIC_CREATION_GROUPS_CONFIG, ConfigDef.Type.LIST, Collections.emptyList(),
                        ConfigDef.CompositeValidator.of(new ConfigDef.NonNullValidator(), ConfigDef.LambdaValidator.with(
                            (name, value) -> {
                                List<?> groupAliases = (List<?>) value;
                                if (groupAliases.size() > new HashSet<>(groupAliases).size()) {
                                    throw new ConfigException(name, value, "Duplicate alias provided.");
                                }
                            },
                            () -> "unique topic creation groups")),
                        ConfigDef.Importance.LOW, TOPIC_CREATION_GROUPS_DOC, TOPIC_CREATION_GROUP,
                        ++orderInGroup, ConfigDef.Width.LONG, TOPIC_CREATION_GROUPS_DISPLAY);
    }

    public static ConfigDef embedDefaultGroup(ConfigDef baseConfigDef) {
        String defaultGroup = "default";
        ConfigDef newDefaultDef = new ConfigDef(baseConfigDef);
        newDefaultDef.embed(DEFAULT_TOPIC_CREATION_PREFIX, defaultGroup, 0, TopicCreationConfig.defaultGroupConfigDef());
        return newDefaultDef;
    }

    /**
     * Returns an enriched {@link ConfigDef} building upon the {@code ConfigDef}, using the current configuration specified in {@code props} as an input.
     *
     * @param baseConfigDef
     * @param props
     * @return the enriched configuration definition
     */
    public static ConfigDef enrich(ConfigDef baseConfigDef, Map<String, String> props, AbstractConfig defaultGroupConfig) {
        List<Object> topicCreationGroups = new ArrayList<>();
        Object aliases = ConfigDef.parseType(TOPIC_CREATION_GROUPS_CONFIG, props.get(TOPIC_CREATION_GROUPS_CONFIG), ConfigDef.Type.LIST);
        if (aliases instanceof List) {
            topicCreationGroups.addAll((List<?>) aliases);
        }

        ConfigDef newDef = new ConfigDef(baseConfigDef);
        String defaultGroupPrefix = TOPIC_CREATION_PREFIX + DEFAULT_TOPIC_CREATION_GROUP + ".";
        topicCreationGroups.stream().distinct().forEach(group -> {
            if (!(group instanceof String)) {
                throw new ConfigException("Item in " + TOPIC_CREATION_GROUPS_CONFIG + " property is not of type String");
            }
            String alias = (String) group;
            String prefix = TOPIC_CREATION_PREFIX + alias + ".";
            String configGroup = TOPIC_CREATION_GROUP + ": " + alias;
            newDef.embed(prefix, configGroup, 0, TopicCreationConfig.configDef(
                    configGroup,
                    defaultGroupConfig.getInt(defaultGroupPrefix + REPLICATION_FACTOR_CONFIG),
                    defaultGroupConfig.getInt(defaultGroupPrefix + PARTITIONS_CONFIG)));
        });
        return newDef;
    }

    @Override
    public Object get(String key) {
        return enrichedSourceConfig.get(key);
    }

    public SourceConnectorConfig(Plugins plugins, Map<String, String> props, boolean createTopics) {
        super(plugins, config, props);
        if (createTopics) {
            ConfigDef defaultConfigDef = embedDefaultGroup(config);
            AbstractConfig defaultGroup = new AbstractConfig(defaultConfigDef, props, false);
            enrichedSourceConfig = new EnrichedSourceConnectorConfig(enrich(defaultConfigDef, props,
                    defaultGroup), props);
        } else {
            enrichedSourceConfig = null;
        }
    }

    public static void main(String[] args) {
        System.out.println(config.toHtml());
    }
}
