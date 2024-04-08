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
package org.apache.kafka.server.config;

import static java.util.Arrays.asList;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.utils.Utils;

public final class ServerTopicConfigSynonyms {
    private static final String LOG_PREFIX = "log.";
    public static final String LOG_CLEANER_PREFIX = LOG_PREFIX + "cleaner.";

    /**
     * Maps topic configurations to their equivalent broker configurations.
     * <br>
     * Topics can be configured either by setting their dynamic topic configurations, or by
     * setting equivalent broker configurations. For historical reasons, the equivalent broker
     * configurations have different names. This table maps each topic configuration to its
     * equivalent broker configurations.
     * <br>
     * In some cases, the equivalent broker configurations must be transformed before they
     * can be used. For example, log.roll.hours must be converted to milliseconds before it
     * can be used as the value of segment.ms.
     * <br>
     * The broker configurations will be used in the order specified here. In other words, if
     * both the first and the second synonyms are configured, we will use only the value of
     * the first synonym and ignore the second.
     */
    // Topic configs with no mapping to a server config can be found in `LogConfig.CONFIGS_WITH_NO_SERVER_DEFAULTS`
    @SuppressWarnings("deprecation")
    public static final Map<String, List<ConfigSynonym>> ALL_TOPIC_CONFIG_SYNONYMS = Collections.unmodifiableMap(Utils.mkMap(
        sameNameWithLogPrefix(TopicConfig.SEGMENT_BYTES_CONFIG),
        listWithLogPrefix(TopicConfig.SEGMENT_MS_CONFIG,
            new ConfigSynonym("roll.ms"),
            new ConfigSynonym("roll.hours", ConfigSynonym.HOURS_TO_MILLISECONDS)),
        listWithLogPrefix(TopicConfig.SEGMENT_JITTER_MS_CONFIG,
            new ConfigSynonym("roll.jitter.ms"),
            new ConfigSynonym("roll.jitter.hours", ConfigSynonym.HOURS_TO_MILLISECONDS)),
        singleWithLogPrefix(TopicConfig.SEGMENT_INDEX_BYTES_CONFIG, "index.size.max.bytes"),
        singleWithLogPrefix(TopicConfig.FLUSH_MESSAGES_INTERVAL_CONFIG, "flush.interval.messages"),
        listWithLogPrefix(TopicConfig.FLUSH_MS_CONFIG,
            new ConfigSynonym("flush.interval.ms"),
            new ConfigSynonym("flush.scheduler.interval.ms")),
        sameNameWithLogPrefix(TopicConfig.RETENTION_BYTES_CONFIG),
        listWithLogPrefix(TopicConfig.RETENTION_MS_CONFIG,
            new ConfigSynonym("retention.ms"),
            new ConfigSynonym("retention.minutes", ConfigSynonym.MINUTES_TO_MILLISECONDS),
            new ConfigSynonym("retention.hours", ConfigSynonym.HOURS_TO_MILLISECONDS)),
        single(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, "message.max.bytes"),
        sameNameWithLogPrefix(TopicConfig.INDEX_INTERVAL_BYTES_CONFIG),
        sameNameWithLogCleanerPrefix(TopicConfig.DELETE_RETENTION_MS_CONFIG),
        sameNameWithLogCleanerPrefix(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG),
        sameNameWithLogCleanerPrefix(TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG),
        singleWithLogPrefix(TopicConfig.FILE_DELETE_DELAY_MS_CONFIG, "segment.delete.delay.ms"),
        singleWithLogCleanerPrefix(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "min.cleanable.ratio"),
        sameNameWithLogPrefix(TopicConfig.CLEANUP_POLICY_CONFIG),
        sameName(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG),
        sameName(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG),
        sameName(TopicConfig.COMPRESSION_TYPE_CONFIG),
        sameNameWithLogPrefix(TopicConfig.PREALLOCATE_CONFIG),
        sameNameWithLogPrefix(TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG),
        sameNameWithLogPrefix(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG),
        sameNameWithLogPrefix(TopicConfig.MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_CONFIG),
        sameNameWithLogPrefix(TopicConfig.MESSAGE_TIMESTAMP_BEFORE_MAX_MS_CONFIG),
        sameNameWithLogPrefix(TopicConfig.MESSAGE_TIMESTAMP_AFTER_MAX_MS_CONFIG),
        sameNameWithLogPrefix(TopicConfig.MESSAGE_DOWNCONVERSION_ENABLE_CONFIG),
        sameNameWithLogPrefix(TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG),
        sameNameWithLogPrefix(TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG)
    ));

    /**
     * Map topic config to the server config with the highest priority. Some of these have additional
     * synonyms that can be obtained using [[kafka.server.DynamicBrokerConfig#brokerConfigSynonyms]]
     * or using [[AllTopicConfigSynonyms]]
     */
    public static final Map<String, String> TOPIC_CONFIG_SYNONYMS = Collections.unmodifiableMap(
        ALL_TOPIC_CONFIG_SYNONYMS.entrySet()
        .stream()
        .collect(Collectors.toMap(Entry::getKey, e -> e.getValue().get(0).name())));

    /**
     * Return the server config with the highest priority for `topicConfigName` if it exists. Otherwise,
     * throw NoSuchElementException.
     */
    public static String serverSynonym(String topicConfigName) {
        String serverSynonym = TOPIC_CONFIG_SYNONYMS.get(topicConfigName);
        if (serverSynonym == null)
            throw new NoSuchElementException("No server synonym found for " + topicConfigName);
        return serverSynonym;
    }

    private static Entry<String, List<ConfigSynonym>> sameName(String configName) {
        return Utils.mkEntry(configName, asList(new ConfigSynonym(configName)));
    }

    private static Entry<String, List<ConfigSynonym>> sameNameWithLogPrefix(String configName) {
        return Utils.mkEntry(configName, asList(new ConfigSynonym(LOG_PREFIX + configName)));
    }

    private static Entry<String, List<ConfigSynonym>> sameNameWithLogCleanerPrefix(String configName) {
        return Utils.mkEntry(configName, asList(new ConfigSynonym(LOG_CLEANER_PREFIX + configName)));
    }

    private static Entry<String, List<ConfigSynonym>> singleWithLogPrefix(String topicConfigName, String brokerConfigName) {
        return Utils.mkEntry(topicConfigName, asList(new ConfigSynonym(LOG_PREFIX + brokerConfigName)));
    }

    private static Entry<String, List<ConfigSynonym>> singleWithLogCleanerPrefix(String topicConfigName, String brokerConfigName) {
        return Utils.mkEntry(topicConfigName, asList(new ConfigSynonym(LOG_CLEANER_PREFIX + brokerConfigName)));
    }

    private static Entry<String, List<ConfigSynonym>> listWithLogPrefix(String topicConfigName, ConfigSynonym... synonyms) {
        List<ConfigSynonym> synonymsWithPrefix = Arrays.stream(synonyms)
            .map(s -> new ConfigSynonym(LOG_PREFIX + s.name(), s.converter()))
            .collect(Collectors.toList());
        return Utils.mkEntry(topicConfigName, synonymsWithPrefix);
    }

    private static Entry<String, List<ConfigSynonym>> single(String topicConfigName, String brokerConfigName) {
        return Utils.mkEntry(topicConfigName, asList(new ConfigSynonym(brokerConfigName)));
    }
}
