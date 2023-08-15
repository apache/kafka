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
package org.apache.kafka.connect.mirror;

import org.apache.kafka.common.Configurable;

import java.util.Map;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Defines remote topics like "us-west.topic1". The separator is customizable and defaults to a period. */
public class DefaultReplicationPolicy implements ReplicationPolicy, Configurable {
    
    private static final Logger log = LoggerFactory.getLogger(DefaultReplicationPolicy.class);

    // In order to work with various metrics stores, we allow custom separators.
    public static final String SEPARATOR_CONFIG = MirrorClientConfig.REPLICATION_POLICY_SEPARATOR;
    public static final String SEPARATOR_DEFAULT = ".";

    public static final String INTERNAL_TOPIC_SEPARATOR_ENABLED_CONFIG = MirrorClientConfig.INTERNAL_TOPIC_SEPARATOR_ENABLED;
    public static final Boolean INTERNAL_TOPIC_SEPARATOR_ENABLED_DEFAULT = true;

    private String separator = SEPARATOR_DEFAULT;
    private Pattern separatorPattern = Pattern.compile(Pattern.quote(SEPARATOR_DEFAULT));
    private boolean isInternalTopicSeparatorEnabled = true;

    @Override
    public void configure(Map<String, ?> props) {
        if (props.containsKey(SEPARATOR_CONFIG)) {
            separator = (String) props.get(SEPARATOR_CONFIG);
            log.info("Using custom remote topic separator: '{}'", separator);
            separatorPattern = Pattern.compile(Pattern.quote(separator));

            if (props.containsKey(INTERNAL_TOPIC_SEPARATOR_ENABLED_CONFIG)) {
                isInternalTopicSeparatorEnabled = Boolean.parseBoolean(props.get(INTERNAL_TOPIC_SEPARATOR_ENABLED_CONFIG).toString());
                if (!isInternalTopicSeparatorEnabled) {
                    log.warn("Disabling custom topic separator for internal topics; will use '.' instead of '{}'", separator);
                }
            }
        }
    }

    @Override
    public String formatRemoteTopic(String sourceClusterAlias, String topic) {
        return sourceClusterAlias + separator + topic;
    }

    @Override
    public String topicSource(String topic) {
        String[] parts = separatorPattern.split(topic);
        if (parts.length < 2) {
            // this is not a remote topic
            return null;
        } else {
            return parts[0];
        }
    }

    @Override
    public String upstreamTopic(String topic) {
        String source = topicSource(topic);
        if (source == null) {
            return null;
        } else {
            return topic.substring(source.length() + separator.length());
        }
    }

    private String internalSeparator() {
        return isInternalTopicSeparatorEnabled ? separator : ".";
    }
    private String internalSuffix() {
        return internalSeparator() + "internal";
    }

    private String checkpointsTopicSuffix() {
        return internalSeparator() + "checkpoints" + internalSuffix();
    }

    @Override
    public String offsetSyncsTopic(String clusterAlias) {
        return "mm2-offset-syncs" + internalSeparator() + clusterAlias + internalSuffix();
    }

    @Override
    public String checkpointsTopic(String clusterAlias) {
        return clusterAlias + checkpointsTopicSuffix();
    }

    @Override
    public boolean isCheckpointsTopic(String topic) {
        return  topic.endsWith(checkpointsTopicSuffix());
    }

    @Override
    public boolean isMM2InternalTopic(String topic) {
        return  topic.endsWith(internalSuffix());
    }
}
