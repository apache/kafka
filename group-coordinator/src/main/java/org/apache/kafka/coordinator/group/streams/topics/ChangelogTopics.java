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
package org.apache.kafka.coordinator.group.streams.topics;

import java.util.List;
import org.apache.kafka.common.utils.LogContext;

import org.slf4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class ChangelogTopics {

    private final Map<String, TopicsInfo> topicGroups;
    private final Function<String, Integer> topicPartitionCountProvider;
    private final Logger log;

    public ChangelogTopics(
        final LogContext logContext,
        final Map<String, TopicsInfo> subtopologyToTopicsInfo,
        final Function<String, Integer> topicPartitionCountProvider) {
        this.log = logContext.logger(getClass());
        this.topicGroups = subtopologyToTopicsInfo;
        this.topicPartitionCountProvider = topicPartitionCountProvider;
    }

    public Map<String, InternalTopicConfig> setup() {
        final Map<String, InternalTopicConfig> changelogTopicMetadata = new HashMap<>();
        for (final Map.Entry<String, TopicsInfo> entry : topicGroups.entrySet()) {
            final TopicsInfo topicsInfo = entry.getValue();
            final int maxNumPartitions = maxNumPartitions(topicsInfo.sourceTopics());

            for (final InternalTopicConfig topicConfig : topicsInfo.nonSourceChangelogTopics()) {
                topicConfig.setNumberOfPartitions(maxNumPartitions);
                changelogTopicMetadata.put(topicConfig.name(), topicConfig);
            }
        }

        log.debug("Creating state changelog topics {} from the requested topology.", changelogTopicMetadata.values());

        return changelogTopicMetadata;
    }


    protected int maxNumPartitions(final List<String> topics) {
        int maxNumPartitions = 0;
        for (final String topic : topics) {
            final Integer topicPartitionCount = topicPartitionCountProvider.apply(topic);
            if (topicPartitionCount == null) {
                throw new IllegalStateException("No partition count for topic " + topic);
            }

            final int numPartitions = topicPartitionCount;
            if (numPartitions > maxNumPartitions) {
                maxNumPartitions = numPartitions;
            }
        }
        return maxNumPartitions;
    }
}