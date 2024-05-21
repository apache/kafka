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
package org.apache.kafka.streams.processor.internals.assignment;

import java.util.Optional;
import java.util.Set;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.processor.assignment.TaskTopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a simple container class used during the assignment process to distinguish
 * TopicPartitions type. Since the assignment logic can depend on the type of topic we're
 * looking at, and the rack information of the partition, this container class should have
 * everything necessary to make informed task assignment decisions.
 */
public class DefaultTaskTopicPartition implements TaskTopicPartition {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultTaskTopicPartition.class);

    private final TopicPartition topicPartition;
    private final boolean isSourceTopic;
    private final boolean isChangelogTopic;
    private final Optional<Set<String>> rackIds;

    public DefaultTaskTopicPartition(final TopicPartition topicPartition,
                                     final boolean isSourceTopic,
                                     final boolean isChangelogTopic,
                                     final Set<String> rackIds) {
        this.topicPartition = topicPartition;
        this.isSourceTopic = isSourceTopic;
        this.isChangelogTopic = isChangelogTopic;
        this.rackIds = Optional.ofNullable(rackIds);
    }

    @Override
    public TopicPartition topicPartition() {
        return topicPartition;
    }

    @Override
    public boolean isSource() {
        return isSourceTopic;
    }

    @Override
    public boolean isChangelog() {
        return isChangelogTopic;
    }

    @Override
    public Optional<Set<String>> rackIds() {
        return rackIds;
    }
}
