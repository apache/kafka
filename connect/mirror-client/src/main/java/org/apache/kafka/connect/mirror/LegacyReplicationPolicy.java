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

import static org.apache.kafka.connect.mirror.MirrorClientConfig.HEARTBEATS_TOPIC;

/**
 * The replication policy that imitates the behavior of MirrorMaker 1.
 *
 * <p>The policy doesn't rename topics: {@code topic1} remains {@code topic1} after replication.
 * There is one exception to this: for {@code heartbeats}, it behaves identical to {@link DefaultReplicationPolicy}.
 *
 * <p>The policy has some notable limitations. The most important one is that the policy is unable to detect
 * cycles for any topic apart from {@code heartbeats}. This makes cross-replication effectively impossible.
 *
 * <p>Another limitation is that {@link MirrorClient#remoteTopics()} will be able to list only
 * {@code heartbeats} topics.
 *
 * <p>{@link MirrorClient#countHopsForTopic(String, String)} will return {@code -1} for any topic
 * apart from {@code heartbeats}.
 *
 * <p>The policy supports {@link DefaultReplicationPolicy}'s configurations
 * for the behavior related to {@code heartbeats}.
 */
public class LegacyReplicationPolicy implements ReplicationPolicy, Configurable {
    // Replication sub-policy for heartbeats topics
    private final DefaultReplicationPolicy heartbeatTopicReplicationPolicy = new DefaultReplicationPolicy();

    @Override
    public void configure(final Map<String, ?> props) {
        heartbeatTopicReplicationPolicy.configure(props);
    }

    @Override
    public String formatRemoteTopic(final String sourceClusterAlias, final String topic) {
        if (isOriginalTopicHeartbeats(topic)) {
            return heartbeatTopicReplicationPolicy.formatRemoteTopic(sourceClusterAlias, topic);
        } else {
            return topic;
        }
    }

    @Override
    public String topicSource(final String topic) {
        if (isOriginalTopicHeartbeats(topic)) {
            return heartbeatTopicReplicationPolicy.topicSource(topic);
        } else {
            return null;
        }
    }

    @Override
    public String upstreamTopic(final String topic) {
        if (isOriginalTopicHeartbeats(topic)) {
            return heartbeatTopicReplicationPolicy.upstreamTopic(topic);
        } else {
            return topic;
        }
    }

    @Override
    public String originalTopic(final String topic) {
        if (isOriginalTopicHeartbeats(topic)) {
            return HEARTBEATS_TOPIC;
        } else {
            return topic;
        }
    }

    @Override
    public boolean canTrackSource(final String topic) {
        return isOriginalTopicHeartbeats(topic);
    }

    private boolean isOriginalTopicHeartbeats(final String topic) {
        return HEARTBEATS_TOPIC.equals(heartbeatTopicReplicationPolicy.originalTopic(topic));
    }
}
