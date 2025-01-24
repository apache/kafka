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

import org.apache.kafka.common.annotation.InterfaceStability;

/**
 * An interface used by the MirrorMaker connectors to manage topics names between source and target clusters.
 */
@InterfaceStability.Evolving
public interface ReplicationPolicy {

    /**
     * Returns the remote topic name for the given topic and source cluster alias.
     */
    String formatRemoteTopic(String sourceClusterAlias, String topic);

    /**
     * Returns the source cluster alias of given topic.
     * Returns null if the given topic is not a remote topic.
     */
    String topicSource(String topic);

    /**
     * Return the name of the given topic on the source cluster.
     * <p>
     * Topics may be replicated multiple hops, so the immediately upstream topic may itself be a remote topic.
     * <p>
     * Returns null if the given topic is not a remote topic.
     */
    String upstreamTopic(String topic); 

    /**
     * Returns the name of the original topic, which may have been replicated multiple hops.
     * Returns the topic if it is not a remote topic.
     */
    default String originalTopic(String topic) {
        String upstream = upstreamTopic(topic);
        if (upstream == null || upstream.equals(topic)) {
            return topic;
        } else {
            return originalTopic(upstream);
        }
    }

    /**
     * Returns the name of heartbeats topic.
     */
    default String heartbeatsTopic() {
        return "heartbeats";
    }

    /**
     * Returns the name of the offset-syncs topic for given cluster alias.
     */
    default String offsetSyncsTopic(String clusterAlias) {
        return "mm2-offset-syncs." + clusterAlias + ".internal";
    }

    /**
     * Returns the name of the checkpoints topic for given cluster alias.
     */
    default String checkpointsTopic(String clusterAlias) {
        return clusterAlias + ".checkpoints.internal";
    }

    /**
     * Returns true if the topic is a heartbeats topic
     */
    default boolean isHeartbeatsTopic(String topic) {
        return heartbeatsTopic().equals(originalTopic(topic));
    }

    /**
     * Returns true if the topic is a checkpoints topic.
     */
    default boolean isCheckpointsTopic(String topic) {
        return  topic.endsWith(".checkpoints.internal");
    }

    /**
     * Returns true if the topic is one of MirrorMaker internal topics.
     * This is used to make sure the topic doesn't need to be replicated.
     */
    default boolean isMM2InternalTopic(String topic) {
        return  topic.startsWith("mm2") && topic.endsWith(".internal") || isCheckpointsTopic(topic);
    }

    /**
     * Returns true if the topic is considered an internal topic.
     */
    default boolean isInternalTopic(String topic) {
        boolean isKafkaInternalTopic = topic.startsWith("__") || topic.startsWith(".");
        return isMM2InternalTopic(topic) || isKafkaInternalTopic;
    }
}
