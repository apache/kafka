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

/** Defines which topics are "remote topics". e.g. "us-west.topic1". */
@InterfaceStability.Evolving
public interface ReplicationPolicy {

    /** How to rename remote topics; generally should be like us-west.topic1. */
    String formatRemoteTopic(String sourceClusterAlias, String topic);

    /** Source cluster alias of given remote topic, e.g. "us-west" for "us-west.topic1".
     *  Returns null if not a remote topic.
     */
    String topicSource(String topic);

    /** Name of topic on the source cluster, e.g. "topic1" for "us-west.topic1".
     *
     *  Topics may be replicated multiple hops, so the immediately upstream topic
     *  may itself be a remote topic.
     *
     *  Returns null if not a remote topic.
     */
    String upstreamTopic(String topic); 

    /** The name of the original source-topic, which may have been replicated multiple hops.
     *  Returns the topic if it is not a remote topic.
     */
    default String originalTopic(String topic) {
        String upstream = upstreamTopic(topic);
        if (upstream == null || upstream.equals(topic)) {
            return topic;
        } else {
            return originalTopic(upstream);
        }
    }

    /** Returns heartbeats topic name.*/
    default String heartbeatsTopic() {
        return "heartbeats";
    }

    /** Returns the offset-syncs topic for given cluster alias. */
    default String offsetSyncsTopic(String clusterAlias) {
        return "mm2-offset-syncs." + clusterAlias + ".internal";
    }

    /** Returns the name checkpoint topic for given cluster alias. */
    default String checkpointsTopic(String clusterAlias) {
        return clusterAlias + ".checkpoints.internal";
    }

    /** check if topic is a heartbeat topic, e.g heartbeats, us-west.heartbeats. */
    default boolean isHeartbeatsTopic(String topic) {
        return heartbeatsTopic().equals(originalTopic(topic));
    }

    /** check if topic is a checkpoint topic. */
    default boolean isCheckpointsTopic(String topic) {
        return  topic.endsWith(".checkpoints.internal");
    }

    /** Check topic is one of MM2 internal topic, this is used to make sure the topic doesn't need to be replicated.*/
    default boolean isMM2InternalTopic(String topic) {
        return  topic.endsWith(".internal");
    }

    /** Internal topics are never replicated. */
    default boolean isInternalTopic(String topic) {
        boolean isKafkaInternalTopic = topic.startsWith("__") || topic.startsWith(".");
        boolean isDefaultConnectTopic =  topic.endsWith("-internal") ||  topic.endsWith(".internal");
        return isMM2InternalTopic(topic) || isKafkaInternalTopic || isDefaultConnectTopic;
    }
}
