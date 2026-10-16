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

package org.apache.kafka.server.share.dlq;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.Uuid;

import java.util.List;
import java.util.Optional;

/**
 * Interface encapsulating metadata cache related methods which are
 * required by share group DLQ operations. This will be helpful in testing
 * and keeping implementations manageable.
 */
public interface ShareGroupDLQMetadataCacheHelper {

    public record TopicPartitionData(
        String topicName,
        Optional<Integer> numPartitions,
        Optional<Uuid> topicId,
        List<Node> partitionLeaderNodes
    ) {
    }

    /**
     * Return optional of string representing of DLQ topic.
     *
     * @param groupId Id of the share group
     * @return Optional of string representing of DLQ topic if set, empty otherwise
     */
    Optional<String> shareGroupDlqTopic(String groupId);

    /**
     * Check if DLQ dynamic config is set on the topic to mark it available for DLQ writes.
     *
     * @param topic The name of the topic
     * @return Boolean which is true when DLQ is set on the topic, false otherwise
     */
    boolean isDlqEnabledOnTopic(String topic);

    /**
     * Check if the cluster config to auto create DLQ topics is enabled.
     *
     * @return Boolean which is true when DLQ topic auto create cluster config is set, false otherwise
     */
    boolean isDlqAutoTopicCreateEnabled();

    /**
     * Return optional of string representing the configured DLQ prefix.
     *
     * @return Optional of string representing DLQ prefix if configured, empty otherwise
     */
    Optional<String> shareGroupDlqTopicPrefix();

    /**
     * Check is copy record data into DLQ topic is enabled.
     *
     * @param groupId The id of the share group
     * @return Boolean which is true if config is set, false otherwise
     */
    boolean isShareGroupDlqCopyRecordEnabled(String groupId);

    /**
     * Check if a topic is present in the metadata cache.
     *
     * @param topic The name of the topic
     * @return Boolean which is true is topic exists, false otherwise
     */
    boolean containsTopic(String topic);

    /**
     * Get the max message size, in bytes, allowed to be produced to the given topic. Honors
     * a topic-level {@code max.message.bytes} override when present, otherwise falls back to
     * the broker's configured {@code message.max.bytes}.
     *
     * @param topic The name of the topic
     * @return The maximum message size, in bytes, allowed for the topic
     */
    int dlqTopicMaxMessageBytes(String topic);

    /**
     * Get all nodes in the kafka cluster encapsulated in the {@link Node} object.
     *
     * @return List of nodes representing the cluster nodes
     */
    List<Node> getClusterNodes();

    /**
     * Fetch topic name, based on the topic id.
     *
     * @param topicId The uuid of the topic
     * @return Optional specifying the name, or empty in case of error/not found.
     */
    Optional<String> topicName(Uuid topicId);

    /**
     * Fetch topic partition data, based on the topic name.
     *
     * @param topicName The name of the topic
     * @return TopicPartitionData java record specifying the information.
     */
    TopicPartitionData topicPartitionData(String topicName);
}
