/*
 * Copyright (c) 2025 Aiven, Helsinki, Finland. https://aiven.io/
 */
package org.apache.kafka.controller;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.timeline.TimelineHashMap;

import java.util.Map;
import java.util.Set;

/**
 * The policy class that allows putting limitations such as "max user topics", "max user partitions",
 * and "max partitions per user topic".
 */
final class AivenTopicPolicy implements Configurable {
    private static final long LATEST_SNAPSHOT = Long.MAX_VALUE;

    private volatile AivenTopicPolicyConfig config;
    private volatile Set<String> excludedTopics;

    @Override
    public void configure(final Map<String, ?> configs) {
        this.config = new AivenTopicPolicyConfig(configs);
        this.excludedTopics = config.excludedTopics();
    }

    ApiError validateTopicCreation(
        final String topic,
        final int partitions,
        final TimelineHashMap<Uuid, ReplicationControlManager.TopicControlInfo> topics
    ) {
        // Always allow for excluded topics.
        if (excludedTopics.contains(topic)) {
            return ApiError.NONE;
        }

        if (config.maxPartitionsPerUserTopic().isPresent()) {
            final int maxPartitionsPerUserTopic = config.maxPartitionsPerUserTopic().get();
            if (partitions > maxPartitionsPerUserTopic) {
                return new ApiError(Errors.POLICY_VIOLATION,
                    String.format("Partition limit exceeded: maximum %d partitions per user topic allowed", maxPartitionsPerUserTopic));
            }
        }

        if (config.maxUserTopics().isPresent() || config.maxUserPartitions().isPresent()) {
            int currentUserTopics = 0;
            int currentUserPartitions = 0;
            for (final var value : topics.values()) {
                if (!excludedTopics.contains(value.name())) {
                    currentUserTopics += 1;
                    currentUserPartitions += value.numPartitions(LATEST_SNAPSHOT);
                }
            }

            if (config.maxUserTopics().isPresent()) {
                final int maxUserTopics = config.maxUserTopics().get();
                if (currentUserTopics >= maxUserTopics) {
                    return new ApiError(Errors.POLICY_VIOLATION,
                        String.format("Topic limit exceeded: maximum %d user topics allowed", maxUserTopics));
                }
            }

            if (config.maxUserPartitions().isPresent()) {
                final int maxUserPartitions = config.maxUserPartitions().get();
                if (currentUserPartitions + partitions > maxUserPartitions) {
                    return new ApiError(Errors.POLICY_VIOLATION,
                        String.format("Partition limit exceeded: maximum %d user partitions allowed", maxUserPartitions));
                }
            }
        }

        return ApiError.NONE;
    }

    ApiError validatePartitionCreation(
        final String topic,
        final int targetPartitions,
        final int additionalPartitions,
        final TimelineHashMap<Uuid, ReplicationControlManager.TopicControlInfo> topics
    ) {
        // Always allow for excluded topics.
        if (excludedTopics.contains(topic)) {
            return ApiError.NONE;
        }

        if (config.maxPartitionsPerUserTopic().isPresent()) {
            final int maxPartitionsPerUserTopic = config.maxPartitionsPerUserTopic().get();
            if (targetPartitions > maxPartitionsPerUserTopic) {
                return new ApiError(Errors.POLICY_VIOLATION,
                    String.format("Partition limit exceeded: maximum %d partitions per user topic allowed", maxPartitionsPerUserTopic));
            }
        }

        if (config.maxUserPartitions().isPresent()) {
            final int maxUserPartitions = config.maxUserPartitions().get();
            int currentUserPartitions = 0;
            for (final var value : topics.values()) {
                if (!excludedTopics.contains(value.name())) {
                    currentUserPartitions += value.numPartitions(LATEST_SNAPSHOT);
                }
            }

            if (currentUserPartitions + additionalPartitions > maxUserPartitions) {
                return new ApiError(Errors.POLICY_VIOLATION,
                    String.format("Partition limit exceeded: maximum %d user partitions allowed", maxUserPartitions));
            }
        }

        return ApiError.NONE;
    }
}
