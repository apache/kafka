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

import org.apache.kafka.common.config.ConfigException;

import java.util.Optional;

/**
 * Shared validation logic for DLQ managers ({@link ShareGroupDLQStateManager} and
 * {@code K2ShareGroupDLQManager}).
 */
public final class ShareGroupDLQValidator {

    private ShareGroupDLQValidator() {}

    /**
     * Validates the fields of a {@link ShareGroupDLQRecordParameter}.
     *
     * @throws IllegalArgumentException if any field is invalid
     */
    public static void validateParam(ShareGroupDLQRecordParameter param) {
        String prefix = "DLQ records parameters";
        if (param == null) {
            throw new IllegalArgumentException(prefix + " cannot be null.");
        }
        if (param.groupId() == null || param.groupId().isEmpty()) {
            throw new IllegalArgumentException(prefix + " group cannot be null or empty.");
        }
        if (param.topicIdPartition() == null) {
            throw new IllegalArgumentException(prefix + " topic/partition data cannot be null or empty.");
        }
        if (param.topicIdPartition().topicId() == null) {
            throw new IllegalArgumentException(prefix + " topic id data cannot be null or empty.");
        }
        if (param.topicIdPartition().partition() < 0) {
            throw new IllegalArgumentException(prefix + " partition cannot be negative.");
        }
        if (param.firstOffset() < 0) {
            throw new IllegalArgumentException(prefix + " first offset cannot be negative.");
        }
        if (param.lastOffset() < 0) {
            throw new IllegalArgumentException(prefix + " last offset cannot be negative.");
        }
        if (param.lastOffset() < param.firstOffset()) {
            throw new IllegalArgumentException(prefix + " last offset cannot be less than first offset.");
        }
    }

    /**
     * Validates DLQ topic configuration. Checks that the topic name does not start with {@code __},
     * that DLQ is enabled on the topic (if it exists), and that the topic name complies with the
     * configured prefix.
     *
     * <p>Callers are responsible for checking that the topic name is present in the config (non-empty)
     * before calling this method, and for any implementation-specific checks (e.g., K1 auto-create).
     *
     * @param groupId           the share group ID, for error messages
     * @param userTopicName     the raw DLQ topic name from config (without tenant prefix)
     * @param resolvedTopicName the topic name used for metadata cache lookups (may include tenant
     *                          prefix in K2; same as {@code userTopicName} in K1)
     * @param cacheHelper       metadata cache helper for topic lookups
     * @return an error if validation fails, or empty if valid
     */
    public static Optional<Throwable> validateDlqTopicConfig(
            String groupId,
            String userTopicName,
            String resolvedTopicName,
            ShareGroupDLQMetadataCacheHelper cacheHelper
    ) {
        if (userTopicName.startsWith("__")) {
            return Optional.of(new ConfigException(String.format(
                    "Configured DLQ topic name in share group: %s cannot start with __, topic: %s.", groupId, userTopicName)));
        }

        // Verify that DLQ is enabled on a correctly named topic, configured on a share group.
        if (cacheHelper.containsTopic(resolvedTopicName) && !cacheHelper.isDlqEnabledOnTopic(resolvedTopicName)) {
            return Optional.of(new ConfigException(
                    "DLQ is not enabled on configured DLQ topic for share group: "
                            + groupId + ", topic: " + userTopicName));
        }

        Optional<String> topicPrefix = cacheHelper.shareGroupDlqTopicPrefix();
        return topicPrefix.map(prefix -> {
            if (!prefix.isEmpty() && !userTopicName.startsWith(prefix)) {
                return new ConfigException(
                        "Configured DLQ topic name does not comply with the DLQ topic prefix in share group: "
                                + groupId + ", topic: " + userTopicName + ", prefix: " + prefix);
            }
            return null;
        });
    }
}
