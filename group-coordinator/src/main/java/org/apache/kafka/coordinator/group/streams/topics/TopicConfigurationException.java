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

import org.apache.kafka.common.requests.StreamsGroupHeartbeatResponse;
import org.apache.kafka.common.requests.StreamsGroupHeartbeatResponse.Status;

public class TopicConfigurationException extends RuntimeException {

    private final Status status;

    public TopicConfigurationException(StreamsGroupHeartbeatResponse.Status status, String message) {
        super(message);
        this.status = status;
    }

    public Status status() {
        return status;
    }

    public static TopicConfigurationException incorrectlyPartitionedTopics(String message) {
        return new TopicConfigurationException(Status.INCORRECTLY_PARTITIONED_TOPICS, message);
    }

    public static TopicConfigurationException missingSourceTopics(String message) {
        return new TopicConfigurationException(Status.MISSING_SOURCE_TOPICS, message);
    }

    public static TopicConfigurationException missingInternalTopics(String message) {
        return new TopicConfigurationException(Status.MISSING_INTERNAL_TOPICS, message);
    }
}
