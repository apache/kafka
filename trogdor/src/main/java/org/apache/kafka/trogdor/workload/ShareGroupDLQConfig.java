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
package org.apache.kafka.trogdor.workload;

import org.apache.kafka.trogdor.rest.Message;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Describes the dead-letter queue (DLQ) setup of the share group used by a ShareConsumeBench workload.
 *
 * Before the consumers start, the worker creates the DLQ topic as described by "topicSpec" (or with the default
 * partitions spec if absent), enables it for use as a DLQ, and points the share group at it. If the topic already
 * exists it is reused and only enabled as a DLQ.
 *
 * Example:
 * {
 *    "topicName": "sg-dlq",
 *    "topicSpec": {"numPartitions": 1, "replicationFactor": 3},
 *    "copyRecordEnable": true
 * }
 */
public class ShareGroupDLQConfig extends Message {
    private final String topicName;
    private final PartitionsSpec topicSpec;
    private final boolean copyRecordEnable;

    @JsonCreator
    public ShareGroupDLQConfig(@JsonProperty("topicName") String topicName,
                               @JsonProperty("topicSpec") PartitionsSpec topicSpec,
                               @JsonProperty("copyRecordEnable") Boolean copyRecordEnable) {
        if (topicName == null || topicName.isEmpty() || topicName.startsWith("__")) {
            throw new IllegalArgumentException("topicName must be non-empty and must not start with \"__\", but was " + topicName);
        }
        this.topicName = topicName;
        this.topicSpec = topicSpec == null ? new PartitionsSpec(0, (short) 0, null, null) : topicSpec;
        this.copyRecordEnable = copyRecordEnable != null && copyRecordEnable;
    }

    @JsonProperty
    public String topicName() {
        return topicName;
    }

    @JsonProperty
    public PartitionsSpec topicSpec() {
        return topicSpec;
    }

    @JsonProperty
    public boolean copyRecordEnable() {
        return copyRecordEnable;
    }
}
