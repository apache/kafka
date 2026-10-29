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
package org.apache.kafka.coordinator.group;

import org.apache.kafka.common.Uuid;

/**
 * Functional interface for validating offset commits on a per-partition basis.
 */
@FunctionalInterface
public interface CommitPartitionValidator {
    /**
     * Validates an offset commit for a specific partition.
     *
     * @param topicName   The topic name.
     * @param topicId     The topic id (may be ZERO_UUID if not available).
     * @param partitionId The partition index.
     */
    void validate(String topicName, Uuid topicId, int partitionId);

    /**
     * A no-op validator that performs no validation.
     */
    CommitPartitionValidator NO_OP = (topicName, topicId, partitionId) -> { };
}
