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

/**
 * The target assignment metadata.
 *
 * @param assignmentEpoch     The target assignment epoch. An assignment epoch smaller than the
 *                            group epoch means that a new assignment is required. The
 *                            assignment epoch is updated when a new assignment is installed.
 * @param assignmentTimestamp The time at which the target assignment calculation finished.
 */
public record TargetAssignmentMetadata(int assignmentEpoch, long assignmentTimestamp) {
    /**
     * The initial target assignment metadata for groups.
     * This is different to tombstoned assignment metadata which has an assignment epoch of -1.
     */
    public static final TargetAssignmentMetadata ZERO = new TargetAssignmentMetadata(0, 0L);

    public TargetAssignmentMetadata {
        if (assignmentEpoch < 0 && assignmentEpoch != -1) {
            throw new IllegalArgumentException("The assignment epoch must be non-negative or -1.");
        }
        if (assignmentTimestamp < 0) {
            throw new IllegalArgumentException("The assignment timestamp must be non-negative.");
        }
    }
}
