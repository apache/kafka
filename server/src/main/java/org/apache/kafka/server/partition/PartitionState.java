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
package org.apache.kafka.server.partition;

import org.apache.kafka.metadata.LeaderRecoveryState;

import java.util.Set;

/**
 * Represents the state of a partition, including its In-Sync Replicas (ISR) and leader recovery state.
 */
public interface PartitionState {
    /**
     * Includes only the in-sync replicas which have been committed to Controller.
     */
    Set<Integer> isr();

    /**
     * This set may include uncommitted ISR members following an expansion. This "effective" ISR is used for advancing
     * the high watermark as well as determining which replicas are required for acks=all produce requests.*
     */
    Set<Integer> maximalIsr();

    /**
     * The leader recovery state. See the description for LeaderRecoveryState for details on the different values.
     */
    LeaderRecoveryState leaderRecoveryState();

    /**
     * Indicates if we have an AlterPartition request inflight.
     */
    boolean isInflight();

}
