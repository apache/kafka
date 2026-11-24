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
package org.apache.kafka.server;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Names of the metrics registered by the broker's ReplicaManager.
 * Metrics are exposed as MBeans named {@code kafka.server:type=ReplicaManager,name=<name>}.
 */
public final class ReplicaManager {

    public static final String LEADER_COUNT = "LeaderCount";
    public static final String PARTITION_COUNT = "PartitionCount";
    public static final String OFFLINE_REPLICA_COUNT = "OfflineReplicaCount";
    public static final String UNDER_REPLICATED_PARTITIONS = "UnderReplicatedPartitions";
    public static final String UNDER_MIN_ISR_PARTITION_COUNT = "UnderMinIsrPartitionCount";
    public static final String AT_MIN_ISR_PARTITION_COUNT = "AtMinIsrPartitionCount";
    public static final String REASSIGNING_PARTITIONS = "ReassigningPartitions";
    public static final String PARTITIONS_WITH_LATE_TRANSACTIONS_COUNT = "PartitionsWithLateTransactionsCount";
    public static final String PRODUCER_ID_COUNT = "ProducerIdCount";
    public static final String ISR_EXPANDS_PER_SEC = "IsrExpandsPerSec";
    public static final String ISR_SHRINKS_PER_SEC = "IsrShrinksPerSec";
    public static final String FAILED_ISR_UPDATES_PER_SEC = "FailedIsrUpdatesPerSec";

    public static final Set<String> GAUGE_METRIC_NAMES = Set.of(
        LEADER_COUNT,
        PARTITION_COUNT,
        OFFLINE_REPLICA_COUNT,
        UNDER_REPLICATED_PARTITIONS,
        UNDER_MIN_ISR_PARTITION_COUNT,
        AT_MIN_ISR_PARTITION_COUNT,
        REASSIGNING_PARTITIONS,
        PARTITIONS_WITH_LATE_TRANSACTIONS_COUNT,
        PRODUCER_ID_COUNT
    );

    public static final Set<String> METER_METRIC_NAMES = Set.of(
        ISR_EXPANDS_PER_SEC,
        ISR_SHRINKS_PER_SEC,
        FAILED_ISR_UPDATES_PER_SEC
    );

    public static final Set<String> ALL_METRIC_NAMES = Stream.concat(GAUGE_METRIC_NAMES.stream(), METER_METRIC_NAMES.stream())
        .collect(Collectors.toUnmodifiableSet());

    private ReplicaManager() {
    }
}
