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
package org.apache.kafka.server.common;

public final class MinInSyncReplicas {

    private MinInSyncReplicas() {
    }

    /**
     * min.insync.replicas has no upper bound, so it can be larger than the replica count. The broker, the controller
     * and TopicCommand all use this effective value; keeping the rule in one place stops them from disagreeing on
     * whether a partition is under or at min ISR.
     *
     * @param minInSyncReplicas the configured min.insync.replicas
     * @param replicaCount      the number of replicas of the partition
     * @return the lesser of the two
     */
    public static int effectiveMinIsr(int minInSyncReplicas, int replicaCount) {
        return Math.min(minInSyncReplicas, replicaCount);
    }
}
