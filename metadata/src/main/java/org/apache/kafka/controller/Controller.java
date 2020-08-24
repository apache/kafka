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

package org.apache.kafka.controller;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public interface Controller extends AutoCloseable {
    /**
     * Change partition ISRs.
     *
     * @param brokerId      The ID of the broker making the change.
     * @param brokerEpoch   The epoch of the broker making the change.
     * @param changes       The changes to make.
     *
     * @return              A map from partitions to error results.
     */
    CompletableFuture<Map<TopicPartition, Errors>>
        alterIsr(int brokerId, long brokerEpoch, Map<TopicPartition, LeaderAndIsr> changes);

    /**
     * Elect new partition leaders.
     *
     * @param timeoutMs     The timeout to use.
     * @param parts         The partitions to elect new leaders for.
     * @param unclean       If this is true, we will elect the first live replic if
     *                      there are no in-sync replicas.
     *
     * @return              A map from partitions to error results.
     */
    CompletableFuture<Map<TopicPartition, PartitionLeaderElectionResult>>
        electLeaders(int timeoutMs, Set<TopicPartition> parts, boolean unclean);

    /**
     * Begin shutting down, but don't block.  You must still call close to clean up all
     * resources.
     */
    void beginShutdown();

    /**
     * Blocks until we have shut down and freed all resources.
     */
    void close();
}
