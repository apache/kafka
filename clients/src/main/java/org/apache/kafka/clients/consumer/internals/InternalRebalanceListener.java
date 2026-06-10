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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.common.TopicPartition;

import java.util.Collection;

/**
 * Internal counterpart to the public rebalance listener interfaces. The consumer's runtime internals
 * (subscription state, rebalance listener invoker, membership management) operates exclusively against
 * this type rather than the user-facing {@link org.apache.kafka.clients.consumer.RebalanceListener}.
 *
 * <p>Keeping a single internal abstraction with the simple one-argument signatures means the invocation
 * path can pass implementation-specific arguments and lifecycle hooks without leaking them into the public
 * API.
 */
public abstract class InternalRebalanceListener {
    /**
     * Invoked before a set of partitions is removed from this consumer's assignment, giving the
     * implementation a chance to flush state or commit offsets while the partitions are still owned.
     *
     * @param partitions partitions being revoked from the assignment
     */
    public abstract void onPartitionsRevoked(Collection<TopicPartition> partitions);

    /**
     * Invoked after a rebalance completes with the set of partitions newly added to this consumer's
     * assignment. Always fires exactly once per rebalance, even when the collection is empty, so it
     * doubles as a notification that a rebalance has occurred.
     *
     * @param partitions partitions added to the assignment
     */
    public abstract void onPartitionsAssigned(Collection<TopicPartition> partitions);

    /**
     * Invoked when partitions are removed from this consumer's assignment without an opportunity for
     * graceful revocation, for example after a session timeout. The partitions are no longer owned by
     * this consumer at the time of the call, so committing offsets for them is not meaningful.
     *
     * @param partitions partitions that were lost
     */
    public abstract void onPartitionsLost(Collection<TopicPartition> partitions);
}
