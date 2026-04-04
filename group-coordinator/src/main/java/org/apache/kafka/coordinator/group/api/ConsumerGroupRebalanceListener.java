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
package org.apache.kafka.coordinator.group.api;

/**
 * A callback interface that users can implement to get notified when a consumer group rebalance occurs.
 * <p>
 * This listener is invoked on the broker side (GroupCoordinator) when a consumer group's epoch is bumped,
 * which indicates that a rebalance has occurred. This can happen when:
 * <ul>
 *   <li>Members join or leave the group</li>
 *   <li>Member subscriptions change</li>
 *   <li>Topic metadata changes (e.g., partition count changes)</li>
 * </ul>
 * <p>
 * Implementations of this interface can be used for monitoring, alerting, or logging purposes
 * to track which consumer groups are experiencing rebalances.
 */
public interface ConsumerGroupRebalanceListener {

    /**
     * Called when a consumer group rebalance occurs.
     *
     * @param groupId The ID of the consumer group that rebalanced
     * @param groupEpoch The new group epoch after the rebalance
     * @param metadataHash The metadata hash of the group after the rebalance
     */
    void onConsumerGroupRebalance(String groupId, int groupEpoch, long metadataHash);
}
















