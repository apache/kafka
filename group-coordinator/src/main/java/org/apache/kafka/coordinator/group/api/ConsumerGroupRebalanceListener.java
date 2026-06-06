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
 * which indicates that a rebalance has occurred.
 * <p>
 * Implementations of this interface can be used for monitoring, alerting, or logging purposes
 * to track which consumer groups are experiencing rebalances.
 */
public interface ConsumerGroupRebalanceListener {

    /**
     * Called when a consumer group rebalance occurs.
     *
     * @param groupId The ID of the consumer group that rebalanced
     * @param groupType The type of group that rebalanced
     * @param reason The rebalance reason
     * @param eventTimeMs The event timestamp in milliseconds
     */
    void onConsumerGroupRebalance(String groupId, String groupType, String reason, long eventTimeMs);
}
