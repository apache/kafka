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

/**
 * {@code FetchUtils} provides a place for disparate parts of the fetch logic to live.
 */
public class FetchUtils {

    /**
     * Performs two combined actions based on the state related to the {@link TopicPartition}:
     *
     * <ol>
     *     <li>
     *         Invokes {@link ConsumerMetadata#requestUpdate(boolean)} to signal that the metadata is incorrect and
     *         needs to be updated
     *     </li>
     *     <li>
     *         Invokes {@link SubscriptionState#clearPreferredReadReplica(TopicPartition)} to clear out any read replica
     *         information that may be present.
     *     </li>
     * </ol>
     *
     * This utility method should be invoked if the client detects (or is told by a node in the broker) that an
     * attempt was made to fetch from a node that isn't the leader or preferred replica.
     *
     * @param metadata {@link ConsumerMetadata} for which to request an update
     * @param subscriptions {@link SubscriptionState} to clear any internal read replica node
     * @param topicPartition {@link TopicPartition} for which this state change is related
     */
    static void requestMetadataUpdate(final ConsumerMetadata metadata,
                                      final SubscriptionState subscriptions,
                                      final TopicPartition topicPartition) {
        metadata.requestUpdate(false);
        subscriptions.clearPreferredReadReplica(topicPartition);
    }
}
