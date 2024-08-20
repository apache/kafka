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

package org.apache.kafka.clients.consumer.internals.events;

import org.apache.kafka.clients.consumer.internals.SubscriptionState;

/**
 * Event to update the position to fetch from. This will use the committed offsets if available.
 * If no committed offsets exist, it will use the partition offsets.
 *
 * <p/>
 *
 * The event completes with a boolean value indicating if all assigned partitions already had
 * valid fetch positions (based on {@link SubscriptionState#hasAllFetchPositions()}).
 */
public class UpdateFetchPositionsEvent extends CompletableApplicationEvent<Boolean> {

    /**
     * Deadline for the OffsetFetch request needed to update positions. This is expected to be
     * longer than the update positions deadline so that an OffsetFetch request can be
     * used if it completes after the update positions event that triggered it has expired. In
     * that case, the following update positions event will use the retrieved offsets only if it
     * has the same set of partitions.
     */
    private final long fetchOffsetsDeadlineMs;

    public UpdateFetchPositionsEvent(long deadlineMs, long fetchOffsetsDeadlineMs) {
        super(Type.UPDATE_FETCH_POSITIONS, deadlineMs);
        this.fetchOffsetsDeadlineMs = fetchOffsetsDeadlineMs;
    }

    public long fetchOffsetsDeadlineMs() {
        return fetchOffsetsDeadlineMs;
    }

}
