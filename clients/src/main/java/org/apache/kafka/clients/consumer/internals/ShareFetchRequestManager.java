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

import org.apache.kafka.common.requests.ShareFetchRequest;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;

import java.util.function.Predicate;

import static org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.PollResult.EMPTY;

/**
 * {@code ShareFetchRequestManager} is responsible for generating {@link ShareFetchRequest} that
 * represent the {@link SubscriptionState#fetchablePartitions(Predicate)} based on the share group
 * consumer's assignment.
 */
public class ShareFetchRequestManager implements RequestManager {

    private final Logger log;
    private final LogContext logContext;
    private final Time time;
    private final ConsumerMetadata metadata;
    private final SubscriptionState subscriptions;
    private final ShareFetchBuffer shareFetchBuffer;
    private final NetworkClientDelegate networkClientDelegate;

    ShareFetchRequestManager(final LogContext logContext,
                             final Time time,
                             final ConsumerMetadata metadata,
                             final SubscriptionState subscriptions,
                             final ShareFetchBuffer shareFetchBuffer,
                             final NetworkClientDelegate networkClientDelegate) {
        this.log = logContext.logger(AbstractFetch.class);
        this.logContext = logContext;
        this.time = time;
        this.metadata = metadata;
        this.subscriptions = subscriptions;
        this.shareFetchBuffer = shareFetchBuffer;
        this.networkClientDelegate = networkClientDelegate;
    }

    @Override
    public NetworkClientDelegate.PollResult poll(long currentTimeMs) {
        return EMPTY;
    }

    @Override
    public NetworkClientDelegate.PollResult pollOnClose() {
        return EMPTY;
    }
}