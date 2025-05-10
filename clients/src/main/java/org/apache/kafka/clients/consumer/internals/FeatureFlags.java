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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FeatureFlags {

    private static final Logger LOG = LoggerFactory.getLogger(FeatureFlags.class);

    public final boolean ignoreEventWakeups;
    public final boolean ignorePrefetchWakeups;
    public final boolean dedupeEventWakeups;
    public final boolean wakeupAfterTrySend;
    public final boolean ignoreNoopPoll;
    public final boolean wakeupAfterPoll;
    public final boolean useQueueForWakeup;

    public FeatureFlags() {
        this.ignoreEventWakeups = featureFlag("IGNORE_EVENT_WAKEUPS");
        this.ignorePrefetchWakeups = featureFlag("IGNORE_PREFETCH_WAKEUPS");
        this.dedupeEventWakeups = featureFlag("DEDUPE_EVENT_WAKEUPS");
        this.wakeupAfterTrySend = featureFlag("WAKEUP_AFTER_TRYSEND");
        this.ignoreNoopPoll = featureFlag("IGNORE_NOOP_POLL");
        this.wakeupAfterPoll = featureFlag("WAKEUP_AFTER_POLL");
        this.useQueueForWakeup = featureFlag("USE_QUEUE_FOR_WAKEUP");
    }

    private static boolean featureFlag(String flag) {
        String s = System.getenv(flag);
        boolean isEnabled = s != null && s.equalsIgnoreCase("true");

        LOG.error("{}: {}", flag, isEnabled);

        return isEnabled;
    }
}
