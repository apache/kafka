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

import org.apache.kafka.common.utils.ExponentialBackoff;

import java.util.OptionalLong;
import java.util.function.UnaryOperator;

import static org.apache.kafka.common.requests.AssignReplicasToDirsRequest.MAX_ASSIGNMENTS_PER_REQUEST;

/**
 * This class calculates when the MaybeSendAssignmentsEvent should run for AssignmentsManager.
 */
public class AssignmentsManagerDeadlineFunction implements UnaryOperator<OptionalLong> {

    /**
     * The exponential backoff to use.
     */
    private final ExponentialBackoff backoff;

    /**
     * The current time in monotonic nanoseconds.
     */
    private final long nowNs;

    /**
     * The number of global failures immediately prior to this attempt.
     */
    private final int previousGlobalFailures;

    /**
     * True if there are current inflight requests.
     */
    private final boolean hasInflightRequests;

    /**
     * The number of requests that are ready to send.
     */
    private final int numReadyRequests;

    AssignmentsManagerDeadlineFunction(
        ExponentialBackoff backoff,
        long nowNs,
        int previousGlobalFailures,
        boolean hasInflightRequests,
        int numReadyRequests
    ) {
        this.backoff = backoff;
        this.nowNs = nowNs;
        this.previousGlobalFailures = previousGlobalFailures;
        this.hasInflightRequests = hasInflightRequests;
        this.numReadyRequests = numReadyRequests;
    }

    @Override
    public OptionalLong apply(OptionalLong previousSendTimeNs) {
        long delayNs;
        if (previousGlobalFailures > 0) {
            // If there were global failures (like a response timeout), we want to wait for the
            // full backoff period.
            delayNs = backoff.backoff(previousGlobalFailures);
        } else if ((numReadyRequests > MAX_ASSIGNMENTS_PER_REQUEST) && !hasInflightRequests) {
            // If there were no previous failures, and we have lots of requests, send it as soon
            // as possible.
            delayNs = 0;
        } else {
            // Otherwise, use the standard delay period. This helps to promote batching, which
            // reduces load on the controller.
            delayNs = backoff.initialInterval();
        }
        long newSendTimeNs = nowNs + delayNs;
        if (previousSendTimeNs.isPresent() && previousSendTimeNs.getAsLong() < newSendTimeNs) {
            // If the previous send time was before the new one we calculated, go with the
            // previous one.
            return previousSendTimeNs;
        }
        // Otherwise, return our new send time.
        return OptionalLong.of(newSendTimeNs);
    }
}
