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

import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;

/**
 * {@code TimedRequestState} adds to a {@link RequestState} a {@link Timer} with which to keep track
 * of the request's expiration.
 */
public class TimedRequestState extends RequestState {

    private final Timer timer;

    public TimedRequestState(final LogContext logContext,
                             final String owner,
                             final long retryBackoffMs,
                             final long retryBackoffMaxMs,
                             final Timer timer) {
        super(logContext, owner, retryBackoffMs, retryBackoffMaxMs);
        this.timer = timer;
    }

    public TimedRequestState(final LogContext logContext,
                             final String owner,
                             final long retryBackoffMs,
                             final int retryBackoffExpBase,
                             final long retryBackoffMaxMs,
                             final double jitter,
                             final Timer timer) {
        super(logContext, owner, retryBackoffMs, retryBackoffExpBase, retryBackoffMaxMs, jitter);
        this.timer = timer;
    }

    public boolean isExpired() {
        timer.update();
        return timer.isExpired();
    }

    public void resetTimeout(long timeoutMs) {
        timer.updateAndReset(timeoutMs);
    }

    public long remainingMs() {
        timer.update();
        return timer.remainingMs();
    }

    public static Timer deadlineTimer(final Time time, final long deadlineMs) {
        long diff = Math.max(0, deadlineMs - time.milliseconds());
        return time.timer(diff);
    }


    @Override
    protected String toStringBase() {
        return super.toStringBase() + ", remainingMs=" + remainingMs();
    }
}
