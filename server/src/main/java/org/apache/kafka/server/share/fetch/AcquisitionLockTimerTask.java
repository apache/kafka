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
package org.apache.kafka.server.share.fetch;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.share.metrics.SharePartitionMetrics;
import org.apache.kafka.server.util.timer.TimerTask;

/**
 * AcquisitionLockTimerTask is a timer task that is executed when the acquisition lock timeout is reached.
 * It releases the acquired records.
 */
public class AcquisitionLockTimerTask extends TimerTask {

    private final long expirationMs;
    private final String memberId;
    private final long firstOffset;
    private final long lastOffset;
    private final AcquisitionLockTimeoutHandler timeoutHandler;
    private final SharePartitionMetrics sharePartitionMetrics;
    private volatile boolean hasExpired;

    public AcquisitionLockTimerTask(
        Time time,
        long delayMs,
        String memberId,
        long firstOffset,
        long lastOffset,
        AcquisitionLockTimeoutHandler timeoutHandler,
        SharePartitionMetrics sharePartitionMetrics
    ) {
        super(delayMs);
        this.expirationMs = time.hiResClockMs() + delayMs;
        this.memberId = memberId;
        this.firstOffset = firstOffset;
        this.lastOffset = lastOffset;
        this.timeoutHandler = timeoutHandler;
        this.sharePartitionMetrics = sharePartitionMetrics;
        this.hasExpired = false;
    }

    public long expirationMs() {
        return expirationMs;
    }

    public boolean hasExpired() {
        return hasExpired;
    }

    /**
     * The task is executed when the acquisition lock timeout is reached. The task releases the acquired records.
     */
    @Override
    public void run() {
        // Mark the request as expired prior executing the timeout. There might be concurrent execution
        // of timeout task and failed acknowledgement which checks if the timeout task has expired.
        // But only one shall update the state to available. The concurrent execution is protected by
        // write lock on the state.
        hasExpired = true;
        sharePartitionMetrics.recordAcquisitionLockTimeoutPerSec(lastOffset - firstOffset + 1);
        timeoutHandler.handle(memberId, firstOffset, lastOffset, this);
    }
}
