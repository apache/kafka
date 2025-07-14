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
package org.apache.kafka.server.quota;

import org.apache.kafka.common.metrics.QuotaViolationException;
import org.apache.kafka.common.utils.Time;

import java.util.Objects;

/**
 * The AbstractControllerMutationQuota is the base class of StrictControllerMutationQuota and
 * PermissiveControllerMutationQuota.
 */
public abstract class AbstractControllerMutationQuota implements ControllerMutationQuota {
    protected final Time time;
    protected long lastThrottleTimeMs = 0L;
    private long lastRecordedTimeMs = 0L;

    /**
     * @param time Time object to use
     */
    protected AbstractControllerMutationQuota(Time time) {
        this.time = Objects.requireNonNull(time, "time cannot be null");
    }

    protected void updateThrottleTime(QuotaViolationException e, long timeMs) {
        lastThrottleTimeMs = ControllerMutationQuotaManager.throttleTimeMs(e);
        lastRecordedTimeMs = timeMs;
    }

    @Override
    public int throttleTime() {
        // If no throttle time has been recorded, return 0
        if (lastThrottleTimeMs == 0L) {
            return 0;
        }
        
        // If a throttle time has been recorded, we adjust it by deducting the time elapsed
        // between the recording and now. We do this because `throttleTime` may be called
        // long after having recorded it, especially when a request waits in the purgatory.
        var deltaTimeMs = time.milliseconds() - lastRecordedTimeMs;
        return Math.max(0, (int) (lastThrottleTimeMs - deltaTimeMs));
    }
}