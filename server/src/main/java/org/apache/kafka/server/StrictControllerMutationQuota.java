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

import org.apache.kafka.common.errors.ThrottlingQuotaExceededException;
import org.apache.kafka.common.metrics.QuotaViolationException;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.Time;

import java.util.Objects;

/**
 * The StrictControllerMutationQuota defines a strict quota for a given user/clientId pair. The
 * quota is strict meaning that 1) it does not accept any mutations once the quota is exhausted
 * until it gets back to the defined rate; and 2) it does not throttle for any number of mutations
 * if quota is not already exhausted.
 * time @Time object to use
 * quotaSensor @Sensor object with a defined quota for a given user/clientId pair
 */
public class StrictControllerMutationQuota extends AbstractControllerMutationQuota {
    private final Sensor quotaSensor;

    public StrictControllerMutationQuota(Time time, Sensor quotaSensor) {
        super(Objects.requireNonNull(time, "time cannot be null"));
        this.quotaSensor = Objects.requireNonNull(quotaSensor, "quotaSensor cannot be null");
    }

    @Override
    public boolean isExceeded() {
        return lastThrottleTimeMs > 0;
    }

    @Override
    public void record(double permits) {
        var timeMs = time.milliseconds();
        try {
            synchronized (quotaSensor) {
                quotaSensor.checkQuotas(timeMs);
                quotaSensor.record(permits, timeMs, false);
            }
        } catch (QuotaViolationException e) {
            updateThrottleTime(e, timeMs);
            throw new ThrottlingQuotaExceededException((int) lastThrottleTimeMs, Errors.THROTTLING_QUOTA_EXCEEDED.message());
        }
    }
}
