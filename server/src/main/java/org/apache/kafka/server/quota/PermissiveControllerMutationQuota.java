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
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.Time;

import java.util.Objects;

/**
 * The PermissiveControllerMutationQuota defines a permissive quota for a given user/clientId pair.
 * The quota is permissive meaning that 1) it does accept any mutations even if the quota is
 * exhausted; and 2) it does throttle as soon as the quota is exhausted.
 */
public class PermissiveControllerMutationQuota extends AbstractControllerMutationQuota {
    private final Sensor quotaSensor;

    /**
     * Creates a new PermissiveControllerMutationQuota with the specified time source and quota sensor.
     *
     * @param time the Time object used for time-based calculations and quota tracking
     * @param quotaSensor the Sensor object that tracks quota usage for a specific user/clientId pair
     * @throws IllegalArgumentException if time or quotaSensor is null
     */
    public PermissiveControllerMutationQuota(Time time, Sensor quotaSensor) {
        super(time);
        this.quotaSensor = Objects.requireNonNull(quotaSensor, "quotaSensor cannot be null");
    }

    @Override
    public boolean isExceeded() {
        return false;
    }

    @Override
    public void record(double permits) {
        var timeMs = time.milliseconds();
        try {
            quotaSensor.record(permits, timeMs, true);
        } catch (QuotaViolationException e) {
            updateThrottleTime(e, timeMs);
        }
    }
}