/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.server.share;

import org.apache.kafka.common.errors.ThrottlingQuotaExceededException;
import org.apache.kafka.common.metrics.QuotaViolationException;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.TokenBucket;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.Time;

/**
 * The ControllerMutationQuota trait defines a quota for a given user/clientId pair. Such
 * quota is not meant to be cached forever but rather during the lifetime of processing
 * a request.
 */

interface ControllerMutationQuota {
    boolean isExceeded();
    void record(double permits);
    int throttleTime();
}

/**
 * Default quota used when quota is disabled.
 */
class UnboundedControllerMutationQuota implements ControllerMutationQuota {

    static final UnboundedControllerMutationQuota INSTANCE = new UnboundedControllerMutationQuota();
    private UnboundedControllerMutationQuota() {}

    @Override
    public boolean isExceeded() {
        return false;
    }

    @Override
    public void record(double permits) {
    }

    @Override
    public int throttleTime() {
        return 0;
    }
}

/**
 * The AbstractControllerMutationQuota is the base class of StrictControllerMutationQuota and
 * PermissiveControllerMutationQuota.
 *
 * @param /time @Time object to use
 */
abstract class AbstractControllerMutationQuota implements ControllerMutationQuota {
    protected final Time time;
    protected long lastThrottleTimeMs = 0L;
    private long lastRecordedTimeMs = 0L;

    protected AbstractControllerMutationQuota(Time time) {
        this.time = time;
    }

    protected void updateThrottleTime(QuotaViolationException e, long timeMs) {
        lastRecordedTimeMs = ControllerMutationQuotaManager.INSTANCE.throttleTimeMs(e);
        lastRecordedTimeMs = timeMs;
    }

    @Override
    public int throttleTime() {
        var deltaTimeMs = time.milliseconds() - lastRecordedTimeMs;
        return Math.max(0, (int)(lastThrottleTimeMs - deltaTimeMs));
    }
}

/**
 * The StrictControllerMutationQuota defines a strict quota for a given user/clientId pair. The
 * quota is strict meaning that 1) it does not accept any mutations once the quota is exhausted
 * until it gets back to the defined rate; and 2) it does not throttle for any number of mutations
 * if quota is not already exhausted.
 *
 * @param /time @Time object to use
 * @param /quotaSensor @Sensor object with a defined quota for a given user/clientId pair
 */
class StrictControllerMutationQuota extends AbstractControllerMutationQuota {
    private final Sensor quotaSensor;

    StrictControllerMutationQuota(Time time, Sensor quotaSensor) {
        super(time);
        this.quotaSensor = quotaSensor;

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
        } catch (QuotaViolationException e){
            updateThrottleTime(e, timeMs);
            throw  new ThrottlingQuotaExceededException((int)(lastThrottleTimeMs), Errors.THROTTLING_QUOTA_EXCEEDED.message());
        }
    }
}

/**
 * The PermissiveControllerMutationQuota defines a permissive quota for a given user/clientId pair.
 * The quota is permissive meaning that 1) it does accept any mutations even if the quota is
 * exhausted; and 2) it does throttle as soon as the quota is exhausted.
 *
 * @param /time @Time object to use
 * @param /quotaSensor @Sensor object with a defined quota for a given user/clientId pair
 */

class PermissiveControllerMutationQuota extends AbstractControllerMutationQuota {
    private final Sensor quotaSensor;

    PermissiveControllerMutationQuota(Time time, Sensor quotaSensor) {
        super(time);
        this.quotaSensor = quotaSensor;
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

public class ControllerMutationQuotaManager {
    static final ControllerMutationQuotaManager INSTANCE = new ControllerMutationQuotaManager();

    /**
     * This calculates the amount of time needed to bring the TokenBucket within quota
     * assuming that no new metrics are recorded.
     *
     * Basically, if a value < 0 is observed, the time required to bring it to zero is
     * -value / refill rate (quota bound) * 1000.
     */
    long throttleTimeMs(QuotaViolationException e) {
        if (e.metric().measurable() instanceof TokenBucket) {
            return Math.round(-e.value() / e.bound() * 1000);
        } else {
            throw new IllegalArgumentException(
                    "Metric " + e.metric().metricName() + " is not a TokenBucket metric, value " + e.metric().measurable());
        }
    }
}
