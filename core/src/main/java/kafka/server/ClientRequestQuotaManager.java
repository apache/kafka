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
package kafka.server;

import kafka.network.RequestChannel;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.QuotaViolationException;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.config.ClientQuotaManagerConfig;
import org.apache.kafka.server.quota.ClientQuotaCallback;
import org.apache.kafka.server.quota.QuotaType;
import org.apache.kafka.server.quota.QuotaUtils;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import scala.jdk.javaapi.CollectionConverters;
import scala.jdk.javaapi.OptionConverters;

@SuppressWarnings("this-escape")
public class ClientRequestQuotaManager extends ClientQuotaManager {
    // Since exemptSensor is for all clients and has a constant name, we do not expire exemptSensor and only
    // create once.
    static final double NANOS_TO_PERCENTAGE_PER_SECOND = 100.0 / TimeUnit.SECONDS.toNanos(1);
    private static final long DEFAULT_INACTIVE_EXEMPT_SENSOR_EXPIRATION_TIME_SECONDS = Long.MAX_VALUE;
    private static final String EXEMPT_SENSOR_NAME = "exempt-" + QuotaType.REQUEST;

    private final long maxThrottleTimeMs;
    private final Metrics metrics;
    private final MetricName exemptMetricName;
    // Visible for testing
    private final Sensor exemptSensor;

    public ClientRequestQuotaManager(ClientQuotaManagerConfig config, Metrics metrics, Time time, String threadNamePrefix, Optional<ClientQuotaCallback> quotaCallback) {
        super(config, metrics, QuotaType.REQUEST, time, threadNamePrefix, OptionConverters.toScala(quotaCallback));
        this.maxThrottleTimeMs = TimeUnit.SECONDS.toMillis(config.quotaWindowSizeSeconds);
        this.metrics = metrics;
        this.exemptMetricName = metrics.metricName("exempt-request-time", QuotaType.REQUEST.toString(), "Tracking exempt-request-time utilization percentage");
        exemptSensor = getOrCreateSensor(EXEMPT_SENSOR_NAME, DEFAULT_INACTIVE_EXEMPT_SENSOR_EXPIRATION_TIME_SECONDS, sensor -> sensor.add(exemptMetricName, new Rate()));
    }

    public Sensor exemptSensor() {
        return exemptSensor;
    }

    private void recordExempt(double value) {
        exemptSensor.record(value);
    }

    /**
     * Records that a user/clientId changed request processing time being throttled. If quota has been violated, return
     * throttle time in milliseconds. Throttle time calculation may be overridden by sub-classes.
     * @param request client request
     * @return Number of milliseconds to throttle in case of quota violation. Zero otherwise
     */
    public int maybeRecordAndGetThrottleTimeMs(RequestChannel.Request request, long timeMs) {
        if (quotasEnabled()) {
            request.setRecordNetworkThreadTimeCallback(timeNanos -> {
                recordNoThrottle(request.session(), request.header().clientId(), nanosToPercentage(Long.parseLong(timeNanos.toString())));
            });
            return recordAndGetThrottleTimeMs(request.session(), request.header().clientId(), nanosToPercentage(request.requestThreadTimeNanos()), timeMs);
        } else {
            return 0;
        }
    }

    public void maybeRecordExempt(RequestChannel.Request request) {
        if (quotasEnabled()) {
            request.setRecordNetworkThreadTimeCallback(timeNanos -> {
                recordExempt(nanosToPercentage(Long.parseLong(timeNanos.toString())));
            });
            recordExempt(nanosToPercentage(request.requestThreadTimeNanos()));
        }
    }

    @Override
    public long throttleTime(QuotaViolationException e, long timeMs) {
        return QuotaUtils.boundedThrottleTime(e, maxThrottleTimeMs, timeMs);
    }

    @Override
    public MetricName clientQuotaMetricName(scala.collection.immutable.Map<String, String> quotaMetricTags) {
        return metrics.metricName("request-time", QuotaType.REQUEST.toString(), "Tracking request-time per user/client-id", CollectionConverters.asJava(quotaMetricTags));
    }

    private double nanosToPercentage(long nanos) {
        return nanos * NANOS_TO_PERCENTAGE_PER_SECOND;
    }
}
