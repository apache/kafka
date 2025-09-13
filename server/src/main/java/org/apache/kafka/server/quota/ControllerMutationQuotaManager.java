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

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.internals.Plugin;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.QuotaViolationException;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.metrics.stats.TokenBucket;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.network.Session;
import org.apache.kafka.server.config.ClientQuotaManagerConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;

/**
 * The ControllerMutationQuotaManager is a specialized ClientQuotaManager used in the context
 * of throttling controller's operations/mutations.
 */
public class ControllerMutationQuotaManager extends ClientQuotaManager {

    private static final Logger LOG = LoggerFactory.getLogger(ControllerMutationQuotaManager.class);

    /**
     * @param config ClientQuotaManagerConfig quota configs
     * @param metrics Metrics instance
     * @param time Time object to use
     * @param threadNamePrefix The thread prefix to use
     * @param quotaCallback ClientQuotaCallback to use
     */
    public ControllerMutationQuotaManager(ClientQuotaManagerConfig config,
                                          Metrics metrics,
                                          Time time,
                                          String threadNamePrefix,
                                          Optional<Plugin<ClientQuotaCallback>> quotaCallback) {
        super(config, metrics, QuotaType.CONTROLLER_MUTATION, time, threadNamePrefix, quotaCallback);
    }

    @Override
    protected MetricName clientQuotaMetricName(Map<String, String> quotaMetricTags) {
        return metrics.metricName("tokens", QuotaType.CONTROLLER_MUTATION.toString(),
                "Tracking remaining tokens in the token bucket per user/client-id",
                quotaMetricTags);
    }

    private MetricName clientRateMetricName(Map<String, String> quotaMetricTags) {
        return metrics.metricName("mutation-rate", QuotaType.CONTROLLER_MUTATION.toString(),
                "Tracking mutation-rate per user/client-id",
                quotaMetricTags);
    }

    @Override
    protected void registerQuotaMetrics(Map<String, String> metricTags, Sensor sensor) {
        sensor.add(
                clientRateMetricName(metricTags),
                new Rate()
        );
        sensor.add(
                clientQuotaMetricName(metricTags),
                new TokenBucket(),
                getQuotaMetricConfig(metricTags)
        );
    }

    /**
     * Records that a user/clientId accumulated or would like to accumulate the provided amount at the
     * specified time, returns throttle time in milliseconds. The quota is strict, meaning that it
     * does not accept any mutations once the quota is exhausted until it gets back to the defined rate.
     *
     * @param session The session from which the user is extracted
     * @param clientId The client id
     * @param value The value to accumulate
     * @param timeMs The time at which to accumulate the value
     * @return The throttle time in milliseconds defines as the time to wait until the average
     *         rate gets back to the defined quota
     */
    @Override
    public int recordAndGetThrottleTimeMs(Session session, String clientId, double value, long timeMs) {
        ClientSensors clientSensors = getOrCreateQuotaSensors(session, clientId);
        Sensor quotaSensor = clientSensors.quotaSensor();

        try {
            synchronized (quotaSensor) {
                quotaSensor.checkQuotas(timeMs);
                quotaSensor.record(value, timeMs, false);
            }
            return 0;
        } catch (QuotaViolationException e) {
            int throttleTimeMs = (int) throttleTimeMs(e);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Quota violated for sensor ({}). Delay time: ({})",
                        quotaSensor.name(), throttleTimeMs);
            }
            return throttleTimeMs;
        }
    }

    /**
     * Returns a StrictControllerMutationQuota for the given user/clientId pair or
     * a UNBOUNDED_CONTROLLER_MUTATION_QUOTA if the quota is disabled.
     *
     * @param session The session from which the user is extracted
     * @param clientId The client id
     * @return ControllerMutationQuota
     */
    public ControllerMutationQuota newStrictQuotaFor(Session session, String clientId) {
        if (quotasEnabled()) {
            ClientSensors clientSensors = getOrCreateQuotaSensors(session, clientId);
            return new StrictControllerMutationQuota(time, clientSensors.quotaSensor());
        } else {
            return ControllerMutationQuota.UNBOUNDED_CONTROLLER_MUTATION_QUOTA;
        }
    }

    public ControllerMutationQuota newStrictQuotaFor(Session session, RequestHeader header) {
        return newStrictQuotaFor(session, header.clientId());
    }

    /**
     * Returns a PermissiveControllerMutationQuota for the given user/clientId pair or
     * a UNBOUNDED_CONTROLLER_MUTATION_QUOTA if the quota is disabled.
     *
     * @param session The session from which the user is extracted
     * @param clientId The client id
     * @return ControllerMutationQuota
     */
    public ControllerMutationQuota newPermissiveQuotaFor(Session session, String clientId) {
        if (quotasEnabled()) {
            ClientSensors clientSensors = getOrCreateQuotaSensors(session, clientId);
            return new PermissiveControllerMutationQuota(time, clientSensors.quotaSensor());
        } else {
            return ControllerMutationQuota.UNBOUNDED_CONTROLLER_MUTATION_QUOTA;
        }
    }

    /**
     * Returns a ControllerMutationQuota based on `strictSinceVersion`. It returns a strict
     * quota if the version is equal to or above of the `strictSinceVersion`, a permissive
     * quota if the version is below, and an unbounded quota if the quota is disabled.
     * When the quota is strictly enforced. Any operation above the quota is not allowed
     * and rejected with a THROTTLING_QUOTA_EXCEEDED error.
     *
     * @param session The session from which the user is extracted
     * @param header The request header to extract the clientId and apiVersion from
     * @param strictSinceVersion The version since quota is strict
     * @return ControllerMutationQuota instance
     */
    public ControllerMutationQuota newQuotaFor(Session session, RequestHeader header, short strictSinceVersion) {
        if (header.apiVersion() >= strictSinceVersion) return newStrictQuotaFor(session, header);
        return newPermissiveQuotaFor(session, header.clientId());
    }

    /**
     * This calculates the amount of time needed to bring the TokenBucket within quota
     * assuming that no new metrics are recorded.
     * Basically, if a value < 0 is observed, the time required to bring it to zero is
     * -value/ refill rate (quota bound) * 1000.
     */
    public static long throttleTimeMs(QuotaViolationException e) {
        if (e.metric().measurable() instanceof TokenBucket) {
            return Math.round(-e.value() / e.bound() * 1000);
        } 
        throw new IllegalArgumentException("Metric " + e.metric().metricName() + 
            " is not a TokenBucket metric, value " + e.metric().measurable());
    }
}