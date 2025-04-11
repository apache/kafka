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
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.QuotaViolationException;
import org.apache.kafka.common.metrics.stats.Rate;

/**
 * Helper functions related to quotas
 */
public class QuotaUtils {

    /**
     * This calculates the amount of time needed to bring the observed rate within quota
     * assuming that no new metrics are recorded.
     * <br/>
     * If O is the observed rate and T is the target rate over a window of W, to bring O down to T,
     * we need to add a delay of X to W such that O * W / (W + X) = T.
     * Solving for X, we get X = (O - T)/T * W.
     *
     * @param timeMs current time in milliseconds
     * @return Delay in milliseconds
     */
    public static long throttleTime(QuotaViolationException e, long timeMs) {
        double difference = e.value() - e.bound();
        // Use the precise window used by the rate calculation
        double throttleTimeMs = difference / e.bound() * windowSize(e.metric(), timeMs);
        return Math.round(throttleTimeMs);
    }

    /**
     * Calculates the amount of time needed to bring the observed rate within quota using the same algorithm as
     * throttleTime() utility method but the returned value is capped to given maxThrottleTime
     */
    public static long boundedThrottleTime(QuotaViolationException e, long maxThrottleTime, long timeMs) {
        return Math.min(throttleTime(e, timeMs), maxThrottleTime);
    }

    /**
     * Returns window size of the given metric
     *
     * @param metric metric with measurable of type Rate
     * @param timeMs current time in milliseconds
     * @throws IllegalArgumentException if given measurable is not Rate
     */
    private static long windowSize(KafkaMetric metric, long timeMs) {
        return measurableAsRate(metric.metricName(), metric.measurable()).windowSize(metric.config(), timeMs);
    }

    /**
     * Casts provided Measurable to Rate
     * @throws IllegalArgumentException if given measurable is not Rate
     */
    private static Rate measurableAsRate(MetricName name, Measurable measurable) {
        if (measurable instanceof Rate) {
            return (Rate) measurable;
        } else {
            throw new IllegalArgumentException("Metric " + name + " is not a Rate metric, value " + measurable);
        }
    }

}
