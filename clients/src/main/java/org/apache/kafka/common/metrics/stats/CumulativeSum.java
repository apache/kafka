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
package org.apache.kafka.common.metrics.stats;

import org.apache.kafka.common.metrics.MeasurableStat;
import org.apache.kafka.common.metrics.MetricConfig;

/**
 * An non-sampled cumulative total maintained over all time.
 * This is a non-sampled version of {@link WindowedSum}.
 *
 * See also {@link CumulativeCount} if you just want to increment the value by 1 on each recording.
 */
public class CumulativeSum implements MeasurableStat {

    private double total;

    public CumulativeSum() {
        total = 0.0;
    }

    public CumulativeSum(double value) {
        total = value;
    }

    @Override
    public void record(MetricConfig config, double value, long now) {
        total += value;
    }

    @Override
    public double measure(MetricConfig config, long now) {
        return total;
    }

    @Override
    public String toString() {
        return "CumulativeSum(total=" + total + ")";
    }
}
