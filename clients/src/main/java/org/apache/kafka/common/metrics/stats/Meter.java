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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.CompoundStat;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.stats.Rate.SampledTotal;


/**
 * A compound stat that includes a rate metric and a cumulative total metric.
 */
public class Meter implements CompoundStat {

    private final MetricName rateMetricName;
    private final MetricName totalMetricName;
    private final Rate rate;
    private final Total total;

    public Meter(MetricName rateMetricName, MetricName totalMetricName) {
        this(TimeUnit.SECONDS, new SampledTotal(), rateMetricName, totalMetricName);
    }

    public Meter(TimeUnit unit, MetricName rateMetricName, MetricName totalMetricName) {
        this(unit, new SampledTotal(), rateMetricName, totalMetricName);
    }

    public Meter(SampledStat total, MetricName rateMetricName, MetricName totalMetricName) {
        this(TimeUnit.SECONDS, total, rateMetricName, totalMetricName);
    }

    public Meter(TimeUnit unit, SampledStat sampledStat, MetricName rateMetricName, MetricName totalMetricName) {
        this.total = new Total();
        this.rate = new Rate(unit, sampledStat) {
            @Override
            public void record(MetricConfig config, double value, long now) {
                super.record(config, value, now);
                total.record(config, value, now);
            }
        };
        this.rateMetricName = rateMetricName;
        this.totalMetricName = totalMetricName;
    }

    @Override
    public List<NamedMeasurable> stats() {
        List<NamedMeasurable> stats = new ArrayList<NamedMeasurable>(2);
        stats.add(new NamedMeasurable(totalMetricName, total));
        stats.add(new NamedMeasurable(rateMetricName, rate));
        return stats;
    }

    @Override
    public void record(MetricConfig config, double value, long timeMs) {
        rate.record(config, value, timeMs);
    }
}
