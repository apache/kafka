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
package org.apache.kafka.common.telemetry.internals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;

public class TestEmitter implements MetricsEmitter {

    private final List<SinglePointMetric> emittedMetrics;
    private Predicate<? super MetricKeyable> metricsPredicate = metricKeyable -> true;
    private boolean onlyDeltaMetrics;

    public TestEmitter() {
        this(false);
    }

    public TestEmitter(boolean onlyDeltaMetrics) {
        this.emittedMetrics = new ArrayList<>();
        this.onlyDeltaMetrics = onlyDeltaMetrics;
    }

    @Override
    public boolean shouldEmitMetric(MetricKeyable metricKeyable) {
        return metricsPredicate.test(metricKeyable);
    }

    @Override
    public boolean shouldEmitDeltaMetrics() {
        return onlyDeltaMetrics;
    }

    @Override
    public boolean emitMetric(SinglePointMetric metric) {
        return emittedMetrics.add(metric);
    }

    @Override
    public List<SinglePointMetric> emittedMetrics() {
        return Collections.unmodifiableList(emittedMetrics);
    }

    public void reset() {
        this.emittedMetrics.clear();
    }

    public void onlyDeltaMetrics(boolean onlyDeltaMetrics) {
        this.onlyDeltaMetrics = onlyDeltaMetrics;
    }

    public void reconfigurePredicate(Predicate<? super MetricKeyable> metricsPredicate) {
        this.metricsPredicate = metricsPredicate;
    }
}
