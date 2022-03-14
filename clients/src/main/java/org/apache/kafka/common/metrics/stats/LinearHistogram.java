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

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.CompoundStat;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;

import java.util.ArrayList;
import java.util.List;

/**
 * A Histogram class of CompoundStat class using Linear BinScheme. See {@link Histogram} for the histogram implementation
 */
public class LinearHistogram extends HistogramStat implements CompoundStat {
    public LinearHistogram(int numBins, int maxBin, MetricName metricName) {
        super(new LinearBinScheme(numBins, maxBin), metricName);
    }

    @Override
    public List<NamedMeasurable> stats() {
        List<NamedMeasurable> ms = new ArrayList<>();
        for (Bucket bucket: getBuckets()) {
            ms.add(new NamedMeasurable(bucket.name(), new Measurable() {
                @Override
                public double measure(MetricConfig config, long now) {
                    return bucket.value();
                }
            }));
        }
        return ms;
    }

    @Override
    public void record(MetricConfig config, double value, long timeMs) {
        super.record(value);
    }
}
