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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class extends {@link Histogram} but providing its binning strategy by exposing
 * {@link org.apache.kafka.common.metrics.stats.Histogram.BinScheme}
 */

public class HistogramStat extends Histogram {

    private final BinScheme binScheme;
    private final MetricName metricName;
    private final List<Bucket> buckets;

    public HistogramStat(BinScheme binScheme, MetricName metricName) {

        super(binScheme);
        this.binScheme = binScheme;
        this.metricName = metricName;
        this.buckets = initializeHistogramBuckets(metricName, binScheme);
    }

    private static List<LinearHistogram.Bucket> initializeHistogramBuckets(MetricName metricName, BinScheme binScheme) {
        List<LinearHistogram.Bucket> buckets = new ArrayList<>();
        double lowerBound = 0;
        for (int i = 0; i < binScheme.bins(); i++) {
            String name = metricName.name();
            Map<String, String> tags = new HashMap<>(metricName.tags());
            tags.put("lower_bound", String.valueOf(lowerBound));
            tags.put("upper_bound", String.valueOf(binScheme.fromBin(i)));
            lowerBound = binScheme.fromBin(i);
            buckets.add(new LinearHistogram.Bucket(
                    new MetricName(
                            name,
                            metricName.group(),
                            metricName.description(),
                            tags),
                    0)); // initial value as 0
        }

        return buckets;
    }

    public BinScheme getBinScheme() {
        return binScheme;
    }

    public List<Bucket> getBuckets() {
        return buckets;
    }

    public MetricName getMetricName() {
        return metricName;
    }

    static class Bucket {

        private final MetricName name;
        private final double value;

        Bucket(MetricName name, double value) {
            this.name = name;
            this.value = value;
        }

        public MetricName name() {
            return this.name;
        }

        public double value() {
            return this.value;
        }
    }
}
