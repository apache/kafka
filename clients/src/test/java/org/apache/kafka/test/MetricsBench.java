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
package org.apache.kafka.test;

import java.util.Arrays;

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Percentile;
import org.apache.kafka.common.metrics.stats.Percentiles;
import org.apache.kafka.common.metrics.stats.Percentiles.BucketSizing;
import org.apache.kafka.common.metrics.stats.WindowedCount;

public class MetricsBench {

    public static void main(String[] args) {
        long iters = Long.parseLong(args[0]);
        Metrics metrics = new Metrics();
        try {
            Sensor parent = metrics.sensor("parent");
            Sensor child = metrics.sensor("child", parent);
            for (Sensor sensor : Arrays.asList(parent, child)) {
                sensor.add(metrics.metricName(sensor.name() + ".avg", "grp1"), new Avg());
                sensor.add(metrics.metricName(sensor.name() + ".count", "grp1"), new WindowedCount());
                sensor.add(metrics.metricName(sensor.name() + ".max", "grp1"), new Max());
                sensor.add(new Percentiles(1024,
                        0.0,
                        iters,
                        BucketSizing.CONSTANT,
                        new Percentile(metrics.metricName(sensor.name() + ".median", "grp1"), 50.0),
                        new Percentile(metrics.metricName(sensor.name() +  ".p_99", "grp1"), 99.0)));
            }
            long start = System.nanoTime();
            for (int i = 0; i < iters; i++)
                parent.record(i);
            double elapsed = (System.nanoTime() - start) / (double) iters;
            System.out.printf("%.2f ns per metric recording.%n", elapsed);
        } finally {
            metrics.close();
        }
    }
}
