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

package kafka.metrics;

import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.reporting.JmxReporter;

import java.util.function.Predicate;

public class FilteringJmxReporter extends JmxReporter {

    private volatile Predicate<MetricName> metricPredicate;

    public FilteringJmxReporter(MetricsRegistry registry, Predicate<MetricName> metricPredicate) {
        super(registry);
        this.metricPredicate = metricPredicate;
    }

    @Override
    public void onMetricAdded(MetricName name, Metric metric) {
        if (metricPredicate.test(name)) {
            super.onMetricAdded(name, metric);
        }
    }

    @Override
    public void onMetricRemoved(MetricName name) {
        super.onMetricRemoved(name);
    }

    public void updatePredicate(Predicate<MetricName> predicate) {
        this.metricPredicate = predicate;
        // re-register metrics on update
        getMetricsRegistry()
            .allMetrics()
            .forEach((name, metric) -> {
                if (metricPredicate.test(name)) {
                    super.onMetricAdded(name, metric);
                } else {
                    super.onMetricRemoved(name);
                }
            }
            );
    }
}
