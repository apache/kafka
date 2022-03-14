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
package org.apache.kafka.clients.telemetry;

import java.util.Collection;
import java.util.StringJoiner;
import java.util.function.Predicate;

public interface MetricSelector extends Predicate<TelemetryMetric> {

    MetricSelector NONE = new MetricSelector() {
        @Override
        public boolean test(TelemetryMetric metric) {
            return false;
        }

        @Override
        public String toString() {
            return String.format("%s.NONE", MetricSelector.class.getSimpleName());
        }

    };

    MetricSelector ALL = new MetricSelector() {

        @Override
        public boolean test(TelemetryMetric metric) {
            return true;
        }

        @Override
        public String toString() {
            return String.format("%s.ALL", MetricSelector.class.getSimpleName());
        }

    };

    class FilteredMetricSelector implements MetricSelector {

        private final Collection<String> filters;

        public FilteredMetricSelector(Collection<String> filters) {
            this.filters = filters;
        }

        @Override
        public boolean test(TelemetryMetric metric) {
            return filters.stream().anyMatch(f -> metric.metricName().name().startsWith(f));
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", FilteredMetricSelector.class.getSimpleName() + "[", "]")
                .add("filters=" + filters)
                .toString();
        }

    }

}
