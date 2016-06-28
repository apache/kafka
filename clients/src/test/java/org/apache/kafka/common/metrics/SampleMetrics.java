/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.common.metrics;

import java.util.List;

import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.utils.Time;

/**
 * A registry of predefined Metrics for the SpecificMetricsTest.java class.
 */
public class SampleMetrics extends SpecificMetrics {


    public SampleMetrics(MetricConfig defaultConfig, List<MetricsReporter> reporters, Time time,
            boolean enableExpiration) {
        super(defaultConfig, reporters, time, enableExpiration);
    }

    public static final MetricNameTemplate METRIC1 = new MetricNameTemplate("name", "group", "The first metric used in testMetricName()", "key1", "key2");
    public static final MetricNameTemplate METRIC2 = new MetricNameTemplate("name", "group", "The second metric used in testMetricName()", "key1", "key2");
    public static final MetricNameTemplate DIRECT_MEASUREABLE = new MetricNameTemplate("direct.measurable", "grp1", "The fraction of time an appender waits for space allocation.");

    public static final MetricNameTemplate SIMPLE_STATS_AVG = new MetricNameTemplate("test.avg", "grp1", "Average of the metric in testSimpleStats");
    public static final MetricNameTemplate SIMPLE_STATS_MAX = new MetricNameTemplate("test.max", "grp1", "Max of the metric in testSimpleStats");
    public static final MetricNameTemplate SIMPLE_STATS_MIN = new MetricNameTemplate("test.min", "grp1", "Min of the metric in testSimpleStats");
    public static final MetricNameTemplate SIMPLE_STATS_RATE = new MetricNameTemplate("test.rate", "grp1", "Rate of the metric in testSimpleStats");
    public static final MetricNameTemplate SIMPLE_STATS_OCCURENCES = new MetricNameTemplate("test.occurences", "grp1", "Rate of occurences of the metric in testSimpleStats");
    public static final MetricNameTemplate SIMPLE_STATS_COUNT = new MetricNameTemplate("test.count", "grp1", "Count of the metric in testSimpleStats");
    public static final MetricNameTemplate SIMPLE_STATS_MEDIAN = new MetricNameTemplate("test.median", "grp1", "Median of the metric in testSimpleStats");
    public static final MetricNameTemplate SIMPLE_STATS_PERCENT = new MetricNameTemplate("test.percent", "grp1", "Percent of the metric in testSimpleStats");
    public static final MetricNameTemplate SIMPLE_STATS_TOTAL = new MetricNameTemplate("s2.total", "grp1", "Total of the metric in testSimpleStats");

    public static final MetricNameTemplate HIERARCHICAL_SENSORS_PARENT1_COUNT = new MetricNameTemplate("test.parent1.count", "grp1", "parent1 in testHierarchicalSensors");
    public static final MetricNameTemplate HIERARCHICAL_SENSORS_PARENT2_COUNT = new MetricNameTemplate("test.parent2.count", "grp1", "parent2 in testHierarchicalSensors");
    public static final MetricNameTemplate HIERARCHICAL_SENSORS_CHILD1_COUNT = new MetricNameTemplate("test.child1.count", "grp1", "child1 in testHierarchicalSensors");
    public static final MetricNameTemplate HIERARCHICAL_SENSORS_CHILD2_COUNT = new MetricNameTemplate("test.child2.count", "grp1", "child2 in testHierarchicalSensors");
    public static final MetricNameTemplate HIERARCHICAL_SENSORS_GRANDCHILD_COUNT = new MetricNameTemplate("test.grandchild.count", "grp1", "grandchild in testHierarchicalSensors");

    
    private static final MetricNameTemplate[] ALL_METRICS = {
        METRIC1,
        METRIC2,
        DIRECT_MEASUREABLE,
        SIMPLE_STATS_AVG,
        SIMPLE_STATS_MAX,
        SIMPLE_STATS_MIN,
        SIMPLE_STATS_RATE,
        SIMPLE_STATS_OCCURENCES,
        SIMPLE_STATS_COUNT,
        SIMPLE_STATS_MEDIAN,
        SIMPLE_STATS_PERCENT,
        SIMPLE_STATS_TOTAL,
        HIERARCHICAL_SENSORS_PARENT1_COUNT,
        HIERARCHICAL_SENSORS_PARENT2_COUNT,
        HIERARCHICAL_SENSORS_CHILD1_COUNT,
        HIERARCHICAL_SENSORS_CHILD2_COUNT,
        HIERARCHICAL_SENSORS_GRANDCHILD_COUNT
    };

    public static void main(String[] args) {
        System.out.println(SpecificMetrics.toHtmlTable("sample.domain", ALL_METRICS));
        
    }
}
