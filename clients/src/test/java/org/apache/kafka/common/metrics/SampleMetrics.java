package org.apache.kafka.common.metrics;

import java.util.List;

import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.utils.Time;

/**
 * @author jcheng
 *
 * A registry of predefined Metrics for the SpecificMetricsTest.java class.
 * 
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
                SIMPLE_STATS_TOTAL
    };

    public static void main(String[] args) {
        System.out.println(SpecificMetrics.toHtmlTable("sample.domain", ALL_METRICS));
        
    }
}
