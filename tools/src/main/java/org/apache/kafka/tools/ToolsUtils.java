package org.apache.kafka.tools;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;

import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;

public class ToolsUtils {

    /**
     * print out the metrics in alphabetical order
     * @param metrics   the metrics to be printed out
     */
    public static void printMetrics(Map<MetricName, ? extends Metric> metrics) {
        if (metrics != null && !metrics.isEmpty()) {
            int maxLengthOfDisplayName = 0;
            TreeMap<String, Double> sortedMetrics = new TreeMap<>(new Comparator<String>() {
                @Override
                public int compare(String o1, String o2) {
                    return o1.compareTo(o2);
                }
            });
            for (Metric metric : metrics.values()) {
                MetricName mName = metric.metricName();
                String mergedName = mName.group() + ":" + mName.name() + ":" + mName.tags();
                maxLengthOfDisplayName = maxLengthOfDisplayName < mergedName.length() ? mergedName.length() : maxLengthOfDisplayName;
                sortedMetrics.put(mergedName, metric.value());
            }
            String outputFormat = "%-" + maxLengthOfDisplayName + "s : %.3f";
            System.out.println(String.format("\n%-" + maxLengthOfDisplayName + "s   %s", "Metric Name", "Value"));

            for (String key : sortedMetrics.keySet()) {
                System.out.println(String.format(outputFormat, key, sortedMetrics.get(key)));
            }
        }
    }
}
