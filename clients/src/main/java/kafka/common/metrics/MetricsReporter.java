package kafka.common.metrics;

import java.util.List;

/**
 * A plugin interface to allow things to listen as new metrics are created so they can be reported
 */
public interface MetricsReporter {

    /**
     * This is called when the reporter is first registered to initially register all existing metrics
     * @param metrics All currently existing metrics
     */
    public void init(List<KafkaMetric> metrics);

    /**
     * This is called whenever a metric is updated or added
     * @param metric
     */
    public void metricChange(KafkaMetric metric);

    /**
     * Called when the metrics repository is closed.
     */
    public void close();

}
