package kafka.common.metrics;

/**
 * A Stat is a quanity such as average, max, etc that is computed off the stream of updates to a sensor
 */
public interface Stat {

    /**
     * Record the given value
     * @param config The configuration to use for this metric
     * @param value The value to record
     * @param time The time this value occurred
     */
    public void record(MetricConfig config, double value, long time);

}
