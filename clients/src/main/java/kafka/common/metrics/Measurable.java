package kafka.common.metrics;

/**
 * A measurable quantity that can be registered as a metric
 */
public interface Measurable {

    /**
     * Measure this quantity and return the result as a double
     * @param config The configuration for this metric
     * @param now The time the measurement is being taken
     * @return The measured value
     */
    public double measure(MetricConfig config, long now);

}
