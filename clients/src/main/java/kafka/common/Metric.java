package kafka.common;

/**
 * A numerical metric tracked for monitoring purposes
 */
public interface Metric {

    /**
     * A unique name for this metric
     */
    public String name();

    /**
     * A description of what is measured...this will be "" if no description was given
     */
    public String description();

    /**
     * The value of the metric
     */
    public double value();

}
