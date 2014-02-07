package org.apache.kafka.common.metrics.stats;

public class Percentile {

    private final String name;
    private final String description;
    private final double percentile;

    public Percentile(String name, double percentile) {
        this(name, "", percentile);
    }

    public Percentile(String name, String description, double percentile) {
        super();
        this.name = name;
        this.description = description;
        this.percentile = percentile;
    }

    public String name() {
        return this.name;
    }

    public String description() {
        return this.description;
    }

    public double percentile() {
        return this.percentile;
    }

}
