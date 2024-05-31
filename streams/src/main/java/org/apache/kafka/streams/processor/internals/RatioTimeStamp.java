package org.apache.kafka.streams.processor.internals;

class RatioTimeStamp {
    private final double ratio;
    private final long timestamp;

    public RatioTimeStamp(double ratio, long timestamp) {
        this.ratio = ratio;
        this.timestamp = timestamp;
    }

    public double getRatio() {
        return ratio;
    }

    public long getTimestamp() {
        return timestamp;
    }
}
