package kafka.common.metrics;

/**
 * An upper or lower bound for metrics
 */
public final class Quota {

    private final boolean upper;
    private final double bound;

    public Quota(double bound, boolean upper) {
        this.bound = bound;
        this.upper = upper;
    }

    public static Quota lessThan(double upperBound) {
        return new Quota(upperBound, true);
    }

    public static Quota moreThan(double lowerBound) {
        return new Quota(lowerBound, false);
    }

    public boolean isUpperBound() {
        return this.upper;
    }

    public double bound() {
        return this.bound;
    }

    public boolean acceptable(double value) {
        return (upper && value <= bound) || (!upper && value >= bound);
    }

}
