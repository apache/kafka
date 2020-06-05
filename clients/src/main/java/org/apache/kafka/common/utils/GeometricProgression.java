package org.apache.kafka.common.utils;

import java.util.concurrent.ThreadLocalRandom;

public class GeometricProgression {
    private final int ratio;
    private final double expMax;
    private final long scaleFactor;
    private final double jitter;

    public GeometricProgression(long scaleFactor, int ratio, long termMax, double jitter) {
        this.scaleFactor = scaleFactor;
        this.ratio = ratio;
        this.jitter = jitter;
        this.expMax = termMax > scaleFactor ?
                Math.log(termMax / (double) Math.max(scaleFactor, 1)) / Math.log(ratio) : 0;
    }

    public long term(long n) {
        if (n == 0 || expMax == 0) {
            System.out.println("term " + n + ": " + scaleFactor);
            return scaleFactor;
        }
        double exp = Math.min(n, this.expMax);
        double term = scaleFactor * Math.pow(ratio, exp);
        double randomFactor = ThreadLocalRandom.current().nextDouble(1 - jitter, 1 + jitter);
        System.out.println("term " + n + ": " + (long) (randomFactor * term));
        return (long) (randomFactor * term);
    }
}
