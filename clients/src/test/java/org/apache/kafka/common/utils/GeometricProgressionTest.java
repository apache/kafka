package org.apache.kafka.common.utils;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GeometricProgressionTest {
    @Test
    public void testGeometricProgression() {
        long scaleFactor = 100;
        int ratio = 2;
        long termMax = 2000;
        double jitter = 0.2;
        GeometricProgression geometricProgression = new GeometricProgression(
                scaleFactor, ratio, termMax, jitter
        );
        assertEquals(scaleFactor, geometricProgression.term(0));
        for (int i = 0; i <= 100; i++) {
            for (int n = 0; n <= 4; n++) {
                assertEquals(scaleFactor * Math.pow(ratio, n), geometricProgression.term(n),
                        scaleFactor * Math.pow(ratio, n) * jitter);
            }
            System.out.println(geometricProgression.term(5));
            assertTrue(geometricProgression.term(1000) <= termMax * (1 + jitter));
        }
    }
}
