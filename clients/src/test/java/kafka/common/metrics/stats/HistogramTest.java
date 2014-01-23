package kafka.common.metrics.stats;

import static org.junit.Assert.assertEquals;
import kafka.common.metrics.stats.Histogram.BinScheme;
import kafka.common.metrics.stats.Histogram.ConstantBinScheme;
import kafka.common.metrics.stats.Histogram.LinearBinScheme;

import org.junit.Test;

public class HistogramTest {

    private static final double EPS = 0.0000001d;

    // @Test
    public void testHistogram() {
        BinScheme scheme = new ConstantBinScheme(12, -5, 5);
        Histogram hist = new Histogram(scheme);
        for (int i = -5; i < 5; i++)
            hist.record(i);
        for (int i = 0; i < 10; i++)
            assertEquals(scheme.fromBin(i + 1), hist.value(i / 10.0), EPS);
    }

    @Test
    public void testConstantBinScheme() {
        ConstantBinScheme scheme = new ConstantBinScheme(5, -5, 5);
        assertEquals("A value below the lower bound should map to the first bin", 0, scheme.toBin(-5.01));
        assertEquals("A value above the upper bound should map to the last bin", 4, scheme.toBin(5.01));
        assertEquals("Check boundary of bucket 1", 1, scheme.toBin(-5));
        assertEquals("Check boundary of bucket 4", 4, scheme.toBin(5));
        assertEquals("Check boundary of bucket 3", 3, scheme.toBin(4.9999));
        checkBinningConsistency(new ConstantBinScheme(4, 0, 5));
        checkBinningConsistency(scheme);
    }

    public void testLinearBinScheme() {
        LinearBinScheme scheme = new LinearBinScheme(5, 5);
        for (int i = 0; i < scheme.bins(); i++)
            System.out.println(i + " " + scheme.fromBin(i));
        checkBinningConsistency(scheme);
    }

    private void checkBinningConsistency(BinScheme scheme) {
        for (int bin = 0; bin < scheme.bins(); bin++) {
            double fromBin = scheme.fromBin(bin);
            int binAgain = scheme.toBin(fromBin);
            assertEquals("unbinning and rebinning " + bin
                         + " gave a different result ("
                         + fromBin
                         + " was placed in bin "
                         + binAgain
                         + " )", bin, binAgain);
        }
    }

}
