package kafka.common.utils;

/**
 * A time implementation that uses the system clock and sleep call
 */
public class SystemTime implements Time {

    @Override
    public long milliseconds() {
        return System.currentTimeMillis();
    }

    public long nanoseconds() {
        return System.nanoTime();
    }

    @Override
    public void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            // no stress
        }
    }

}
