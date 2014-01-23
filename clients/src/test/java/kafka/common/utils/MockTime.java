package kafka.common.utils;

import java.util.concurrent.TimeUnit;

public class MockTime implements Time {

    private long nanos = 0;

    public MockTime() {
        this.nanos = System.nanoTime();
    }

    @Override
    public long milliseconds() {
        return TimeUnit.MILLISECONDS.convert(this.nanos, TimeUnit.NANOSECONDS);
    }

    @Override
    public long nanoseconds() {
        return nanos;
    }

    @Override
    public void sleep(long ms) {
        this.nanos += TimeUnit.NANOSECONDS.convert(ms, TimeUnit.MILLISECONDS);
    }

}
