package org.apache.kafka.common.utils;

import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.utils.Time;

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
