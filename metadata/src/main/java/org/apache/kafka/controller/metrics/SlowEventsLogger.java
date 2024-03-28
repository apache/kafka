package org.apache.kafka.controller.metrics;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.slf4j.Logger;

import java.util.function.Supplier;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class SlowEventsLogger {
    /**
     * Don't report any p99 events below this threshold. This prevents the controller from reporting p99 event
     * times in the idle case.
     */
    private static final int MIN_SLOW_EVENT_TIME_MS = 100;

    /**
     * Calculating the p99 from the histogram consumes some resources, so only update it every so often.
     */
    private static final int P99_REFRESH_INTERVAL_MS = 30000;

    private final Supplier<Double> p99Supplier;
    private final Logger log;
    private final Time time;
    private double p99;
    private Timer percentileUpdateTimer;

    public SlowEventsLogger(
        Supplier<Double> p99Supplier,
        Logger log,
        Time time
    ) {
        this.p99Supplier = p99Supplier;
        this.log = log;
        this.time = time;
        this.percentileUpdateTimer = time.timer(P99_REFRESH_INTERVAL_MS);
    }

    public void maybeLog(String name, long durationNs) {
        long durationMs = MILLISECONDS.convert(durationNs, NANOSECONDS);
        if (durationMs > MIN_SLOW_EVENT_TIME_MS && durationMs > p99) {
            log.info("Slow controller event {} processed in {} us", name,
                MICROSECONDS.convert(durationNs, NANOSECONDS));
        }
    }

    public void maybeRefreshPercentile() {
        percentileUpdateTimer.update();
        if (percentileUpdateTimer.isExpired()) {
            p99 = p99Supplier.get();
            log.trace("Update slow events p99 to {}", p99);
            percentileUpdateTimer.reset(P99_REFRESH_INTERVAL_MS);
        }
    }
}
