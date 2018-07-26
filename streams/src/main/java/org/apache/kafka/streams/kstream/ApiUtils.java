package org.apache.kafka.streams.kstream;

import java.time.Duration;

public final class ApiUtils {
    private ApiUtils() {}

    public static Duration validateMillisecondDuration(final Duration duration, final String message) {
        try {
            //noinspection ResultOfMethodCallIgnored
            duration.toMillis();
            return duration;
        } catch (final ArithmeticException e) {
            throw new IllegalArgumentException(message, e);
        }
    }
}
