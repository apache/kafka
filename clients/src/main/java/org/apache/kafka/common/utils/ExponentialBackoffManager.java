package org.apache.kafka.common.utils;

import org.apache.kafka.clients.CommonClientConfigs;

/**
 * Manages retry attempts and exponential backoff for requests.
 */
public class ExponentialBackoffManager {
    private final int maxAttempts;
    private int attempts;
    private final ExponentialBackoff backoff;

    public ExponentialBackoffManager(int maxAttempts, long initialBackoffMs, long maxBackoffMs) {
        this.maxAttempts = maxAttempts;
        this.backoff = new ExponentialBackoff(
            initialBackoffMs,
            CommonClientConfigs.RETRY_BACKOFF_EXP_BASE,
            maxBackoffMs,
            CommonClientConfigs.RETRY_BACKOFF_JITTER
        );
    }

    public void incrementAttempt() {
        attempts++;
    }

    public void resetAttempts() {
        attempts = 0;
    }

    public boolean canAttempt() {
        return attempts < maxAttempts;
    }

    public long backOff() {
        return this.backoff.backoff(attempts);
    }

    public int attempts() {
        return attempts;
    }
}