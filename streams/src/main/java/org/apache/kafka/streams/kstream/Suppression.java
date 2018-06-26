package org.apache.kafka.streams.kstream;

import java.time.Duration;

public class Suppression {
    private Duration latenessBound = Duration.ofMillis(Long.MAX_VALUE);
    private IntermediateSuppression intermediateSuppression = new IntermediateSuppression();

    public enum BufferFullStrategy {
        EMIT,
        SPILL_TO_DISK,
        SHUT_DOWN
    }

    public static class IntermediateSuppression {
        private Duration timeToWaitForMoreEvents = Duration.ZERO;
        private long numberOfKeysToRemember = Long.MAX_VALUE;
        private long bytesToUseForSuppressionStorage = Long.MAX_VALUE;
        private BufferFullStrategy bufferFullStrategy = BufferFullStrategy.EMIT;

        public IntermediateSuppression() {}

        public IntermediateSuppression emitAfter(final Duration timeToWaitForMoreEvents) {
            this.timeToWaitForMoreEvents = timeToWaitForMoreEvents;
            return this;
        }

        public IntermediateSuppression bufferKeys(final long numberOfKeysToRemember) {
            this.numberOfKeysToRemember = numberOfKeysToRemember;
            return this;
        }

        public IntermediateSuppression bufferBytes(final long bytesToUseForSuppressionStorage) {
            this.bytesToUseForSuppressionStorage = bytesToUseForSuppressionStorage;
            return this;
        }

        public IntermediateSuppression bufferFullStrategy(final BufferFullStrategy bufferFullStrategy) {
            this.bufferFullStrategy = bufferFullStrategy;
            return this;
        }

        public Duration getTimeToWaitForMoreEvents() {
            return timeToWaitForMoreEvents;
        }

        public long getNumberOfKeysToRemember() {
            return numberOfKeysToRemember;
        }

        public long getBytesToUseForSuppressionStorage() {
            return bytesToUseForSuppressionStorage;
        }

        public BufferFullStrategy getBufferFullStrategy() {
            return bufferFullStrategy;
        }
    }

    public Suppression() {}

    public Suppression suppressLateEvents(final Duration maxAllowedLateness) {
        this.latenessBound = maxAllowedLateness;
        return this;
    }

    public Suppression suppressIntermediateEvents(final IntermediateSuppression intermediateSuppression) {
        this.intermediateSuppression = intermediateSuppression;
        return this;
    }

    public Duration getLatenessBound() {
        return latenessBound;
    }

    public IntermediateSuppression getIntermediateSuppression() {
        return intermediateSuppression;
    }
}
