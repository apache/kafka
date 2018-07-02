package org.apache.kafka.streams.kstream;

import java.time.Duration;

public class Suppression<K, V> {
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

        public static IntermediateSuppression emitAfter(final Duration timeToWaitForMoreEvents) {
            return new IntermediateSuppression().withEmitAfter(timeToWaitForMoreEvents);
        }

        public IntermediateSuppression withEmitAfter(final Duration timeToWaitForMoreEvents) {
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

    public static <K extends Windowed, V> Suppression<K, V> finalResultsOnly(final Duration maxAllowedLateness, final BufferFullStrategy bufferFullStrategy) {
        return Suppression
            .<K, V>suppressLateEvents(maxAllowedLateness)
            .suppressIntermediateEvents(new IntermediateSuppression()
                .emitAfter(maxAllowedLateness)
                .bufferFullStrategy(bufferFullStrategy)
            );
    }

    public static <K, V> Suppression<K, V> suppressLateEvents(final Duration maxAllowedLateness) {
        final Suppression<K, V> kvSuppression = new Suppression<>();
        kvSuppression.latenessBound = maxAllowedLateness;
        return kvSuppression;
    }

    public static <K, V> Suppression<K, V> suppressIntermediateEvents(final IntermediateSuppression intermediateSuppression) {
        final Suppression<K, V> suppression = new Suppression<>();
        suppression.intermediateSuppression = intermediateSuppression;
        return suppression;
    }

    public Duration getLatenessBound() {
        return latenessBound;
    }

    public IntermediateSuppression getIntermediateSuppression() {
        return intermediateSuppression;
    }
}
