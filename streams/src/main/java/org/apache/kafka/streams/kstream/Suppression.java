package org.apache.kafka.streams.kstream;

import java.time.Duration;

@SuppressWarnings("WeakerAccess")
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

        private IntermediateSuppression() {}

        private IntermediateSuppression(final IntermediateSuppression from) {
            timeToWaitForMoreEvents = from.timeToWaitForMoreEvents;
            numberOfKeysToRemember = from.numberOfKeysToRemember;
            bytesToUseForSuppressionStorage = from.bytesToUseForSuppressionStorage;
            bufferFullStrategy = from.bufferFullStrategy;
        }

        public static IntermediateSuppression withEmitAfter(final Duration timeToWaitForMoreEvents) {
            return new IntermediateSuppression().emitAfter(timeToWaitForMoreEvents);
        }

        public IntermediateSuppression emitAfter(final Duration timeToWaitForMoreEvents) {
            final IntermediateSuppression result = new IntermediateSuppression(this);
            result.timeToWaitForMoreEvents = timeToWaitForMoreEvents;
            return result;
        }

        public static IntermediateSuppression withBufferKeys(final long numberOfKeysToRemember) {
            return new IntermediateSuppression().bufferKeys(numberOfKeysToRemember);
        }

        public IntermediateSuppression bufferKeys(final long numberOfKeysToRemember) {
            final IntermediateSuppression result = new IntermediateSuppression(this);
            result.numberOfKeysToRemember = numberOfKeysToRemember;
            return result;
        }

        public static IntermediateSuppression withBufferBytes(final long bytesToUseForSuppressionStorage) {
            return new IntermediateSuppression().bufferBytes(bytesToUseForSuppressionStorage);
        }

        public IntermediateSuppression bufferBytes(final long bytesToUseForSuppressionStorage) {
            final IntermediateSuppression result = new IntermediateSuppression(this);
            result.bytesToUseForSuppressionStorage = bytesToUseForSuppressionStorage;
            return result;
        }

        public static IntermediateSuppression withBufferFullStrategy(final BufferFullStrategy bufferFullStrategy) {
            return new IntermediateSuppression().bufferFullStrategy(bufferFullStrategy);
        }

        public IntermediateSuppression bufferFullStrategy(final BufferFullStrategy bufferFullStrategy) {
            final IntermediateSuppression result = new IntermediateSuppression(this);
            result.bufferFullStrategy = bufferFullStrategy;
            return result;
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
            .<K, V>withSuppressedLateEvents(maxAllowedLateness)
            .suppressIntermediateEvents(IntermediateSuppression
                .withEmitAfter(maxAllowedLateness)
                .bufferFullStrategy(bufferFullStrategy)
            );
    }

    public static <K, V> Suppression<K, V> withSuppressedLateEvents(final Duration maxAllowedLateness) {
        return new Suppression<K, V>().suppressLateEvents(maxAllowedLateness);
    }

    public Suppression<K, V> suppressLateEvents(final Duration maxAllowedLateness) {
        final Suppression<K, V> result = new Suppression<>();
        result.latenessBound = maxAllowedLateness;
        result.intermediateSuppression = this.intermediateSuppression;
        return result;
    }

    public static <K, V> Suppression<K, V> withSuppressedIntermediateEvents(final IntermediateSuppression intermediateSuppression) {
        return new Suppression<K, V>().suppressIntermediateEvents(intermediateSuppression);
    }

    public Suppression<K, V> suppressIntermediateEvents(final IntermediateSuppression intermediateSuppression) {
        final Suppression<K, V> result = new Suppression<>();
        result.latenessBound = this.latenessBound;
        result.intermediateSuppression = intermediateSuppression;
        return result;
    }

    public Duration getLatenessBound() {
        return latenessBound;
    }

    public IntermediateSuppression getIntermediateSuppression() {
        return intermediateSuppression;
    }
}
