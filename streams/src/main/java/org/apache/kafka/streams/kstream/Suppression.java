package org.apache.kafka.streams.kstream;

import org.apache.kafka.common.serialization.Serializer;

import java.time.Duration;

@SuppressWarnings({"WeakerAccess", "unused"})
public class Suppression<K, V> {
    private Duration latenessBound = Duration.ofMillis(Long.MAX_VALUE);
    private IntermediateSuppression<K, V> intermediateSuppression = new IntermediateSuppression<>();

    public enum BufferFullStrategy {
        EMIT,
        SPILL_TO_DISK,
        SHUT_DOWN
    }

    public static class IntermediateSuppression<K, V> {
        private Duration timeToWaitForMoreEvents = null;
        private long numberOfKeysToRemember = Long.MAX_VALUE;
        private long bytesToUseForSuppressionStorage = Long.MAX_VALUE;
        private BufferFullStrategy bufferFullStrategy = BufferFullStrategy.EMIT;
        private Serializer<K> keySerializer;
        private Serializer<V> valueSerializer;

        private IntermediateSuppression() {}

        private IntermediateSuppression(final IntermediateSuppression<K, V> from) {
            timeToWaitForMoreEvents = from.timeToWaitForMoreEvents;
            numberOfKeysToRemember = from.numberOfKeysToRemember;
            bytesToUseForSuppressionStorage = from.bytesToUseForSuppressionStorage;
            bufferFullStrategy = from.bufferFullStrategy;
            keySerializer = from.keySerializer;
            valueSerializer = from.valueSerializer;
        }

        public static <K, V> IntermediateSuppression<K, V> withEmitAfter(final Duration timeToWaitForMoreEvents) {
            return new IntermediateSuppression<K, V>().emitAfter(timeToWaitForMoreEvents);
        }

        public IntermediateSuppression<K, V> emitAfter(final Duration timeToWaitForMoreEvents) {
            final IntermediateSuppression<K, V> result = new IntermediateSuppression<>(this);
            result.timeToWaitForMoreEvents = timeToWaitForMoreEvents;
            return result;
        }

        public static <K, V> IntermediateSuppression<K, V> withBufferKeys(final long numberOfKeysToRemember) {
            return new IntermediateSuppression<K, V>().bufferKeys(numberOfKeysToRemember);
        }

        public IntermediateSuppression<K, V> bufferKeys(final long numberOfKeysToRemember) {
            final IntermediateSuppression<K, V> result = new IntermediateSuppression<>(this);
            result.numberOfKeysToRemember = numberOfKeysToRemember;
            return result;
        }

        public static <K, V> IntermediateSuppression<K, V> withBufferBytes(final long bytesToUseForSuppressionStorage, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) {
            return new IntermediateSuppression<K, V>().bufferBytes(bytesToUseForSuppressionStorage, keySerializer, valueSerializer);
        }

        public IntermediateSuppression<K, V> bufferBytes(final long bytesToUseForSuppressionStorage, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) {
            this.keySerializer = keySerializer;
            this.valueSerializer = valueSerializer;
            final IntermediateSuppression<K, V> result = new IntermediateSuppression<>(this);
            result.bytesToUseForSuppressionStorage = bytesToUseForSuppressionStorage;
            return result;
        }

        public static IntermediateSuppression withBufferFullStrategy(final BufferFullStrategy bufferFullStrategy) {
            return new IntermediateSuppression().bufferFullStrategy(bufferFullStrategy);
        }

        public IntermediateSuppression<K, V> bufferFullStrategy(final BufferFullStrategy bufferFullStrategy) {
            final IntermediateSuppression<K, V> result = new IntermediateSuppression<>(this);
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

        public Serializer<K> getKeySerializer() {
            return keySerializer;
        }

        public Serializer<V> getValueSerializer() {
            return valueSerializer;
        }
    }

    public Suppression() {}

    public static <K extends Windowed, V> Suppression<K, V> finalResultsOnly(final Duration maxAllowedLateness, final BufferFullStrategy bufferFullStrategy) {
        return Suppression
            .<K, V>withSuppressedLateEvents(maxAllowedLateness)
            .<K, V>suppressIntermediateEvents(
                IntermediateSuppression
                    .<K, V>withEmitAfter(maxAllowedLateness)
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

    public static <K, V> Suppression<K, V> withSuppressedIntermediateEvents(final IntermediateSuppression<K, V> intermediateSuppression) {
        return new Suppression<K, V>().<K, V>suppressIntermediateEvents(intermediateSuppression);
    }

    public Suppression<K, V> suppressIntermediateEvents(final IntermediateSuppression<K, V> intermediateSuppression) {
        final Suppression<K, V> result = new Suppression<>();
        result.latenessBound = this.latenessBound;
        result.intermediateSuppression = intermediateSuppression;
        return result;
    }

    public Duration getLatenessBound() {
        return latenessBound;
    }

    public IntermediateSuppression<K, V> getIntermediateSuppression() {
        return intermediateSuppression;
    }
}
