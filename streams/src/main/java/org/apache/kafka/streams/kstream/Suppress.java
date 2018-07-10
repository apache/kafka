package org.apache.kafka.streams.kstream;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.time.Duration;

@SuppressWarnings({"WeakerAccess"})
public class Suppress<K, V> {
    private IntermediateSuppression<K, V> intermediateSuppression = null;
    private TimeDefinition<K, V> timeDefinition = ((context, k, v) -> context.timestamp());
    private BufferConfig<K, V> finalResultsConfig;

    public enum BufferFullStrategy {
        EMIT,
        SPILL_TO_DISK,
        SHUT_DOWN
    }

    public static class BufferConfig<K, V> {
        private long numberOfKeysToRemember = Long.MAX_VALUE;
        private long bytesToUseForSuppressionStorage = Long.MAX_VALUE;
        private BufferFullStrategy bufferFullStrategy = BufferFullStrategy.EMIT;

        private Serializer<K> keySerializer;
        private Serializer<V> valueSerializer;

        private BufferConfig() {}

        private BufferConfig(final BufferConfig<K, V> from) {
            this.numberOfKeysToRemember = from.numberOfKeysToRemember;
            this.bytesToUseForSuppressionStorage = from.bytesToUseForSuppressionStorage;
            this.bufferFullStrategy = from.bufferFullStrategy;
            this.keySerializer = from.keySerializer;
            this.valueSerializer = from.valueSerializer;
        }

        public static <K, V> BufferConfig<K, V> withBufferKeys(final long numberOfKeysToRemember) {
            return new BufferConfig<K, V>().bufferKeys(numberOfKeysToRemember);
        }

        public BufferConfig<K, V> bufferKeys(final long numberOfKeysToRemember) {
            final BufferConfig<K, V> result = new BufferConfig<>(this);
            result.numberOfKeysToRemember = numberOfKeysToRemember;
            return result;
        }


        public static <K, V> BufferConfig<K, V> withBufferBytes(final long bytesToUseForSuppressionStorage, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) {
            return new BufferConfig<K, V>().bufferBytes(bytesToUseForSuppressionStorage, keySerializer, valueSerializer);
        }

        public BufferConfig<K, V> bufferBytes(final long bytesToUseForSuppressionStorage, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) {
            this.keySerializer = keySerializer;
            this.valueSerializer = valueSerializer;
            final BufferConfig<K, V> result = new BufferConfig<>(this);
            result.bytesToUseForSuppressionStorage = bytesToUseForSuppressionStorage;
            return result;
        }


        public static <K, V> BufferConfig<K, V> withBufferFullStrategy(final BufferFullStrategy bufferFullStrategy) {
            return new BufferConfig<K, V>().bufferFullStrategy(bufferFullStrategy);
        }

        public BufferConfig<K, V> bufferFullStrategy(final BufferFullStrategy bufferFullStrategy) {
            final BufferConfig<K, V> result = new BufferConfig<>(this);
            result.bufferFullStrategy = bufferFullStrategy;
            return result;
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

    public static class IntermediateSuppression<K, V> {
        private BufferConfig<K, V> bufferConfig = new BufferConfig<>();
        private Duration timeToWaitForMoreEvents = null;

        private IntermediateSuppression() {}

        private IntermediateSuppression(final IntermediateSuppression<K, V> from) {
            timeToWaitForMoreEvents = from.timeToWaitForMoreEvents;
            this.bufferConfig = from.bufferConfig;
        }

        public static <K, V> IntermediateSuppression<K, V> withEmitAfter(final Duration timeToWaitForMoreEvents) {
            return new IntermediateSuppression<K, V>().emitAfter(timeToWaitForMoreEvents);
        }

        public IntermediateSuppression<K, V> emitAfter(final Duration timeToWaitForMoreEvents) {
            final IntermediateSuppression<K, V> result = new IntermediateSuppression<>(this);
            result.timeToWaitForMoreEvents = timeToWaitForMoreEvents;
            return result;
        }

        public static <K, V> IntermediateSuppression<K, V> withBufferConfig(final BufferConfig<K, V> bufferConfig) {
            return new IntermediateSuppression<K, V>().bufferConfig(bufferConfig);
        }

        public IntermediateSuppression<K, V> bufferConfig(final BufferConfig<K, V> bufferConfig) {
            final IntermediateSuppression<K, V> result = new IntermediateSuppression<>(this);
            result.bufferConfig = bufferConfig;
            return result;
        }

        public Duration getTimeToWaitForMoreEvents() {
            return timeToWaitForMoreEvents;
        }

        public BufferConfig<K, V> getBufferConfig() {
            return bufferConfig;
        }
    }

    public Suppress() {}

    private Suppress(final Suppress<K, V> other) {
        this.timeDefinition = other.timeDefinition;
        this.intermediateSuppression = other.intermediateSuppression;
        this.finalResultsConfig = other.finalResultsConfig;
    }

    public static <K extends Windowed, V> Suppress<K, V> emitFinalResultsOnly(final BufferConfig<K, V> bufferConfig) {
        if (bufferConfig.getBufferFullStrategy() == BufferFullStrategy.EMIT) {
            throw new IllegalArgumentException(
                "The EMIT strategy may produce intermediate results. " +
                    "Select either SHUT_DOWN or SPILL_TO_DISK"
            );
        }

        final Suppress<K, V> suppress = new Suppress<>();
        suppress.finalResultsConfig = bufferConfig;
        suppress.timeDefinition = ((ProcessorContext context, K k, V v) -> k.window().end());
        return suppress;
    }

    public interface TimeDefinition<K, V> {
        long time(ProcessorContext context, K k, V v);
    }

    private static <K, V> Suppress<K, V> usingTimeDefinition(final TimeDefinition<K, V> timeDefinition) {
        final Suppress<K, V> suppress = new Suppress<>();
        suppress.timeDefinition = timeDefinition;
        return suppress;
    }

    public static <K, V> Suppress<K, V> intermediateEvents(final IntermediateSuppression<K, V> intermediateSuppression) {
        return new Suppress<K, V>().<K, V>suppressIntermediateEvents(intermediateSuppression);
    }

    private Suppress<K, V> suppressIntermediateEvents(final IntermediateSuppression<K, V> intermediateSuppression) {
        final Suppress<K, V> result = new Suppress<>(this);
        result.intermediateSuppression = intermediateSuppression;
        return result;
    }

    public IntermediateSuppression<K, V> getIntermediateSuppression() {
        return intermediateSuppression;
    }

    public TimeDefinition<K, V> getTimeDefinition() {
        return timeDefinition;
    }


    public boolean isFinalResultsSuppression() {
        return finalResultsConfig != null;
    }

    public static <K extends Windowed, V> Suppress<K, V> buildFinalResultsSuppression(final Duration windowCloseTime,
                                                                                      final Suppress<K, V> suppress) {
        return Suppress
            .usingTimeDefinition(((ProcessorContext context, K k, V v) -> k.window().end()))
            .suppressIntermediateEvents(
                IntermediateSuppression
                    .<K, V>withEmitAfter(windowCloseTime)
                    .bufferConfig(suppress.finalResultsConfig)
            );
    }
}
