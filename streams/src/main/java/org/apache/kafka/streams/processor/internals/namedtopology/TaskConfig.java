package org.apache.kafka.streams.processor.internals.namedtopology;

import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.util.Properties;
import java.util.function.Function;

/**
 * Streams configs that apply at the task level. At the moment they can be specified either via the configs passed in
 * to the {@link org.apache.kafka.streams.KafkaStreams} or {@link KafkaStreamsNamedTopologyWrapper} constructor, or
 * else by passing them in to {@link org.apache.kafka.streams.StreamsBuilder#build(Properties)} or
 * {@link NamedTopologyStreamsBuilder#buildNamedTopology(Properties)}
 */
public class TaskConfig {
    public final long maxTaskIdleMs;
    public final long taskTimeoutMs;
    public final int maxBufferedSize;
    public final TimestampExtractor timestampExtractor;
    public final DeserializationExceptionHandler deserializationExceptionHandler;

    public TaskConfig(final StreamsConfig applicationConfig, final StreamsConfig topologyConfig) {
        maxTaskIdleMs = getConfig(applicationConfig, topologyConfig, config -> config.getLong(StreamsConfig.MAX_TASK_IDLE_MS_CONFIG));
        taskTimeoutMs = getConfig(applicationConfig, topologyConfig, config -> config.getLong(StreamsConfig.TASK_TIMEOUT_MS_CONFIG));
        maxBufferedSize = getConfig(applicationConfig, topologyConfig, config -> config.getInt(StreamsConfig.BUFFERED_RECORDS_PER_PARTITION_CONFIG));
        timestampExtractor = getConfig(applicationConfig, topologyConfig, StreamsConfig::defaultTimestampExtractor);
        deserializationExceptionHandler = getConfig(applicationConfig, topologyConfig, StreamsConfig::defaultDeserializationExceptionHandler);
    }

    /**
     * @return the value of this config passed in to the topology if it exists, otherwise default application-wide config
     */
    static <V> V getConfig(final StreamsConfig applicationConfig,
                           final StreamsConfig topologyConfig,
                           final Function<StreamsConfig, V> configGetter) {
        final V topologyOverride = configGetter.apply(topologyConfig);
        final V appValue = configGetter.apply(applicationConfig);
        return topologyOverride == null ? appValue : topologyOverride;
    }
}
