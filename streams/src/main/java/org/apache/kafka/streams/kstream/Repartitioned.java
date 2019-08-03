package org.apache.kafka.streams.kstream;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.processor.StreamPartitioner;

/**
 * This class is used to provide the optional parameters for internal repartitioned topics when using:
 * - {@link KStream#repartition(Repartitioned)}
 * - {@link KStream#repartition(KeyValueMapper, Repartitioned)}
 * - {@link KStream#groupByKey(Repartitioned)}
 * - {@link KStream#groupBy(KeyValueMapper, Repartitioned)}
 *
 * @param <K> key type
 * @param <V> value type
 */
public class Repartitioned<K, V> implements NamedOperation<Repartitioned<K, V>> {

    protected final String name;

    protected final Serde<K> keySerde;

    protected final Serde<V> valueSerde;

    protected final Integer numberOfPartitions;

    protected final StreamPartitioner<? super K, ? super V> partitioner;

    private Repartitioned(String name,
                          Serde<K> keySerde,
                          Serde<V> valueSerde,
                          Integer numberOfPartitions,
                          StreamPartitioner<? super K, ? super V> partitioner) {
        this.name = name;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.numberOfPartitions = numberOfPartitions;
        this.partitioner = partitioner;
    }

    /**
     * Create a {@link Repartitioned} instance with the provided name used as part of the repartition topic if required.
     *
     * @param name the name used as a processor named and part of the repartition topic name if required.
     * @param <K>  key type
     * @param <V>  value type
     * @return A new {@link Repartitioned} instance configured with processor name and repartition topic name
     * @see KStream#repartition(Repartitioned)
     * @see KStream#repartition(KeyValueMapper, Repartitioned)
     * @see KStream#groupByKey(Repartitioned)
     * @see KStream#groupBy(KeyValueMapper, Repartitioned)
     */
    public static <K, V> Repartitioned<K, V> as(final String name) {
        return new Repartitioned<>(name, null, null, null, null);
    }

    /**
     * Create a {@link Repartitioned} instance with provided key serde and value serde.
     *
     * @param keySerde   Serde to use for serializing the key
     * @param valueSerde Serde to use for serializing the value
     * @param <K>        key type
     * @param <V>        value type
     * @return A new {@link Repartitioned} instance configured with key serde and value serde
     * @see KStream#repartition(Repartitioned)
     * @see KStream#repartition(KeyValueMapper, Repartitioned)
     * @see KStream#groupByKey(Repartitioned)
     * @see KStream#groupBy(KeyValueMapper, Repartitioned)
     */
    public static <K, V> Repartitioned<K, V> with(final Serde<K> keySerde,
                                                  final Serde<V> valueSerde) {
        return new Repartitioned<>(null, keySerde, valueSerde, null, null);
    }

    /**
     * Create a {@link Repartitioned} instance with provided partitioner.
     *
     * @param partitioner the function used to determine how records are distributed among partitions of the topic,
     *                    if not specified and the key serde provides a {@link WindowedSerializer} for the key
     *                    {@link WindowedStreamPartitioner} will be used—otherwise {@link DefaultPartitioner} will be used
     * @param <K>         key type
     * @param <V>         value type
     * @return A new {@link Repartitioned} instance configured with partitioner
     * @see KStream#repartition(Repartitioned)
     * @see KStream#repartition(KeyValueMapper, Repartitioned)
     * @see KStream#groupByKey(Repartitioned)
     * @see KStream#groupBy(KeyValueMapper, Repartitioned)
     */
    public static <K, V> Repartitioned<K, V> streamPartitioner(final StreamPartitioner<? super K, ? super V> partitioner) {
        return new Repartitioned<>(null, null, null, null, partitioner);
    }

    /**
     * Create a {@link Repartitioned} instance with provided number of partitions for repartition topic if required.
     *
     * @param numberOfPartitions number of partitions used when creating repartition topic if required
     * @param <K>                key type
     * @param <V>                value type
     * @return A new {@link Repartitioned} instance configured number of partitions
     * @see KStream#repartition(Repartitioned)
     * @see KStream#repartition(KeyValueMapper, Repartitioned)
     * @see KStream#groupByKey(Repartitioned)
     * @see KStream#groupBy(KeyValueMapper, Repartitioned)
     */
    public static <K, V> Repartitioned<K, V> numberOfPartitions(final int numberOfPartitions) {
        return new Repartitioned<>(null, null, null, numberOfPartitions, null);
    }

    /**
     * Create a new instance of {@link Repartitioned} with the provided name used as part of repartition topic and processor name.
     * Note that Kafka Streams creates repartition topic only if required.
     *
     * @param name the name used for the processor name and as part of the repartition topic name if required
     * @return a new {@link Repartitioned} instance configured with the name
     */
    @Override
    public Repartitioned<K, V> withName(final String name) {
        return new Repartitioned<>(name, keySerde, valueSerde, numberOfPartitions, partitioner);
    }

    /**
     * Create a new instance of {@link Repartitioned} with the provided number of partitions for repartition topic.
     * Note that Kafka Streams creates repartition topic only if required.
     *
     * @param numberOfPartitions the name used for the processor name and as part of the repartition topic name if required
     * @return a new {@link Repartitioned} instance configured with the number of partitions
     */
    public Repartitioned<K, V> withNumberOfPartitions(final int numberOfPartitions) {
        return new Repartitioned<>(name, keySerde, valueSerde, numberOfPartitions, partitioner);
    }

    /**
     * Create a new instance of {@link Repartitioned} with the provided key serde.
     *
     * @param keySerde Serde to use for serializing the key
     * @return a new {@link Repartitioned} instance configured with the key serde
     */
    public Repartitioned<K, V> withKeySerde(final Serde<K> keySerde) {
        return new Repartitioned<>(name, keySerde, valueSerde, numberOfPartitions, partitioner);
    }

    /**
     * Create a new instance of {@link Repartitioned} with the provided value serde.
     *
     * @param valueSerde Serde to use for serializing the value
     * @return a new {@link Repartitioned} instance configured with the value serde
     */
    public Repartitioned<K, V> withValueSerde(final Serde<V> valueSerde) {
        return new Repartitioned<>(name, keySerde, valueSerde, numberOfPartitions, partitioner);
    }

    /**
     * Create a new instance of {@link Repartitioned} with the provided partitioner.
     *
     * @param partitioner the function used to determine how records are distributed among partitions of the topic,
     *                    if not specified and the key serde provides a {@link WindowedSerializer} for the key
     *                    {@link WindowedStreamPartitioner} will be used—otherwise {@link DefaultPartitioner} wil be used
     * @return a new {@link Repartitioned} instance configured with provided partitioner
     */
    public Repartitioned<K, V> withStreamPartitioner(final StreamPartitioner<? super K, ? super V> partitioner) {
        return new Repartitioned<>(name, keySerde, valueSerde, numberOfPartitions, partitioner);
    }
}