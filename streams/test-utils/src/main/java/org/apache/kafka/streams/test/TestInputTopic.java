package org.apache.kafka.streams.test;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TopologyTestDriver;

import java.util.List;
import java.util.stream.Collectors;

/**
 * This class makes it easier to write tests with {@link TopologyTestDriver} and
 * combines functionality of {@link TopologyTestDriver} and {@link ConsumerRecordFactory}.
 * To use {@code TestInputTopic} create new class with topicName and correct Serdes or Serealizers
 * In actual test code, you can pipe new message values, keys and values or list of {@link KeyValue}
 * without needing to care serdes. You need to have own TestInputTopic for each topic.
 *
 *
 * <h2>Processing messages</h2>*
 * <pre>{@code
 *      private TestInputTopic<String, String> inputTopic;
 * @Before
 *      ...
 *     inputTopic = new TestInputTopic<String, String>(testDriver, inputTopic, new Serdes.StringSerde(), new Serdes.StringSerde());
 *
 * @Test
 *     ...
 *     inputTopic.pipeInput("Hello");
 * </pre>
 *
 * @param <K> the type of the key
 * @param <V> the type of the value
 * @see TopologyTestDriver, ConsumerRecordFactory
 */

public class TestInputTopic<K, V> {
    //Possibility to use in subclasses
    @SuppressWarnings({"WeakerAccess"})
    protected final TopologyTestDriver driver;
    @SuppressWarnings({"WeakerAccess"})
    protected final ConsumerRecordFactory<K, V> factory;

    /**
     * Create a test input topic to pipe messages in.
     * Uses current system time as start timestamp.
     * Auto-advance is disabled.
     *
     * @param driver          @link TopologyTestDriver to use
     * @param topicName       the topic name used
     * @param keySerializer   the key serializer
     * @param valueSerializer the value serializer
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public TestInputTopic(final TopologyTestDriver driver,
                          final String topicName,
                          final Serializer<K> keySerializer,
                          final Serializer<V> valueSerializer) {
        this.driver = driver;
        this.factory = new ConsumerRecordFactory<>(topicName, keySerializer, valueSerializer);
    }

    /**
     * Create a test input topic to pipe messages in.
     * Auto-advance is disabled.
     *
     * @param driver           @link TopologyTestDriver to use
     * @param topicName        the topic name used
     * @param keySerializer    the key serializer
     * @param valueSerializer  the value serializer
     * @param startTimestampMs the initial timestamp for generated records
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public TestInputTopic(final TopologyTestDriver driver,
                          final String topicName,
                          final Serializer<K> keySerializer,
                          final Serializer<V> valueSerializer,
                          final long startTimestampMs) {
        this.driver = driver;
        this.factory = new ConsumerRecordFactory<>(topicName, keySerializer, valueSerializer, startTimestampMs);
    }

    /**
     * Create a test input topic to pipe messages in.
     *
     * @param driver           @link TopologyTestDriver to use
     * @param topicName        the topic name used
     * @param keySerializer    the key serializer
     * @param valueSerializer  the value serializer
     * @param startTimestampMs the initial timestamp for generated records
     * @param autoAdvanceMs    the time increment per generated record
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public TestInputTopic(final TopologyTestDriver driver,
                          final String topicName,
                          final Serializer<K> keySerializer,
                          final Serializer<V> valueSerializer,
                          final long startTimestampMs,
                          final long autoAdvanceMs) {
        this.driver = driver;
        this.factory = new ConsumerRecordFactory<>(topicName, keySerializer, valueSerializer, startTimestampMs, autoAdvanceMs);
    }

    /**
     * Create a test input topic to pipe messages in.
     * Uses current system time as start timestamp.
     * Auto-advance is disabled.
     *
     * @param driver     @link TopologyTestDriver to use
     * @param topicName  the topic name used
     * @param keySerde   the key serializer
     * @param valueSerde the value serializer
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public TestInputTopic(final TopologyTestDriver driver,
                          final String topicName,
                          final Serde<K> keySerde,
                          final Serde<V> valueSerde) {
        this.driver = driver;
        this.factory = new ConsumerRecordFactory<>(topicName, keySerde.serializer(), valueSerde.serializer());
    }

    /**
     * Create a test input topic to pipe messages in.
     * Auto-advance is disabled.
     *
     * @param driver           @link TopologyTestDriver to use
     * @param topicName        the topic name used
     * @param keySerde         the key serializer
     * @param valueSerde       the value serializer
     * @param startTimestampMs the initial timestamp for generated records
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public TestInputTopic(final TopologyTestDriver driver,
                          final String topicName,
                          final Serde<K> keySerde,
                          final Serde<V> valueSerde,
                          final long startTimestampMs) {
        this.driver = driver;
        this.factory = new ConsumerRecordFactory<>(topicName, keySerde.serializer(), valueSerde.serializer(), startTimestampMs);
    }

    /**
     * Create a test input topic to pipe messages in.
     *
     * @param driver           @link TopologyTestDriver to use
     * @param topicName        the topic name used
     * @param keySerde         the key serializer
     * @param valueSerde       the value serializer
     * @param startTimestampMs the initial timestamp for generated records
     * @param autoAdvanceMs    the time increment pre generated record
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public TestInputTopic(final TopologyTestDriver driver,
                          final String topicName,
                          final Serde<K> keySerde,
                          final Serde<V> valueSerde,
                          final long startTimestampMs,
                          final long autoAdvanceMs) {
        this.driver = driver;
        this.factory = new ConsumerRecordFactory<>(topicName, keySerde.serializer(), valueSerde.serializer(), startTimestampMs, autoAdvanceMs);
    }

    /**
     * Send an input message with the given value on the topic and then commit the messages.
     *
     * @param value the record value
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public void pipeInput(V value) {
        driver.pipeInput(factory.create(value));
    }

    /**
     * Send an input message with the given key and value on the topic and then commit the messages.
     *
     * @param key   the record key
     * @param value the record value
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public void pipeInput(K key, V value) {
        driver.pipeInput(factory.create(key, value));
    }

    /**
     * Send an input message with the given key and timestamp on the topic and then commit the messages.
     * Does not auto advance internally tracked time.
     *
     * @param value       the record value
     * @param timestampMs the record timestamp
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public void pipeInput(final V value,
                          final long timestampMs) {
        driver.pipeInput(factory.create(value, timestampMs));
    }

    /**
     * Send an input message with the given key, value and timestamp on the topic and then commit the messages.
     * Does not auto advance internally tracked time.
     *
     * @param key         the record key
     * @param value       the record value
     * @param timestampMs the record timestamp
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public void pipeInput(final K key,
                          final V value,
                          final long timestampMs) {
        driver.pipeInput(factory.create(key, value, timestampMs));
    }

    /**
     * Send an input message with the given key, value and headers on the topic and then commit the messages.
     * The timestamp will be generated based on the constructor provided start time and time will auto advance.
     *
     * @param key     the record key
     * @param value   the record value
     * @param headers the record headers
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public void pipeInput(final K key,
                          final V value,
                          final Headers headers) {
        driver.pipeInput(factory.create(key, value, headers));
    }


    /**
     * Send an input message with the given key, value, timestamp and headers on the topic and then commit the messages.
     * Does not auto advance internally tracked time.
     *
     * @param key         the record key
     * @param value       the record value
     * @param headers     the record headers
     * @param timestampMs the record timestamp
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public void pipeInput(final K key,
                          final V value,
                          final Headers headers,
                          final long timestampMs) {
        driver.pipeInput(factory.create(key, value, headers, timestampMs));
    }

    /**
     * Send input messages with the given KeyValue  list on the topic  then commit each message individually.
     * The timestamp will be generated based on the constructor provided start time and time will auto advance.
     * @param keyValues the list of KeyValue records
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public void pipeKeyValueList(List<KeyValue<K, V>> keyValues) {
        driver.pipeInput(factory.create(keyValues));
    }

    /**
     * Send input messages with the given value liest on the topic then commit each message individually.
     * The timestamp will be generated based on the constructor provided start time and time will auto advance.
     *
     * @param values the list of KeyValue records
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public void pipeValueList(List<V> values) {
        final List<KeyValue<K, V>> keyValues = values.stream().map(v -> new KeyValue<K, V>(null, v)).collect(Collectors.toList());
        pipeKeyValueList(keyValues);
    }

    /**
     * Send input messages with the given KeyValue  list on the topic  then commit each message individually.
     * Does not auto advance internally tracked time.
     * @param keyValues the list of KeyValue records
     * @param startTimestamp the timestamp for the first generated record
     * @param advanceMs the time difference between two consecutive generated records
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public void pipeKeyValueList(List<KeyValue<K, V>> keyValues,
                                 final long startTimestamp,
                                 final long advanceMs) {
        driver.pipeInput(factory.create(keyValues));
    }

    /**
     * Send input messages with the given value liest on the topic then commit each message individually.
     * The timestamp will be generated based on the constructor provided start time and time will auto advance.
     *
     * @param values the list of KeyValue records
     * @param startTimestamp the timestamp for the first generated record
     * @param advanceMs the time difference between two consecutive generated records
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public void pipeValueList(List<V> values,
                              final long startTimestamp,
                              final long advanceMs) {
        final List<KeyValue<K, V>> keyValues = values.stream().map(v -> new KeyValue<K, V>(null, v)).collect(Collectors.toList());
        pipeKeyValueList(keyValues);
    }

}
