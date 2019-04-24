package org.apache.kafka.streams.test;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TopologyTestDriver;

import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This class makes it easier to write tests with {@link TopologyTestDriver}.
 * To use {@code TestOutputTopic} create new class with topicName and correct Serdes or Deserealizers
 * In actual test code, you can read message values, keys, {@link KeyValue} or {@link ProducerRecord}
 * without needing to care serdes. You need to have own TestOutputTopic for each topic.
 *
 * If you need to test key, value and headers, use @{link #readRecord} methods.
 * Using @{link #readKeyValue} you get directly KeyValue, but have no access to headers any more
 * Using @{link #readValue} and @{link #readKey} you get directly Key or Value, but have no access to headers any more
 * Note, if using @{link #readKey} and @{link #readValue} in sequence, you get the key of first record and value of the next one
 *
 * <h2>Processing messages</h2>*
 * <pre>{@code
 *      private TestOutputTopic<String, Long> outputTopic;
 * @Before
 *      ...
 *     outputTopic = new TestOutputTopic<String, Long>(testDriver, outputTopic, new Serdes.StringSerde(), new Serdes.LongSerde());
 *
 * @Test
 *     ...
 *     assertThat(outputTopic.readValue()).isEqual(1);
 * </pre>
 *
 * @param <K> the type of the key
 * @param <V> the type of the value
 *
 * @see TopologyTestDriver, ConsumerRecordFactory
 */
public class TestOutputTopic<K, V> {
    //Possibility to use in subclasses
    @SuppressWarnings({"WeakerAccess"})
    protected final TopologyTestDriver driver;
    @SuppressWarnings({"WeakerAccess"})
    protected final String topic;
    @SuppressWarnings({"WeakerAccess"})
    protected final Deserializer<K> keyDeserializer;
    @SuppressWarnings({"WeakerAccess"})
    protected final Deserializer<V> valueDeserializer;

    @SuppressWarnings({"WeakerAccess", "unused"})
    public TestOutputTopic(final TopologyTestDriver driver,
                           final String topic,
                           final Serde<K> keySerde,
                           final Serde<V> valueSerde) {
        this(driver, topic, keySerde.deserializer(), valueSerde.deserializer());
    }

    @SuppressWarnings({"WeakerAccess", "unused"})
    public TestOutputTopic(final TopologyTestDriver driver,
                           final String topic,
                           final Deserializer<K> keyDeserializer,
                           final Deserializer<V> valueDeserializer) {
        this.driver = driver;
        this.topic = topic;
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
    }


    /**
     * Read one Record from output topic and return value only.
     * <p>
     * Note. The key and header is not available
     *
     * @return Next value for output topic
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public V readValue() {
        ProducerRecord<K, V> record = readRecord();
        if (record == null) return null;
        return record.value();
    }

    /**
     * Read one Record from output topic.
     *
     * @return Next output as ProducerRecord
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public KeyValue<K, V> readKeyValue() {
        ProducerRecord<K, V> record = readRecord();
        if (record == null) return null;
        return new KeyValue<>(record.key(), record.value());
    }

    /**
     * Read one Record from output topic.
     *
     * @return Next output as ProducerRecord
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public ProducerRecord<K, V> readRecord() {
        return driver.readOutput(topic, keyDeserializer, valueDeserializer);
    }

    /**
     * Read output to map.
     * If the existing key is modified, it can appear twice in output and is replaced in map
     *
     * @return Map of output by key
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public Map<K, V> readKeyValuesToMap() {
        final Map<K, V> output = new HashMap<>();
        ProducerRecord<K, V> outputRow;
        while ((outputRow = readRecord()) != null) {
            output.put(outputRow.key(), outputRow.value());
        }
        return output;
    }

    /**
     * Read output KeyValues to map.
     * If the existing key is modified, it can appear twice in output and is replaced in map
     *
     * @return List of output KeyValues
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public List<KeyValue<K, V>> readKeyValuesToList() {
        final List<KeyValue<K, V>> output = new LinkedList<>();
        KeyValue<K, V> outputRow;
        while ((outputRow = readKeyValue()) != null) {
            output.add(outputRow);
        }
        return output;
    }

    /**
     * Read values to list.
     * If the existing key is modified, it can appear twice in output and is replaced in map
     *
     * @return List of output KeyValues
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public List<V> readValuesToList() {
        final List<V> output = new LinkedList<>();
        V outputValue;
        while ((outputValue = readValue()) != null) {
            output.add(outputValue);
        }
        return output;
    }

    public Iterable<ProducerRecord<K, V>> iterableRecords() {
        return driver.iterableOutput(topic, keyDeserializer, valueDeserializer);
    };

    public Iterable<KeyValue<K, V>> iterableKeyValues() {
        final Deque<KeyValue<K, V>> output = new LinkedList<>();
        iterableRecords().forEach(record -> output.add(new KeyValue<>(record.key(), record.value())));
        return output;
    }

    public Iterable<V> iterableValues() {
        final Deque<V> output = new LinkedList<>();
        iterableRecords().forEach(record -> output.add(record.value()));
        return output;
    }
}
