/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.streams.test.TestRecord;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * {@code TestOutputTopic} is used to read records from a topic in {@link TopologyTestDriver}.
 * To use {@code TestOutputTopic} create a new instance via
 * {@link TopologyTestDriver#createOutputTopic(String, Deserializer, Deserializer)}.
 * In actual test code, you can read record values, keys, {@link KeyValue} or {@link TestRecord}
 * If you have multiple source topics, you need to create a {@code TestOutputTopic} for each.
 * <p>
 * If you need to test key, value and headers, use {@link #readRecord()} methods.
 * Using {@link #readKeyValue()} you get a {@link KeyValue} pair, and thus, don't get access to the record's
 * timestamp or headers.
 * Similarly using {@link #readValue()} you only get the value of a record.
 *
 * <h2>Processing records</h2>
 * <pre>{@code
 *     private TestOutputTopic<String, Long> outputTopic;
 *      ...
 *     outputTopic = testDriver.createOutputTopic(OUTPUT_TOPIC, stringDeserializer, longDeserializer);
 *     ...
 *     assertThat(outputTopic.readValue()).isEqual(1);
 * }</pre>
 *
 * @param <K> the type of the record key
 * @param <V> the type of the record value
 * @see TopologyTestDriver
 */
public class TestOutputTopic<K, V> {
    private final TopologyTestDriver driver;
    private final String topic;
    private final Deserializer<K> keyDeserializer;
    private final Deserializer<V> valueDeserializer;

    TestOutputTopic(final TopologyTestDriver driver,
                    final String topicName,
                    final Deserializer<K> keyDeserializer,
                    final Deserializer<V> valueDeserializer) {
        Objects.requireNonNull(driver, "TopologyTestDriver cannot be null");
        Objects.requireNonNull(topicName, "topicName cannot be null");
        Objects.requireNonNull(keyDeserializer, "keyDeserializer cannot be null");
        Objects.requireNonNull(valueDeserializer, "valueDeserializer cannot be null");
        this.driver = driver;
        this.topic = topicName;
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
    }

    /**
     * Read one record from the output topic and return record's value.
     *
     * @return Next value for output topic.
     */
    public V readValue() {
        final TestRecord<K, V> record = readRecord();
        return record.value();
    }

    /**
     * Read one record from the output topic and return its key and value as pair.
     *
     * @return Next output as {@link KeyValue}.
     */
    public KeyValue<K, V> readKeyValue() {
        final TestRecord<K, V> record = readRecord();
        return new KeyValue<>(record.key(), record.value());
    }

    /**
     * Read one Record from output topic.
     *
     * @return Next output as {@link TestRecord}.
     */
    public TestRecord<K, V> readRecord() {
        return driver.readRecord(topic, keyDeserializer, valueDeserializer);
    }

    /**
     * Read output to List.
     * This method can be used if the result is considered a stream.
     * If the result is considered a table, the list will contain all updated, ie, a key might be contained multiple times.
     * If you are only interested in the last table update (ie, the final table state),
     * you can use {@link #readKeyValuesToMap()} instead.
     *
     * @return List of output.
     */
    public List<TestRecord<K, V>> readRecordsToList() {
        final List<TestRecord<K, V>> output = new LinkedList<>();
        while (!isEmpty()) {
            output.add(readRecord());
        }
        return output;
    }


    /**
     * Read output to map.
     * This method can be used if the result is considered a table,
     * when you are only interested in the last table update (ie, the final table state).
     * If the result is considered a stream, you can use {@link #readRecordsToList()} instead.
     * The list will contain all updated, ie, a key might be contained multiple times.
     * If the last update to a key is a delete/tombstone, the key will still be in the map (with null-value).
     *
     * @return Map of output by key.
     */
    public Map<K, V> readKeyValuesToMap() {
        final Map<K, V> output = new HashMap<>();
        TestRecord<K, V> outputRow;
        while (!isEmpty()) {
            outputRow = readRecord();
            if (outputRow.key() == null) {
                throw new IllegalStateException("Null keys not allowed with readKeyValuesToMap method");
            }
            output.put(outputRow.key(), outputRow.value());
        }
        return output;
    }

    /**
     * Read all KeyValues from topic to List.
     *
     * @return List of output KeyValues.
     */
    public List<KeyValue<K, V>> readKeyValuesToList() {
        final List<KeyValue<K, V>> output = new LinkedList<>();
        KeyValue<K, V> outputRow;
        while (!isEmpty()) {
            outputRow = readKeyValue();
            output.add(outputRow);
        }
        return output;
    }

    /**
     * Read all values from topic to List.
     *
     * @return List of output values.
     */
    public List<V> readValuesToList() {
        final List<V> output = new LinkedList<>();
        V outputValue;
        while (!isEmpty()) {
            outputValue = readValue();
            output.add(outputValue);
        }
        return output;
    }

    /**
     * Get size of unread record in the topic queue.
     *
     * @return size of topic queue.
     */
    public final long getQueueSize() {
        return driver.queueSize(topic);
    }

    /**
     * Verify if the topic queue is empty.
     *
     * @return {@code true} if no more record in the topic queue.
     */
    public final boolean isEmpty() {
        return driver.isEmpty(topic);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", TestOutputTopic.class.getSimpleName() + "[", "]")
                .add("topic='" + topic + "'")
                .add("keyDeserializer=" + keyDeserializer.getClass().getSimpleName())
                .add("valueDeserializer=" + valueDeserializer.getClass().getSimpleName())
                .add("size=" + getQueueSize())
                .toString();
    }
}
