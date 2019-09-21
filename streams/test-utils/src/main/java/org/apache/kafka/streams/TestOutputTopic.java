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

/**
 * TestOutputTopic is used to read records from topic in {@link TopologyTestDriver}.
 * To use {@code TestOutputTopic} create new class {@link TopologyTestDriver#createOutputTopic(String, Deserializer, Deserializer)}
 * In actual test code, you can read message values, keys, {@link KeyValue} or {@link TestRecord}
 * You need to have own TestOutputTopic for each topic.
 * <p>
 * If you need to test key, value and headers, use {@link #readRecord()} methods.
 * Using {@link #readKeyValue()} you get directly KeyValue, but have no access to headers any more
 * Using {@link #readValue()} you get directly value, but have no access to key or  headers any more
 *
 * <h2>Processing messages</h2>
 * <pre>{@code
 *     private TestOutputTopic<String, Long> outputTopic;
 *      ...
 *     outputTopic = testDriver.createOutputTopic(OUTPUT_TOPIC, stringSerde, longSerde);
 *     ...
 *     assertThat(outputTopic.readValue()).isEqual(1);
 * }</pre>
 *
 * @param <K> the type of the Kafka key
 * @param <V> the type of the Kafka value
 * @see TopologyTestDriver
 */
public class TestOutputTopic<K, V> {
    private final TopologyTestDriver driver;
    private final String topic;
    private final Deserializer<K> keyDeserializer;
    private final Deserializer<V> valueDeserializer;

    /**
     * Create a test output topic to read messages from
     *
     * @param driver            TopologyTestDriver to use
     * @param topicName         the topic name used
     * @param keyDeserializer   the key deserializer
     * @param valueDeserializer the value deserializer
     */
    TestOutputTopic(final TopologyTestDriver driver,
                           final String topicName,
                           final Deserializer<K> keyDeserializer,
                           final Deserializer<V> valueDeserializer) {
        Objects.requireNonNull(driver, "TopologyTestDriver cannot be null");
        Objects.requireNonNull(topicName, "topicName cannot be null");
        this.driver = driver;
        this.topic = topicName;
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
        final TestRecord<K, V> record = readRecord();
        return record.value();
    }

    /**
     * Read one Record KeyValue from output topic.
     *
     * @return Next output as KeyValue
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public KeyValue<K, V> readKeyValue() {
        final TestRecord<K, V> record = readRecord();
        return new KeyValue<>(record.key(), record.value());
    }

    /**
     * Read one Record from output topic.
     *
     * @return Next output as ProducerRecord
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public TestRecord<K, V> readRecord() {
        return driver.readRecord(topic, keyDeserializer, valueDeserializer);
    }

    /**
     * Read output to List.
     * If the existing key is modified, it can appear twice in output, but replaced in map
     *
     * @return Map of output by key
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public List<TestRecord<K, V>> readRecordsToList() {
        final List<TestRecord<K, V>> output = new LinkedList<>();
        while (!isEmpty()) {
            output.add(readRecord());
        }
        return output;
    }


    /**
     * Read output to map.
     * If the existing key is modified, it can appear twice in output, but replaced in map
     *
     * @return Map of output by key
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public Map<K, V> readKeyValuesToMap() {
        final Map<K, V> output = new HashMap<>();
        TestRecord<K, V> outputRow;
        while (!isEmpty()) {
            outputRow = readRecord();
            output.put(outputRow.key(), outputRow.value());
        }
        return output;
    }

    /**
     * Read all KeyValues from topic to List.
     *
     * @return List of output KeyValues
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
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
     * @return List of output values
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public List<V> readValuesToList() {
        final List<V> output = new LinkedList<>();
        V outputValue;
        while (!isEmpty()) {
            outputValue = readValue();
            output.add(outputValue);
        }
        return output;
    }

    public final long getQueueSize() {
        return driver.getQueueSize(topic);
    }

    public final boolean isEmpty() {
        return driver.isEmpty(topic);
    }

    @Override
    public String toString() {
        return "TestOutputTopic{topic='" + topic + "',size=" + getQueueSize() + "}";
    }
}
