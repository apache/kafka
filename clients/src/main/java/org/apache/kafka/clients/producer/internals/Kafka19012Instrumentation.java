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
package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.apache.kafka.clients.producer.internals.SenderMetricsRegistry.TOPIC_METRIC_GROUP_NAME;

/**
 * This class is a temporary utility class to help us track down the cause/origin of KAFKA-19012. The main
 * idea is that the record's topic is added to the record in a header (in {@link Producer#send(ProducerRecord)})
 * which is later checked at different points in its way to the broker.
 *
 * <p/>
 *
 * After the user "sends" a {@link ProducerRecord}, internally to the Kafka client it is decomposed into its
 * raw elements:
 *
 * <ul>
 *     <li>Topic</li>
 *     <li>Partition</li>
 *     <li>Timestamp</li>
 *     <li>Headers</li>
 *     <li>Key bytes</li>
 *     <li>Value bytes</li>
 * </ul>
 *
 * The timestamp, headers, key, and value are stored together in data structures (e.g. {@link ProducerBatch}) as the
 * data makes its way from the client internals to the broker. These data structures group the raw record data based on
 * the topic and partition. It appears to be the case that somewhere along this path, on very rare occasions, raw
 * record data originally sent to topic <code>A</code> mistakenly ends up being grouped along with data for
 * topic <code>B</code>. The big question is <em>where</em> does this mistake happen and under what circumstances?
 */
public class Kafka19012Instrumentation {

    /**
     * Well-known header that is added to each record that holds the name of the topic at the time of sending.
     */
    private static final String TOPIC_HEADER_NAME = "KAFKA-19012-topic";

    private final Logger log;

    private final Kafka19012MetricsRegistry metrics;

    public Kafka19012Instrumentation(LogContext logContext, Metrics metrics) {
        this.log = logContext.logger(Kafka19012Instrumentation.class);
        this.metrics = new Kafka19012MetricsRegistry(metrics);
    }

    /**
     * Adds the well-known {@link #TOPIC_HEADER_NAME} header to the record with the topic from the record. This is
     * used to verify internal consistency later in the processing path.
     */
    public void addTopicHeader(ProducerRecord<?, ?> record) {
        if (record == null)
            return;

        RecordHeaders headers = (RecordHeaders) record.headers();

        try {
            // This is the happy path: the header can be added to the record.
            headers.add(TOPIC_HEADER_NAME, Utils.utf8(record.topic()));
        } catch (IllegalStateException e) {
            log.warn("An error occurred adding the {} header to the record, likely because the headers are now readonly", TOPIC_HEADER_NAME, e);
            return;
        }

        // The following code defensively checks the following...
        Header header = headers.lastHeader(TOPIC_HEADER_NAME);

        // ...the topic header is present...
        if (header == null) {
            log.warn("The {} header was added to the record, but it was not returned from lastHeader()", TOPIC_HEADER_NAME);
            return;
        }

        byte[] existingTopicBytes = header.value();

        // ...the topic header has a non-null value...
        if (existingTopicBytes == null) {
            log.warn("The {} header was was returned from lastHeader(), but its value was null", TOPIC_HEADER_NAME);
            return;
        }

        String existingTopic = Utils.utf8(existingTopicBytes);

        // ...the topic header value matches the topic from the record.
        if (!record.topic().equals(existingTopic)) {
            log.warn(
                "The {} header had a value of {}, which differs from the record's topic: {}",
                TOPIC_HEADER_NAME,
                existingTopic,
                record.topic()
            );
        }
    }

    /**
     * This is called from the {@link ProducerBatch#tryAppend(long, byte[], byte[], Header[], Callback, long)} to
     * check the topic consistency. This is a separate method from
     * {@link #checkHeadersForTryAppendForSplit(String, Header[])} to disambiguate which path was taken.
     *
     * @param expectedTopic The topic stored in the {@link ProducerBatch}
     * @param headers       Headers that were originally part of the {@link ProducerRecord} which <em>should</em>
     *                      contain the {@link #TOPIC_HEADER_NAME} header
     */
    public void checkHeadersForTryAppend(String expectedTopic, Header[] headers) {
        checkHeaders(
            expectedTopic,
            headers,
            metrics::tryAppendInconsistencySensor,
            "ProducerBatch.tryAppend()"
        );
    }

    /**
     * This is called from the {@code ProducerBatch#tryAppendForSplit} to check the topic consistency. This is a
     * separate method from {@link #checkHeadersForTryAppend(String, Header[])} to disambiguate which path was taken.
     *
     * @param expectedTopic The topic stored in the {@link ProducerBatch}
     * @param headers       Headers that were originally part of the {@link ProducerRecord} which <em>should</em>
     *                      contain the {@link #TOPIC_HEADER_NAME} header
     */
    public void checkHeadersForTryAppendForSplit(String expectedTopic, Header[] headers) {
        checkHeaders(
            expectedTopic,
            headers,
            metrics::tryAppendForSplitInconsistencySensor,
            "ProducerBatch.tryAppendForSplit()"
        );
    }

    private void checkHeaders(String expectedTopic,
                              Header[] headers,
                              Function<String, Sensor> sensorFn,
                              String location) {
        if (headers == null) {
            // This _shouldn't_ happen, but let's be careful to check first.
            return;
        }

        for (Header header : headers) {
            // It's possible that a null Header ended up in the record. Also check to make sure the header matches
            // the internal topic name header before continuing.
            if (header == null || !header.key().equals(TOPIC_HEADER_NAME))
                continue;

            String headerTopic = Utils.utf8(header.value());

            // Good--the header and the ProducerBatch are consistent, at least for this particular header.
            if (headerTopic.equals(expectedTopic))
                continue;

            log.warn(
                "A topic mismatch was detected in {}! Expected topic: {}, topic from record header {}: {}.",
                location,
                expectedTopic,
                TOPIC_HEADER_NAME,
                headerTopic
            );

            // If there's a topic header mismatch for this topic, we only record it once per record.
            sensorFn.apply(expectedTopic).record();
            break;
        }
    }

    /**
     * Called from {@code RecordAccumulator#appendNewBatch()}, checks that the buffer it's about to write into
     * has the expected position of 0.
     *
     * @param topic    Topic for which the buffer is used
     * @param position Position from the {@link ByteBuffer}
     */
    public void checkBuffer(String topic, long position) {
        if (position == 0)
            return;

        log.warn(
            "A corrupted buffer with position {} was detected for topic {}",
            position,
            topic
        );

        metrics.corruptedBufferSensor(topic).record();
    }

    private static class Kafka19012MetricsRegistry {

        private final MetricNameTemplate tryAppendInconsistencyRate;
        private final MetricNameTemplate tryAppendInconsistencyTotal;
        private final MetricNameTemplate tryAppendForSplitInconsistencyRate;
        private final MetricNameTemplate tryAppendForSplitInconsistencyTotal;
        private final MetricNameTemplate corruptedBufferRate;
        private final MetricNameTemplate corruptedBufferTotal;

        private final Metrics metrics;

        public Kafka19012MetricsRegistry(Metrics metrics) {
            this.metrics = metrics;
            LinkedHashSet<String> topicTags = new LinkedHashSet<>(metrics.config().tags().keySet());
            topicTags.add("topic");

            // We can't create the MetricName up front for these, because we don't know the topic yet.
            this.tryAppendInconsistencyRate = createTopicTemplate(
                "kafka-19012-try-append-topic-inconsistency-rate",
                "The average per-second number of records using tryAppend() that contained inconsistent topics",
                topicTags
            );
            this.tryAppendInconsistencyTotal = createTopicTemplate(
                "kafka-19012-try-append-topic-inconsistency-total",
                "The total number of records using tryAppend() that contained inconsistent topics",
                topicTags
            );
            this.tryAppendForSplitInconsistencyRate = createTopicTemplate(
                "kafka-19012-try-append-for-split-topic-inconsistency-rate",
                "The average per-second number of records using tryAppendForSplit() that contained inconsistent topics",
                topicTags
            );
            this.tryAppendForSplitInconsistencyTotal = createTopicTemplate(
                "kafka-19012-try-append-for-split-topic-inconsistency-total",
                "The total number of records using tryAppendForSplit() that contained inconsistent topics",
                topicTags
            );
            this.corruptedBufferRate = createTopicTemplate(
                "kafka-19012-corrupted-batch-buffer-rate",
                "The average per-second number of buffers that had a nonzero position when appending records",
                topicTags
            );
            this.corruptedBufferTotal = createTopicTemplate(
                "kafka-19012-corrupted-batch-buffer-total",
                "The total number of buffers that had a nonzero position when appending records",
                topicTags
            );
        }

        private MetricNameTemplate createTopicTemplate(String name, String description, Set<String> topicTags) {
            return new MetricNameTemplate(name, TOPIC_METRIC_GROUP_NAME, description, topicTags);
        }

        private Sensor tryAppendInconsistencySensor(String topic) {
            return sensor(
                topic,
                "topic." + topic + ".try-append.inconsistency",
                tags -> metrics.metricInstance(tryAppendInconsistencyRate, tags),
                tags -> metrics.metricInstance(tryAppendInconsistencyTotal, tags)
            );
        }

        private Sensor tryAppendForSplitInconsistencySensor(String topic) {
            return sensor(
                topic,
                "topic." + topic + ".try-append-for-split.inconsistency",
                tags -> metrics.metricInstance(tryAppendForSplitInconsistencyRate, tags),
                tags -> metrics.metricInstance(tryAppendForSplitInconsistencyTotal, tags)
            );
        }

        private Sensor corruptedBufferSensor(String topic) {
            return sensor(
                topic,
                "topic." + topic + ".corrupted.buffers",
                tags -> metrics.metricInstance(corruptedBufferRate, tags),
                tags -> metrics.metricInstance(corruptedBufferTotal, tags)
            );
        }

        private Sensor sensor(String topic,
                              String sensorName,
                              Function<Map<String, String>, MetricName> rateMetricName,
                              Function<Map<String, String>, MetricName> totalMetricName) {
            Sensor sensor = metrics.getSensor(sensorName);

            if (sensor == null) {
                Map<String, String> metricTags = Collections.singletonMap("topic", topic);
                sensor = metrics.sensor(sensorName);
                sensor.add(new Meter(rateMetricName.apply(metricTags), totalMetricName.apply(metricTags)));
            }

            return sensor;
        }
    }
}
