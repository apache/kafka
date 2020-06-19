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
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collection;
import java.util.Properties;
import java.util.Set;

import static java.lang.Integer.parseInt;
import static java.util.Collections.singleton;
import static java.util.Objects.requireNonNull;
import static org.apache.kafka.streams.kstream.internals.FullChangeSerde.decomposeLegacyFormattedArrayIntoChangeArrays;
import static org.apache.kafka.streams.state.internals.InMemoryTimeOrderedKeyValueBuffer.V_1_CHANGELOG_HEADERS;
import static org.apache.kafka.streams.state.internals.InMemoryTimeOrderedKeyValueBuffer.V_2_CHANGELOG_HEADERS;

public final class InMemoryTimeOrderedKeyValueBufferChangelogDumpTool {
    private static final String USAGE = "usage: {consumer properties file} {topic} {partition}";

    private static String require(final String[] args, final int index) {
        if (args.length <= index) {
            final String message = "argument missing at position " + index + ". " + USAGE;
            System.out.println(message);
            Exit.exit(1);
            throw new IllegalArgumentException(message);
        } else {
            return args[index];
        }
    }

    private static int requireInt(final String[] args, final int index) {
        if (args.length <= index) {
            final String message = "argument missing at position " + index + ". " + USAGE;
            System.out.println(message);
            Exit.exit(1);
            throw new IllegalArgumentException(message);
        } else {
            try {
                return parseInt(args[index]);
            } catch (final NumberFormatException e) {
                System.out.println("Number format exception while parsing integer at position " + index + ".");
                e.printStackTrace(System.out);
                System.out.println(USAGE);
                Exit.exit(1);
                // to prove to the compiler that this branch exits:
                throw new RuntimeException(e);
            }
        }
    }

    public static void main(String[] args) {

        final Properties properties;
        try {
            properties = Utils.loadProps(require(args, 0));
        } catch (final IOException e) {
            System.out.println("IOException while loading properties.");
            e.printStackTrace(System.out);
            Exit.exit(1);
            // to prove to the compiler that this branch exits:
            throw new RuntimeException(e);
        }

        final String topic = require(args, 1);
        final int partition = requireInt(args, 2);

        dumpBufferChangelog(properties, topic, partition);
        return;
    }

    public static void dumpBufferChangelog(final Properties properties, final String topic, final int partition) {
        final TopicPartition topicPartition = new TopicPartition(topic, partition);
        final Set<TopicPartition> assignment = singleton(topicPartition);

        final ConsumerConfig consumerConfig = new ConsumerConfig(properties);
        final Deserializer<?> providedValueDeserializer = consumerConfig.getConfiguredInstance(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Deserializer.class);

        final Properties config = ConsumerConfig.addDeserializerToConfig(properties, null, new ByteArrayDeserializer());

        try (final KafkaConsumer<?, byte[]> consumer = new KafkaConsumer<>(config)) {
            consumer.assign(assignment);
            consumer.seekToBeginning(assignment);
            while (true) {
                final ConsumerRecords<?, byte[]> batch = consumer.poll(Duration.ofSeconds(1));
                for (final ConsumerRecord<?, byte[]> record : batch) {
                    if (record.value() == null) {
                        System.out.printf(
                            "topic=[%s] partition=[%d] offset=[%d] timestamp=[%d] key=[%s] <tombstone>%n",
                            record.topic(),
                            record.partition(),
                            record.offset(),
                            record.timestamp(),
                            record.key()
                        );
                    } else {
                        if (record.headers().lastHeader("v") == null) {
                            // in this case, the changelog value is just the serialized record value
                            final ByteBuffer timeAndValue = ByteBuffer.wrap(record.value());
                            final long time = timeAndValue.getLong();
                            final byte[] changelogValue = new byte[record.value().length - 8];
                            timeAndValue.get(changelogValue);

                            final Change<byte[]> change = requireNonNull(decomposeLegacyFormattedArrayIntoChangeArrays(changelogValue));

                            System.out.printf(
                                "topic=[%s] partition=[%d] offset=[%d] timestamp=[%d] key=[%s] oldValue=[%s] newValue=[%s]%n",
                                record.topic(),
                                record.partition(),
                                record.offset(),
                                time,
                                record.key(),
                                change.oldValue == null ? "null" : providedValueDeserializer.deserialize(record.topic(), change.oldValue),
                                change.newValue == null ? "null" : providedValueDeserializer.deserialize(record.topic(), change.newValue)
                            );
                        } else if (V_1_CHANGELOG_HEADERS.lastHeader("v").equals(record.headers().lastHeader("v"))) {
                            // in this case, the changelog value is a serialized ContextualRecord
                            final ByteBuffer timeAndValue = ByteBuffer.wrap(record.value());
                            final long time = timeAndValue.getLong();
                            final byte[] changelogValue = new byte[record.value().length - 8];
                            timeAndValue.get(changelogValue);

                            final ContextualRecord contextualRecord = ContextualRecord.deserialize(ByteBuffer.wrap(changelogValue));
                            final Change<byte[]> change = requireNonNull(decomposeLegacyFormattedArrayIntoChangeArrays(contextualRecord.value()));

                            System.out.printf(
                                "topic=[%s] partition=[%d] offset=[%d] timestamp=[%d] key=[%s] oldValue=[%s] newValue=[%s] serializedContext=[%s]%n",
                                record.topic(),
                                record.partition(),
                                record.offset(),
                                time,
                                record.key(),
                                change.oldValue == null ? "null" : providedValueDeserializer.deserialize(record.topic(), change.oldValue),
                                change.newValue == null ? "null" : providedValueDeserializer.deserialize(record.topic(), change.newValue),
                                contextualRecord.recordContext()
                            );
                        } else if (V_2_CHANGELOG_HEADERS.lastHeader("v").equals(record.headers().lastHeader("v"))) {
                            // in this case, the changelog value is a serialized BufferValue

                            final ByteBuffer valueAndTime = ByteBuffer.wrap(record.value());
                            final BufferValue bufferValue = BufferValue.deserialize(valueAndTime);
                            final long time = valueAndTime.getLong();
                            System.out.printf(
                                "topic=[%s] partition=[%d] offset=[%d] timestamp=[%d] key=[%s] priorValue=[%s] oldValue=[%s] newValue=[%s] serializedContext=[%s]%n",
                                record.topic(),
                                record.partition(),
                                record.offset(),
                                time,
                                record.key(),
                                bufferValue.priorValue() == null ? "null" : providedValueDeserializer.deserialize(record.topic(), bufferValue.priorValue()),
                                bufferValue.oldValue() == null ? "null" : providedValueDeserializer.deserialize(record.topic(), bufferValue.oldValue()),
                                bufferValue.newValue() == null ? "null" : providedValueDeserializer.deserialize(record.topic(), bufferValue.newValue()),
                                bufferValue.context()
                            );
                        } else {
                            throw new IllegalArgumentException("Restoring apparently invalid changelog record: " + record);
                        }
                    }
                }

                final long position = consumer.position(topicPartition);
                final Long end = consumer.endOffsets(assignment).get(topicPartition);
                if (position >= end) {
                    System.out.println("Consumed to end of topic.");
                    return;
                }
            }
        }
    }
}
