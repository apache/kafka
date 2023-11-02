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
package org.apache.kafka.streams.processor;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;

/**
 * The context associated with the current record being processed by
 * a {@link org.apache.kafka.streams.processor.Processor}
 */
public interface RecordContext {

    /**
     * Return the topic name of the current input record; could be {@code null} if it is not
     * available.
     *
     * <p> For example, if this method is invoked within a {@link Punctuator#punctuate(long)
     * punctuation callback}, or while processing a record that was forwarded by a punctuation
     * callback, the record won't have an associated topic.
     * Another example is
     * {@link org.apache.kafka.streams.kstream.KTable#transformValues(ValueTransformerWithKeySupplier, String...)}
     * (and siblings), that do not always guarantee to provide a valid topic name, as they might be
     * executed "out-of-band" due to some internal optimizations applied by the Kafka Streams DSL.
     *
     * @return the topic name
     */
    String topic();

    /**
     * Return the partition id of the current input record; could be {@code -1} if it is not
     * available.
     *
     * <p> For example, if this method is invoked within a {@link Punctuator#punctuate(long)
     * punctuation callback}, or while processing a record that was forwarded by a punctuation
     * callback, the record won't have an associated partition id.
     * Another example is
     * {@link org.apache.kafka.streams.kstream.KTable#transformValues(ValueTransformerWithKeySupplier, String...)}
     * (and siblings), that do not always guarantee to provide a valid partition id, as they might be
     * executed "out-of-band" due to some internal optimizations applied by the Kafka Streams DSL.
     *
     * @return the partition id
     */
    int partition();

    /**
     * Return the offset of the current input record; could be {@code -1} if it is not
     * available.
     *
     * <p> For example, if this method is invoked within a {@link Punctuator#punctuate(long)
     * punctuation callback}, or while processing a record that was forwarded by a punctuation
     * callback, the record won't have an associated offset.
     * Another example is
     * {@link org.apache.kafka.streams.kstream.KTable#transformValues(ValueTransformerWithKeySupplier, String...)}
     * (and siblings), that do not always guarantee to provide a valid offset, as they might be
     * executed "out-of-band" due to some internal optimizations applied by the Kafka Streams DSL.
     *
     * @return the offset
     */
    long offset();

    /**
     * Return the current timestamp.
     *
     * <p> If it is triggered while processing a record streamed from the source processor,
     * timestamp is defined as the timestamp of the current input record; the timestamp is extracted from
     * {@link org.apache.kafka.clients.consumer.ConsumerRecord ConsumerRecord} by {@link TimestampExtractor}.
     * Note, that an upstream {@link org.apache.kafka.streams.processor.api.Processor}
     * might have set a new timestamp by calling
     * {@link org.apache.kafka.streams.processor.api.ProcessorContext#forward(org.apache.kafka.streams.processor.api.Record)
     * forward(..., To.all().withTimestamp(...))}.
     * In particular, some Kafka Streams DSL operators set result record timestamps explicitly,
     * to guarantee deterministic results.
     *
     * <p> If it is triggered while processing a record generated not from the source processor (for example,
     * if this method is invoked from the punctuate call), timestamp is defined as the current
     * task's stream time, which is defined as the largest timestamp of any record processed by the task.
     *
     * @return the timestamp
     */
    long timestamp();

    /**
     * Return the headers of the current input record; could be an empty header if it is not
     * available.
     *
     * <p> For example, if this method is invoked within a {@link Punctuator#punctuate(long)
     * punctuation callback}, or while processing a record that was forwarded by a punctuation
     * callback, the record might not have any associated headers.
     * Another example is
     * {@link org.apache.kafka.streams.kstream.KTable#transformValues(ValueTransformerWithKeySupplier, String...)}
     * (and siblings), that do not always guarantee to provide a valid headers, as they might be
     * executed "out-of-band" due to some internal optimizations applied by the Kafka Streams DSL.
     *
     * @return the headers
     */
    Headers headers();

}
