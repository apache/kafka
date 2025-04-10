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
package org.apache.kafka.streams.errors;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

/**
 * This interface allows user code to inspect the context of a record that has failed during processing.
 *
 * <p> {@code ErrorHandlerContext} instances are passed into {@link DeserializationExceptionHandler},
 * {@link ProcessingExceptionHandler}, or {@link ProductionExceptionHandler} dependent on what error occurred.
 */
public interface ErrorHandlerContext {
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
     * Additionally, when writing into a changelog topic, there is no associated input record,
     * and thus no topic name is available.
     *
     * @return The topic name.
     */
    String topic();

    /**
     * Return the partition ID of the current input record; could be {@code -1} if it is not
     * available.
     *
     * <p> For example, if this method is invoked within a {@link Punctuator#punctuate(long)
     * punctuation callback}, or while processing a record that was forwarded by a punctuation
     * callback, the record won't have an associated partition ID.
     * Another example is
     * {@link org.apache.kafka.streams.kstream.KTable#transformValues(ValueTransformerWithKeySupplier, String...)}
     * (and siblings), that do not always guarantee to provide a valid partition ID, as they might be
     * executed "out-of-band" due to some internal optimizations applied by the Kafka Streams DSL.
     * Additionally, when writing into a changelog topic, there is no associated input record,
     * and thus no partition is available.
     *
     * @return The partition ID.
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
     * Additionally, when writing into a changelog topic, there is no associated input record,
     * and thus no offset is available.
     *
     * @return The offset.
     */
    long offset();

    /**
     * Return the headers of the current source record; could be an empty header if it is not
     * available.
     *
     * <p> For example, if this method is invoked within a {@link Punctuator#punctuate(long)
     * punctuation callback}, or while processing a record that was forwarded by a punctuation
     * callback, the record might not have any associated headers.
     * Another example is
     * {@link org.apache.kafka.streams.kstream.KTable#transformValues(ValueTransformerWithKeySupplier, String...)}
     * (and siblings), that do not always guarantee to provide valid headers, as they might be
     * executed "out-of-band" due to some internal optimizations applied by the Kafka Streams DSL.
     * Additionally, when writing into a changelog topic, there is no associated input record,
     * and thus no headers are available.
     *
     * @return The headers.
     */
    Headers headers();

    /**
     * Return the current processor node ID.
     *
     * @return The processor node ID.
     */
    String processorNodeId();

    /**
     * Return the task ID.
     *
     * @return The task ID.
     */
    TaskId taskId();

    /**
     * Return the current timestamp; could be {@code -1} if it is not available.
     *
     * <p> For example, when writing into a changelog topic, there is no associated input record,
     * and thus no timestamp is available.
     *
     * <p> If it is triggered while processing a record streamed from the source processor,
     * timestamp is defined as the timestamp of the current input record; the timestamp is extracted from
     * {@link org.apache.kafka.clients.consumer.ConsumerRecord ConsumerRecord} by {@link TimestampExtractor}.
     * Note, that an upstream {@link Processor} might have set a new timestamp by calling
     * {@link ProcessorContext#forward(Record) forward(record.withTimestamp(...))}.
     * In particular, some Kafka Streams DSL operators set result record timestamps explicitly,
     * to guarantee deterministic results.
     *
     * <p> If it is triggered while processing a record generated not from the source processor (for example,
     * if this method is invoked from the punctuate call):
     * <ul>
     *   <li>In case of {@link PunctuationType#STREAM_TIME} timestamp is defined as the current task's stream time,
     *   which is defined as the largest timestamp of any record processed by the task</li>
     *   <li>In case of {@link PunctuationType#WALL_CLOCK_TIME} timestamp is defined the current system time</li>
     * </ul>
     *
     * <p> If it is triggered from a deserialization failure, timestamp is defined as the timestamp of the
     * current rawRecord {@link org.apache.kafka.clients.consumer.ConsumerRecord ConsumerRecord}.
     *
     * @return The timestamp.
     */
    long timestamp();
}
