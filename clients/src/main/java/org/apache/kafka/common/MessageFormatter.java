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
package org.apache.kafka.common;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.Closeable;
import java.io.PrintStream;
import java.util.Map;

/**
 * This interface allows to define Formatters that can be used to parse and format records read by a
 *  Consumer instance for display.
 * The kafka-console-consumer has built-in support for MessageFormatter, via the --formatter flag.
 *
 * Kafka provides a few implementations to display records of internal topics such as __consumer_offsets,
 * __transaction_state and the MirrorMaker2 topics.
 *
 */
public interface MessageFormatter extends Configurable, Closeable {

    /**
     * Configures the MessageFormatter
     * @param configs Map to configure the formatter
     */
    default void configure(Map<String, ?> configs) {}

    /**
     * Parses and formats a record for display
     * @param consumerRecord the record to format
     * @param output the print stream used to output the record
     */
    void writeTo(ConsumerRecord<byte[], byte[]> consumerRecord, PrintStream output);

    /**
     * Closes the formatter
     */
    default void close() {}
}
