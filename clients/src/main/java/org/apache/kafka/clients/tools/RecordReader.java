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
package org.apache.kafka.clients.tools;

import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.Closeable;
import java.io.InputStream;
import java.util.Map;

/**
 * Typical implementations of this interface convert data from an `InputStream` received via `configure` into a
 * `ProducerRecord` instance on each invocation of `readRecord`. Noted that the implementations to have a public
 * nullary constructor.
 *
 * This is used by the `kafka.tools.ConsoleProducer`.
 */
public interface RecordReader extends Closeable {

    default void configure(InputStream inputStream, Map<String, ?> configs) {}

    /**
     * read byte array from input stream and then generate a producer record
     * @return a producer record
     */
    ProducerRecord<byte[], byte[]> readRecord();


    /**
     * Closes this reader
     */
    default void close() {}
}
