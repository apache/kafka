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
package org.apache.kafka.tools.api;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Configurable;

import java.io.Closeable;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;

/**
 * Typical implementations of this interface convert data from an `InputStream` received via `readRecords` into a
 * iterator of `ProducerRecord` instance. Note that implementations must have a public nullary constructor.
 *
 * This is used by the `kafka.tools.ConsoleProducer`.
 */
public interface RecordReader extends Closeable, Configurable {

    default void configure(Map<String, ?> configs) {}

    /**
     * read byte array from input stream and then generate an iterator of producer record
     * @param {@link InputStream} of messages. the implementation does not need to close the input stream.
     * @return an iterator of producer record. It should implement following rules. 1) the hasNext() method must be idempotent.
     *         2) the convert error should be thrown by next() method.
     */
    Iterator<ProducerRecord<byte[], byte[]>> readRecords(InputStream inputStream);


    /**
     * Closes this reader.
     * This method is invoked if the iterator from readRecords either has no more records or throws exception.
     */
    default void close() {}
}
