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
package org.apache.kafka.tools;

import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.InputStream;
import java.util.Properties;

/**
 * Typical implementations of this interface convert data from an {@link InputStream} received via
 * {@link MessageReader#init(InputStream, Properties)} into a {@link ProducerRecord} instance on each
 * invocation of `{@link MessageReader#readMessage()}`.
 *
 * This is used by the {@link ConsoleProducer}.
 */
public interface MessageReader {

    /**
     * Sets the {@link InputStream} consumed by this reader, and provides configuration properties.
     */
    default void init(InputStream inputStream, Properties props) {}

    /**
     * Reads the next message in the underlying input stream.
     */
    ProducerRecord<byte[], byte[]> readMessage();

    /**
     * Closes this reader. There is no guarantee the underlying stream is closed.
     */
    default void close() {}

}
