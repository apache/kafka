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
package org.apache.kafka.streams.state;

import org.apache.kafka.common.annotation.InterfaceAudience;
import org.apache.kafka.common.annotation.InterfaceStability.Evolving;
import org.apache.kafka.streams.processor.api.ReadOnlyRecord;

import java.io.Closeable;
import java.util.Iterator;

/**
 * Iterator interface of {@link ReadOnlyRecord}.
 * <p>
 * This is the result type of the headers-aware range/window/session IQv2 query types: it yields
 * {@link ReadOnlyRecord} elements directly, so each element carries its own key, value, timestamp,
 * and headers.
 * <p>
 * Users must call its {@code close} method explicitly upon completeness to release resources,
 * or use try-with-resources statement (available since JDK7) for this {@link Closeable} class.
 * Note that {@code remove()} is not supported.
 *
 * @param <K> Type of keys
 * @param <V> Type of values
 */
@InterfaceAudience.Public
@Evolving
public interface ReadOnlyRecordIterator<K, V> extends Iterator<ReadOnlyRecord<K, V>>, Closeable {

    @Override
    void close();
}
