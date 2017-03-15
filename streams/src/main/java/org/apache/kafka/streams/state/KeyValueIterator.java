/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.kafka.streams.state;

import org.apache.kafka.streams.KeyValue;

import java.io.Closeable;
import java.util.Iterator;

/**
 * Iterator interface of {@link KeyValue}.
 *
 * Users need to call its {@code close} method explicitly upon completeness to release resources,
 * or use try-with-resources statement (available since JDK7) for this {@link Closeable} class.
 *
 * @param <K> Type of keys
 * @param <V> Type of values
 */
public interface KeyValueIterator<K, V> extends Iterator<KeyValue<K, V>>, Closeable {

    @Override
    void close();

    /**
     * Peek at the next key without advancing the iterator
     * @return the key of the next value that would be returned from the next call to next
     */
    K peekNextKey();
}
