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

/**
 * Iterator interface of {@link KeyValue} with key typed {@link Long} used for {@link WindowStore#fetch(Object, long, long)}.
 *
 * Users need to call its {@code close} method explicitly upon completeness to release resources,
 * or use try-with-resources statement (available since JDK7) for this {@link Closeable} class.
 *
 * @param <V> Type of values
 */
public interface WindowStoreIterator<V> extends KeyValueIterator<Long, V>, Closeable {

    @Override
    void close();
}
