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

import org.apache.kafka.streams.kstream.Windowed;

/**
 * Interface for storing the aggregated values of sessions
 * @param <K>   type of the record keys
 * @param <AGG> type of the aggregated values
 */
public interface SessionWithTimestampStore<K, AGG> extends SessionStore<K, ValueAndTimestamp<AGG>> {

    /**
     * Write the aggregated value and timestamp for the provided key to the store
     * @param sessionKey key of the session to write
     * @param aggregate  the aggregated value for the session, it can be {@code null};
     *                   if the serialized bytes are also {@code null} it is interpreted as deletes
     * @param timestamp  the timestamp for the session; ignored if {@code aggregate} is {@code null}
     * @throws NullPointerException If null is used for sessionKey.
     */
    void put(final Windowed<K> sessionKey, final AGG aggregate, final long timestamp);
}
