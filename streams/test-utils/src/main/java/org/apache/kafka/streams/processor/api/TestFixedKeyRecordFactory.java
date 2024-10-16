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
package org.apache.kafka.streams.processor.api;

import org.apache.kafka.common.header.Headers;

public final class TestFixedKeyRecordFactory {
    private TestFixedKeyRecordFactory() {
    }

    public static <K, V> FixedKeyRecord<K, V> createFixedKeyRecord(final K key, final V value, final Headers headers) {
        return createFixedKeyRecord(key, value, 0L, headers);
    }

    public static <K, V> FixedKeyRecord<K, V> createFixedKeyRecord(final K key, final V value, final long timestamp) {
        return createFixedKeyRecord(key, value, timestamp, null);
    }

    public static <K, V> FixedKeyRecord<K, V> createFixedKeyRecord(final K key, final V value) {
        return createFixedKeyRecord(key, value, 0L, null);
    }

    public static <K, V> FixedKeyRecord<K, V> createFixedKeyRecord(final K key, final V value, final long timestamp, final Headers headers) {
        return createFixedKeyRecord(new Record<>(key, value, timestamp, headers));
    }

    public static <K, V> FixedKeyRecord<K, V> createFixedKeyRecord(final Record<K, V> record) {
        return InternalFixedKeyRecordFactory.create(record);
    }

}
