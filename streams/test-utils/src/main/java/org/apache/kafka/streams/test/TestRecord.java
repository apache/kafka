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
package org.apache.kafka.streams.test;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

import java.time.Instant;

/**
 * A key/value pair to be send to or received from Kafka. This also consists header information
 * and a timestamp. If record do not contain a timestamp, the TestInputTopic will use auto advance time logic.
 */
public class TestRecord<K, V> {

    private final Headers headers;
    private final K key;
    private final V value;
    private final Instant recordTime;


    /**
     * Creates a record with a specified Instant
     *
     * @param key The key that will be included in the record
     * @param value The record contents
     * @param headers the headers that will be included in the record
     * @param recordTime The timestamp of the record as Instant. If null,
     *                  the timestamp is assigned using Instant.now() or internally tracked time.
     */
    public TestRecord(final K key, final V value, final Headers headers, final Instant recordTime) {
        this.key = key;
        this.value = value;
        this.recordTime = recordTime;
        this.headers = new RecordHeaders(headers);
    }


    /**
     * Creates a record with a specified timestamp
     * 
     * @param key The key that will be included in the record
     * @param value The record contents
     * @param headers the headers that will be included in the record
     * @param timestamp The timestamp of the record, in milliseconds since epoch. If null,
     *                  the timestamp is assigned using System.currentTimeMillis() or internally tracked time.
     */
    public TestRecord(final K key, final V value, final Headers headers, final Long timestamp) {
        if (timestamp != null) {
            if (timestamp < 0)
                throw new IllegalArgumentException(
                        String.format("Invalid timestamp: %d. Timestamp should always be non-negative or null.", timestamp));
            this.recordTime = Instant.ofEpochMilli(timestamp);
        } else
            this.recordTime = null;
        this.key = key;
        this.value = value;
        this.headers = new RecordHeaders(headers);
    }

    /**
     * Creates a record with a specified Instant
     *
     * @param key The key that will be included in the record
     * @param value The record contents
     * @param recordTime The timestamp of the record as Instant. If null,
     *                  the timestamp is assigned using Instant.now() or internally tracked time.
     */
    public TestRecord(final K key, final V value, final Instant recordTime) {
        this(key, value, null, recordTime);
    }

    /**
     * Creates a record
     *
     * @param key The key that will be included in the record
     * @param value The record contents
     * @param headers The headers that will be included in the record
     */
    public TestRecord(final K key, final V value, final Headers headers) {
        this.key = key;
        this.value = value;
        this.headers = new RecordHeaders(headers);
        this.recordTime = null;
    }
    
    /**
     * Creates a record
     *
     * @param key The key that will be included in the record
     * @param value The record contents
     */
    public TestRecord(final K key, final V value) {
        this.key = key;
        this.value = value;
        this.headers = new RecordHeaders();
        this.recordTime = null;
    }

    /**
     * Create a record with no key
     *
     * @param value The record contents
     */
    public TestRecord(final V value) {
        this(null, value);
    }

    /**
     * Create a TestRecord from ConsumerRecord
     *
     * @param record The record contents
     */
    public TestRecord(final ConsumerRecord<K, V> record) {
        this(record.key(), record.value(), record.headers(), record.timestamp());
    }

    /**
     * Create a TestRecord from ProducerRecord
     *
     * @param record The record contents
     */
    public TestRecord(final ProducerRecord<K, V> record) {
        this(record.key(), record.value(), record.headers(), record.timestamp());
    }

    /**
     * @return The headers
     */
    public Headers headers() {
        return headers;
    }

    /**
     * @return The key (or null if no key is specified)
     */
    public K key() {
        return key;
    }


    /**
     * @return The value
     */
    public V value() {
        return value;
    }

    /**
     * @return The timestamp, which is in milliseconds since epoch.
     */
    public Long timestamp() {
        return this.recordTime == null ? null : this.recordTime.toEpochMilli();
    }

    public Headers getHeaders() {
        return headers;
    }

    public K getKey() {
        return key;
    }

    public V getValue() {
        return value;
    }

    public Instant getRecordTime() {
        return recordTime;
    }

    @Override
    public String toString() {
        final String headers = this.headers == null ? "null" : this.headers.toString();
        final String key = this.key == null ? "null" : this.key.toString();
        final String value = this.value == null ? "null" : this.value.toString();
        final String recordTime = this.recordTime == null ? "null" : this.recordTime.toString();
        return "TestRecord(headers=" + headers + ", key=" + key + ", value=" + value +
            ", recordTime=" + recordTime + ")";
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        else if (!(o instanceof TestRecord))
            return false;

        final TestRecord<?, ?> that = (TestRecord<?, ?>) o;

        if (key != null ? !key.equals(that.key) : that.key != null) 
            return false;
        else if (headers != null ? !headers.equals(that.headers) : that.headers != null)
            return false;
        else if (value != null ? !value.equals(that.value) : that.value != null) 
            return false;
        else if (recordTime != null ? !recordTime.equals(that.recordTime) : that.recordTime != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = headers != null ? headers.hashCode() : 0;
        result = 31 * result + (key != null ? key.hashCode() : 0);
        result = 31 * result + (value != null ? value.hashCode() : 0);
        result = 31 * result + (recordTime != null ? recordTime.hashCode() : 0);
        return result;
    }
}
