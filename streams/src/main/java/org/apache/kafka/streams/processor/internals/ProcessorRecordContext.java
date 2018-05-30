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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.streams.processor.RecordContext;

import java.util.Objects;

public class ProcessorRecordContext implements RecordContext {

    long timestamp;
    final long offset;
    final String topic;
    final int partition;
    final Headers headers;

    public ProcessorRecordContext(final long timestamp,
                                  final long offset,
                                  final int partition,
                                  final String topic,
                                  final Headers headers) {

        this.timestamp = timestamp;
        this.offset = offset;
        this.topic = topic;
        this.partition = partition;
        this.headers = headers;
    }

    public ProcessorRecordContext(final long timestamp,
                                  final long offset,
                                  final int partition,
                                  final String topic) {
        this(timestamp, offset, partition, topic, null);
    }

    public void setTimestamp(final long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public long offset() {
        return offset;
    }

    @Override
    public long timestamp() {
        return timestamp;
    }

    @Override
    public String topic() {
        return topic;
    }

    @Override
    public int partition() {
        return partition;
    }

    @Override
    public Headers headers() {
        return headers;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final ProcessorRecordContext that = (ProcessorRecordContext) o;
        return timestamp == that.timestamp &&
                offset == that.offset &&
                partition == that.partition &&
                Objects.equals(topic, that.topic) &&
                Objects.equals(headers, that.headers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, offset, topic, partition, headers);
    }
}
