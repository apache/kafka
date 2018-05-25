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

public class RecordContextStub implements RecordContext {

    private final long offset;
    private long timestamp;
    private final int partition;
    private final String topic;
    private final Headers headers;

    public RecordContextStub() {
        this(-1, -1, -1, "", null);
    }

    public RecordContextStub(final long offset,
                             final long timestamp,
                             final int partition,
                             final String topic,
                             final Headers headers) {
        this.offset = offset;
        this.timestamp = timestamp;
        this.partition = partition;
        this.topic = topic;
        this.headers = headers;
    }

    public RecordContextStub(final long offset,
                             final long timestamp,
                             final int partition,
                             final String topic) {
        this(offset, timestamp, partition, topic, null);
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
    public void setTimestamp(final long timestamp) {
        this.timestamp = timestamp;
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
}
