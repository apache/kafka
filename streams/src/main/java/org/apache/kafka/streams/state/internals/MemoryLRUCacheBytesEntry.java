/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.streams.processor.RecordContext;

/**
 * A cache entry
 */
public class MemoryLRUCacheBytesEntry<V> implements RecordContext {


    enum State { clean, dirty }

    public final V value;
    public final long offset;
    public final long timestamp;
    public final String topic;
    public final int partition;
    public State state;

    public MemoryLRUCacheBytesEntry(final V value) {
        this(value, -1, -1, -1, null, State.clean);
    }

    public MemoryLRUCacheBytesEntry(final V value, final long offset, final long timestamp, final int partition, final String topic, final State state) {
        this.value = value;
        this.partition = partition;
        this.topic = topic;
        this.state = state;
        this.offset = offset;
        this.timestamp = timestamp;
    }

    public boolean shouldForward() {
        return state != State.clean;
    }


    public void markClean() {
        state = State.clean;
    }

    public State state() {
        return state;
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

    public boolean isDirty() {
        return state == State.dirty;
    }
}
