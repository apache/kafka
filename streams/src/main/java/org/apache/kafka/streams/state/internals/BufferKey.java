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
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.utils.Bytes;

import java.util.Objects;

public final class BufferKey implements Comparable<BufferKey> {
    private final long time;
    private final Bytes key;

    BufferKey(final long time, final Bytes key) {
        this.time = time;
        this.key = key;
    }

    long time() {
        return time;
    }

    Bytes key() {
        return key;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final BufferKey bufferKey = (BufferKey) o;
        return time == bufferKey.time &&
            Objects.equals(key, bufferKey.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(time, key);
    }

    @Override
    public int compareTo(final BufferKey o) {
        // ordering of keys within a time uses hashCode.
        final int timeComparison = Long.compare(time, o.time);
        return timeComparison == 0 ? key.compareTo(o.key) : timeComparison;
    }

    @Override
    public String toString() {
        return "BufferKey{" +
            "key=" + key +
            ", time=" + time +
            '}';
    }
}
