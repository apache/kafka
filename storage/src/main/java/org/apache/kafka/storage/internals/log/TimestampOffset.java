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
package org.apache.kafka.storage.internals.log;

/**
 * The mapping between a timestamp to a message offset. The entry means that any message whose timestamp is greater
 * than that timestamp must be at or after that offset.
 */
public class TimestampOffset implements IndexEntry {

    public static final TimestampOffset UNKNOWN = new TimestampOffset(-1, -1);

    public final long timestamp;
    public final long offset;

    /**
     * Create a TimestampOffset with the provided parameters.
     *
     * @param timestamp The max timestamp before the given offset.
     * @param offset The message offset.
     */
    public TimestampOffset(long timestamp, long offset) {
        this.timestamp = timestamp;
        this.offset = offset;
    }

    @Override
    public long indexKey() {
        return timestamp;
    }

    @Override
    public long indexValue() {
        return offset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        TimestampOffset that = (TimestampOffset) o;

        return timestamp == that.timestamp
            && offset == that.offset;
    }

    @Override
    public int hashCode() {
        int result = Long.hashCode(timestamp);
        result = 31 * result + Long.hashCode(offset);
        return result;
    }

    @Override
    public String toString() {
        return String.format("TimestampOffset(offset = %d, timestamp = %d)",
            offset,
            timestamp);
    }
}
