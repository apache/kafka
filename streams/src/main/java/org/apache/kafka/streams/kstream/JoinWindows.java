/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.kstream;

import org.apache.kafka.streams.kstream.internals.TimeWindow;

import java.util.Map;

/**
 * The window specifications used for joins.
 */
public class JoinWindows extends Windows<TimeWindow> {

    public final long before;
    public final long after;

    private JoinWindows(String name, long before, long after) {
        super(name);

        this.after = after;
        this.before = before;
    }

    public static JoinWindows of(String name) {
        return new JoinWindows(name, 0L, 0L);
    }

    /**
     * Specifies that records of the same key are joinable if their timestamp stamps are within
     * timeDifference.
     *
     * @param timeDifference    join window interval in milliseconds
     */
    public JoinWindows within(long timeDifference) {
        return new JoinWindows(this.name, timeDifference, timeDifference);
    }

    /**
     * Specifies that records of the same key are joinable if their timestamp stamps are within
     * the join window interval, and if the timestamp of a record from the secondary stream is
     * earlier than or equal to the timestamp of a record from the first stream.
     *
     * @param timeDifference    join window interval in milliseconds
     */
    public JoinWindows before(long timeDifference) {
        return new JoinWindows(this.name, timeDifference, this.after);
    }

    /**
     * Specifies that records of the same key are joinable if their timestamp stamps are within
     * the join window interval, and if the timestamp of a record from the secondary stream
     * is later than or equal to the timestamp of a record from the first stream.
     *
     * @param timeDifference    join window interval in milliseconds
     */
    public JoinWindows after(long timeDifference) {
        return new JoinWindows(this.name, this.before, timeDifference);
    }

    @Override
    public Map<Long, TimeWindow> windowsFor(long timestamp) {
        // this function should never be called
        throw new UnsupportedOperationException("windowsFor() is not supported in JoinWindows");
    }

    @Override
    public final boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof JoinWindows)) {
            return false;
        }

        JoinWindows other = (JoinWindows) o;
        return this.before == other.before && this.after == other.after;
    }

    @Override
    public int hashCode() {
        int result = (int) (before ^ (before >>> 32));
        result = 31 * result + (int) (after ^ (after >>> 32));
        return result;
    }

}