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


import org.apache.kafka.streams.kstream.internals.SlidingWindow;

import java.util.Collection;

/**
 * This class is used to specify the behaviour of windowed joins.
 */
public class JoinWindows extends Windows<SlidingWindow> {

    private static final int DEFAULT_NUM_SEGMENTS = 3;

    public final long before;
    public final long after;
    public final int segments;

    private JoinWindows(String name, long before, long after, int segments) {
        super(name);

        this.after = after;
        this.before = before;
        this.segments = segments;
    }

    public static JoinWindows of(String name) {
        return new JoinWindows(name, 0L, 0L, DEFAULT_NUM_SEGMENTS);
    }

    /**
     * Specifies that records of the same key are joinable if their timestamp stamps are within
     * timeDifference.
     *
     * @param timeDifference
     * @return
     */
    public JoinWindows within(long timeDifference) {
        return new JoinWindows(this.name, timeDifference, timeDifference, this.segments);
    }

    /**
     * Specifies that records of the same key are joinable if their timestamp stamps are within
     * timeDifference, and if the timestamp of a record from the secondary stream is
     * is earlier than or equal to the timestamp of a record from the first stream.
     *
     * @param timeDifference
     * @return
     */
    public JoinWindows before(long timeDifference) {
        return new JoinWindows(this.name, timeDifference, this.after, this.segments);
    }

    /**
     * Specifies that records of the same key are joinable if their timestamp stamps are within
     * timeDifference, and if the timestamp of a record from the secondary stream is
     * is later than or equal to the timestamp of a record from the first stream.
     *
     * @param timeDifference
     * @return
     */
    public JoinWindows after(long timeDifference) {
        return new JoinWindows(this.name, this.before, timeDifference, this.segments);
    }

    /**
     * Specifies the number of segments to be used for rolling the window store,
     * this function is not exposed to users but can be called by developers that extend this JoinWindows specs
     *
     * @param segments
     * @return
     */
    protected JoinWindows segments(int segments) {
        return new JoinWindows(name, before, after, segments);
    }

    @Override
    public Collection<SlidingWindow> windowsFor(long timestamp) {
        // this function should never be called
        throw new UnsupportedOperationException("windowsFor() is not supported in JoinWindows");
    }

    @Override
    public boolean equalTo(Windows other) {
        if (!other.getClass().equals(JoinWindows.class))
            return false;

        JoinWindows otherWindows = (JoinWindows) other;

        return this.before == otherWindows.before && this.after == otherWindows.after;
    }

}
