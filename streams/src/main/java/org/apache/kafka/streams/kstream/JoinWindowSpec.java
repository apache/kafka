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

/**
 * This class is used to specify the behaviour of windowed joins.
 */
public class JoinWindowSpec {

    public final String name;
    public final long before;
    public final long after;
    public final long retention;
    public final int segments;

    private JoinWindowSpec(String name, long before, long after, long retention, int segments) {
        this.name = name;
        this.after = after;
        this.before = before;
        this.retention = retention;
        this.segments = segments;
    }

    public static JoinWindowSpec of(String name) {
        return new JoinWindowSpec(name, 0L, 0L, 0L, 3);
    }

    /**
     * Specifies that records of the same key are joinable if their timestamp stamps are within
     * timeDifference.
     *
     * @param timeDifference
     * @return
     */
    public JoinWindowSpec within(long timeDifference) {
        return new JoinWindowSpec(name, timeDifference, timeDifference, retention, segments);
    }

    /**
     * Specifies that records of the same key are joinable if their timestamp stamps are within
     * timeDifference, and if the timestamp of a record from the secondary stream is
     * is earlier than or equal to the timestamp of a record from the first stream.
     *
     * @param timeDifference
     * @return
     */
    public JoinWindowSpec before(long timeDifference) {
        return new JoinWindowSpec(name, timeDifference, 0L, retention, segments);
    }

    /**
     * Specifies that records of the same key are joinable if their timestamp stamps are within
     * timeDifference, and if the timestamp of a record from the secondary stream is
     * is later than or equal to the timestamp of a record from the first stream.
     *
     * @param timeDifference
     * @return
     */
    public JoinWindowSpec after(long timeDifference) {
        return new JoinWindowSpec(name, 0L, timeDifference, retention, segments);
    }

    /**
     * Specifies the retention period of windows
     * @param retentionPeriod
     * @return
     */
    public JoinWindowSpec retentionPeriod(long retentionPeriod) {
        return new JoinWindowSpec(name, before, after, retentionPeriod, segments);
    }

    public JoinWindowSpec segments(int segments) {
        return new JoinWindowSpec(name, before, after, retention, segments);
    }

}
