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

package org.apache.kafka.server.common;

/**
 * Records the time spent processing a request.
 */
public interface RequestTimeRecorder {

    /**
     * Begins a local time span. Each call must be followed by exactly one call to {@link #endLocalTime()}.
     * <p>
     * Local time spans may overlap and may begin and end on different threads. The local time of the
     * request is the time during which at least one span is active.
     */
    void beginLocalTime();

    /**
     * Begins a local time span. Each call must be followed by exactly one call to {@link #endLocalTime()}.
     * <p>
     * Local time spans may overlap and may begin and end on different threads. The local time of the
     * request is the time during which at least one span is active.
     *
     * @param startTimeNanos The start time of the local time span, in nanoseconds. This must come from
     *                       the same clock as the request and must not be later than the current time.
     */
    void beginLocalTime(long startTimeNanos);

    /**
     * Ends a local time span begun by {@link #beginLocalTime()} or {@link #beginLocalTime(long)}.
     */
    void endLocalTime();
}
