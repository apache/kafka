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
package org.apache.kafka.server.quota;

import org.apache.kafka.common.utils.Time;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

public class ThrottledChannel implements Delayed {
    private static final Logger LOGGER = LoggerFactory.getLogger(ThrottledChannel.class);
    private final Time time;
    private final int throttleTimeMs;
    private final ThrottleCallback callback;
    private final long endTimeNanos;

    /**
     * Represents a request whose response has been delayed.
     * @param time Time instance to use
     * @param throttleTimeMs Delay associated with this request
     * @param callback Callback for channel throttling
     */
    public ThrottledChannel(Time time, int throttleTimeMs, ThrottleCallback callback) {
        this.time = time;
        this.throttleTimeMs = throttleTimeMs;
        this.callback = callback;
        this.endTimeNanos = time.nanoseconds() + TimeUnit.MILLISECONDS.toNanos(throttleTimeMs);

        // Notify the socket server that throttling has started for this channel.
        callback.startThrottling();
    }

    /**
     * Notify the socket server that throttling has been done for this channel.
     */
    public void notifyThrottlingDone() {
        LOGGER.trace("Channel throttled for: {} ms", throttleTimeMs);
        callback.endThrottling();
    }

    @Override
    public long getDelay(TimeUnit unit) {
        return unit.convert(endTimeNanos - time.nanoseconds(), TimeUnit.NANOSECONDS);
    }

    @Override
    public int compareTo(Delayed other) {
        ThrottledChannel otherChannel = (ThrottledChannel) other;
        return Long.compare(this.endTimeNanos, otherChannel.endTimeNanos);
    }

    public int throttleTimeMs() {
        return throttleTimeMs;
    }
}
