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

package org.apache.kafka.trogdor.workload;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kafka.common.utils.Time;

/**
 * This throughput generator configures constant throughput.
 *
 * The lower the window size, the smoother the traffic will be. Using a 100ms window offers no noticeable spikes in
 * traffic while still being long enough to avoid too much overhead.
 *
 * WARNING: Due to binary nature of throughput in terms of messages sent in a window, each window will send at least 1
 * message, and each window sends the same number of messages, rounded down. For example, 99 messages per second with a
 * 100ms window will only send 90 messages per second, or 9 messages per window. Another example, in order to send only
 * 5 messages per second, a window size of 200ms is required. In cases like these, both the `messagesPerSecond` and
 * `windowSizeMs` parameters should be adjusted together to achieve more accurate throughput.
 *
 * Here is an example spec:
 *
 * {
 *    "type": "constant",
 *    "messagesPerSecond": 500,
 *    "windowSizeMs": 100
 * }
 *
 * This will produce a workload that runs 500 messages per second, with a maximum resolution of 50 messages per 100
 * millisecond.
 */

public class ConstantThroughputGenerator implements ThroughputGenerator {
    private final int messagesPerSecond;
    private final int messagesPerWindow;
    private final long windowSizeMs;

    private long nextWindowStarts = 0;
    private int messageTracker = 0;

    @JsonCreator
    public ConstantThroughputGenerator(@JsonProperty("messagesPerSecond") int messagesPerSecond,
                                       @JsonProperty("windowSizeMs") long windowSizeMs) {
        // Calcualte the default values.
        if (windowSizeMs <= 0) {
            windowSizeMs = 100;
        }
        this.windowSizeMs = windowSizeMs;
        this.messagesPerSecond = messagesPerSecond;

        // Use the rest of the parameters to calculate window properties.
        this.messagesPerWindow = (int) ((long) messagesPerSecond / windowSizeMs);
        calculateNextWindow();
    }

    @JsonProperty
    public int messagesPerSecond() {
        return messagesPerSecond;
    }

    private void calculateNextWindow() {
        // Reset the message count.
        messageTracker = 0;

        // Calculate the next window start time.
        long now = Time.SYSTEM.milliseconds();
        if (nextWindowStarts > 0) {
            while (nextWindowStarts < now) {
                nextWindowStarts += windowSizeMs;
            }
        } else {
            nextWindowStarts = now + windowSizeMs;
        }
    }

    @Override
    public synchronized void throttle() throws InterruptedException {
        // Run unthrottled if messagesPerSecond is negative.
        if (messagesPerSecond < 0) {
            return;
        }

        // Calculate the next window if we've moved beyond the current one.
        if (Time.SYSTEM.milliseconds() >= nextWindowStarts) {
            calculateNextWindow();
        }

        // Increment the message tracker.
        messageTracker += 1;

        // Compare the tracked message count with the throttle limits.
        if (messageTracker >= messagesPerWindow) {

            // Wait the difference in time between now and when the next window starts.
            wait(nextWindowStarts - Time.SYSTEM.milliseconds());
        }
    }
}
