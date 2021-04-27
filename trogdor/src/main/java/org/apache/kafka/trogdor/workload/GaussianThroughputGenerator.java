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
import java.util.Random;

/*
 * This throughput generator configures throughput with a gaussian normal distribution on a per-window basis. You can
 * specify how many windows to keep the throughput at the rate before changing. All traffic will follow a gaussian
 * distribution centered around `messagesPerSecondAverage` with a deviation of `messagesPerSecondDeviation`.
 *
 * The lower the window size, the smoother the traffic will be. Using a 100ms window offers no noticeable spikes in
 * traffic while still being long enough to avoid too much overhead.
 *
 * WARNING: Due to binary nature of throughput in terms of messages sent in a window, this does not work well for an
 * average throughput of less than 5 messages per window.  In cases where you want lower throughput, please adjust the
 * `windowSizeMs` accordingly.
 *
 * Here is an example spec:
 *
 * {
 *    "type": "gaussian",
 *    "messagesPerSecondAverage": 500,
 *    "messagesPerSecondDeviation": 50,
 *    "windowsUntilRateChange": 100,
 *    "windowSizeMs": 100
 * }
 *
 * This will produce a workload that runs on average 500 messages per second, however that speed will change every 10
 * seconds due to the `windowSizeMs * windowsUntilRateChange` parameters. The throughput will have the following
 * normal distribution:
 *
 *    An average of the throughput windows of 500 messages per second.
 *    ~68% of the throughput windows are between 450 and 550 messages per second.
 *    ~95% of the throughput windows are between 400 and 600 messages per second.
 *    ~99% of the throughput windows are between 350 and 650 messages per second.
 *
 */

public class GaussianThroughputGenerator implements ThroughputGenerator {
    private final int messagesPerSecondAverage;
    private final int messagesPerSecondDeviation;
    private final int messagesPerWindowAverage;
    private final int messagesPerWindowDeviation;
    private final int windowsUntilRateChange;
    private final long windowSizeMs;

    private final Random random = new Random();

    private long nextWindowStarts = 0;
    private int messageTracker = 0;
    private int windowTracker = 0;
    private int throttleMessages = 0;

    @JsonCreator
    public GaussianThroughputGenerator(@JsonProperty("messagesPerSecondAverage") int messagesPerSecondAverage,
                                       @JsonProperty("messagesPerSecondDeviation") int messagesPerSecondDeviation,
                                       @JsonProperty("windowsUntilRateChange") int windowsUntilRateChange,
                                       @JsonProperty("windowSizeMs") long windowSizeMs) {
        // Calcualte the default values.
        if (windowSizeMs <= 0) {
            windowSizeMs = 100;
        }
        this.windowSizeMs = windowSizeMs;
        this.messagesPerSecondAverage = messagesPerSecondAverage;
        this.messagesPerSecondDeviation = messagesPerSecondDeviation;
        this.windowsUntilRateChange = windowsUntilRateChange;

        // Take per-second calculations and convert them to per-window calculations.
        messagesPerWindowAverage = (int) (messagesPerSecondAverage * windowSizeMs / 1000);
        messagesPerWindowDeviation = (int) (messagesPerSecondDeviation * windowSizeMs / 1000);

        // Calcualte the first window.
        calculateNextWindow(true);
    }

    @JsonProperty
    public int messagesPerSecondAverage() {
        return messagesPerSecondAverage;
    }

    @JsonProperty
    public long messagesPerSecondDeviation() {
        return messagesPerSecondDeviation;
    }

    @JsonProperty
    public long windowsUntilRateChange() {
        return windowsUntilRateChange;
    }

    private synchronized void calculateNextWindow(boolean force) {
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

        // Check the windows between rate changes.
        if ((windowTracker > windowsUntilRateChange) || force) {
            windowTracker = 0;

            // Calculate the number of messages allowed in this window using a normal distribution.
            // The formula is: Messages = Gaussian * Deviation + Average
            throttleMessages = Math.max((int) (random.nextGaussian() * (double) messagesPerWindowDeviation) + messagesPerWindowAverage, 1);
        }
        windowTracker += 1;
    }

    @Override
    public synchronized void throttle() throws InterruptedException {
        // Calculate the next window if we've moved beyond the current one.
        if (Time.SYSTEM.milliseconds() >= nextWindowStarts) {
            calculateNextWindow(false);
        }

        // Increment the message tracker.
        messageTracker += 1;

        // Compare the tracked message count with the throttle limits.
        if (messageTracker >= throttleMessages) {

            // Wait the difference in time between now and when the next window starts.
            wait(nextWindowStarts - Time.SYSTEM.milliseconds());
        }
    }
}
