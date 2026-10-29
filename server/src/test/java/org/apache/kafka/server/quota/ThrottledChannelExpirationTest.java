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

import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.server.config.ClientQuotaManagerConfig;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ThrottledChannelExpirationTest {
    private final MockTime time = new MockTime();
    private final Metrics metrics = new Metrics(new MetricConfig(), List.of(), time);
    private int numCallbacksForStartThrottling = 0;
    private int numCallbacksForEndThrottling = 0;
    private final ThrottleCallback callback = new ThrottleCallback() {
        @Override
        public void startThrottling() {
            numCallbacksForStartThrottling++;
        }

        @Override
        public void endThrottling() {
            numCallbacksForEndThrottling++;
        }
    };

    @Test
    public void testCallbackInvocationAfterExpiration() {
        ClientQuotaManager clientMetrics = new ClientQuotaManager(new ClientQuotaManagerConfig(), metrics, QuotaType.PRODUCE, time, "");

        DelayQueue<ThrottledChannel> delayQueue = new DelayQueue<>();
        var reaper = clientMetrics.new ThrottledChannelReaper(delayQueue, "");
        try {
            // Add 4 elements to the queue out of order. Add 2 elements with the same expire timestamp.
            ThrottledChannel channel1 = new ThrottledChannel(time, 10, callback);
            ThrottledChannel channel2 = new ThrottledChannel(time, 30, callback);
            ThrottledChannel channel3 = new ThrottledChannel(time, 30, callback);
            ThrottledChannel channel4 = new ThrottledChannel(time, 20, callback);
            delayQueue.add(channel1);
            delayQueue.add(channel2);
            delayQueue.add(channel3);
            delayQueue.add(channel4);
            assertEquals(4, numCallbacksForStartThrottling);

            for (int i = 1; i <= 3; i++) {
                time.sleep(10);
                reaper.doWork();
                assertEquals(i, numCallbacksForEndThrottling);
            }
            reaper.doWork();
            assertEquals(4, numCallbacksForEndThrottling);
            assertEquals(0, delayQueue.size());
            reaper.doWork();
            assertEquals(4, numCallbacksForEndThrottling);
        } finally {
            clientMetrics.shutdown();
        }
    }

    @Test
    public void testThrottledChannelDelay() {
        ThrottledChannel channel1 = new ThrottledChannel(time, 10, callback);
        ThrottledChannel channel2 = new ThrottledChannel(time, 20, callback);
        ThrottledChannel channel3 = new ThrottledChannel(time, 20, callback);
        assertEquals(10, channel1.throttleTimeMs());
        assertEquals(20, channel2.throttleTimeMs());
        assertEquals(20, channel3.throttleTimeMs());

        for (int i = 0; i <= 2; i++) {
            assertEquals(10 - 10 * i, channel1.getDelay(TimeUnit.MILLISECONDS));
            assertEquals(20 - 10 * i, channel2.getDelay(TimeUnit.MILLISECONDS));
            assertEquals(20 - 10 * i, channel3.getDelay(TimeUnit.MILLISECONDS));
            time.sleep(10);
        }
    }
}
