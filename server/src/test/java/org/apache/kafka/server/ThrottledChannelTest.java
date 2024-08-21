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
package org.apache.kafka.server;

import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.server.quota.ThrottleCallback;

import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ThrottledChannelTest {

    private final MockTime time = new MockTime();
    private final ThrottleCallback callback = new ThrottleCallback() {
        @Override
        public void startThrottling() {
        }

        @Override
        public void endThrottling() {
        }
    };

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
