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
package org.apache.kafka.raft.internals;

import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.utils.MockTime;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TimeRatioTest {

    @Test
    public void testRatio() {
        MetricConfig config = new MetricConfig();
        MockTime time = new MockTime();
        TimeRatio ratio = new TimeRatio(1.0);

        ratio.record(config, 0.0, time.milliseconds());
        time.sleep(10);
        ratio.record(config, 10, time.milliseconds());
        time.sleep(10);
        ratio.record(config, 0, time.milliseconds());
        assertEquals(0.5, ratio.measure(config, time.milliseconds()));

        time.sleep(10);
        ratio.record(config, 10, time.milliseconds());
        time.sleep(40);
        ratio.record(config, 0, time.milliseconds());
        assertEquals(0.2, ratio.measure(config, time.milliseconds()));
    }

    @Test
    public void testRatioMisalignedWindow() {
        MetricConfig config = new MetricConfig();
        MockTime time = new MockTime();
        TimeRatio ratio = new TimeRatio(1.0);

        ratio.record(config, 0.0, time.milliseconds());
        time.sleep(10);
        ratio.record(config, 10, time.milliseconds());
        time.sleep(10);

        // No recordings, so the last 10ms are not counted.
        assertEquals(1.0, ratio.measure(config, time.milliseconds()));

        // Now the measurement of 5ms arrives. We measure the time since the last
        // recording, so 5ms/10ms = 0.5.
        ratio.record(config, 5, time.milliseconds());
        assertEquals(0.5, ratio.measure(config, time.milliseconds()));
    }

}
