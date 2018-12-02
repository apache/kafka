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

import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.junit.Assert;
import org.junit.Test;

public class ThrottleTest {
    /**
     * ThrottleMock is a subclass of Throttle that uses a MockTime object.  It calls
     * MockTime#sleep instead of Object#wait.
     */
    private static class ThrottleMock extends Throttle {
        final MockTime time;

        ThrottleMock(MockTime time, int maxPerSec) {
            super(maxPerSec, 100);
            this.time = time;
        }

        @Override
        protected Time time() {
            return time;
        }

        @Override
        protected synchronized void delay(long amount) throws InterruptedException {
            time.sleep(amount);
        }
    }

    @Test
    public void testThrottle() throws Exception {
        MockTime time = new MockTime(0, 0, 0);
        ThrottleMock throttle = new ThrottleMock(time, 3);
        Assert.assertFalse(throttle.increment());
        Assert.assertEquals(0, time.milliseconds());
        Assert.assertFalse(throttle.increment());
        Assert.assertEquals(0, time.milliseconds());
        Assert.assertFalse(throttle.increment());
        Assert.assertEquals(0, time.milliseconds());
        Assert.assertTrue(throttle.increment());
        Assert.assertEquals(100, time.milliseconds());
        time.sleep(50);
        Assert.assertFalse(throttle.increment());
        Assert.assertEquals(150, time.milliseconds());
        Assert.assertFalse(throttle.increment());
        Assert.assertEquals(150, time.milliseconds());
        Assert.assertTrue(throttle.increment());
        Assert.assertEquals(200, time.milliseconds());
    }
};

