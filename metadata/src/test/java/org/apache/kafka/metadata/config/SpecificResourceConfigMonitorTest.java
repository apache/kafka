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

package org.apache.kafka.metadata.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.apache.kafka.metadata.config.ConfigRegistryTestConstants.BROKER_NUM_QUUX;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Timeout(120)
public final class SpecificResourceConfigMonitorTest {
    static class TestContext {
        int numCallbacks = 0;
        int value = -1;

        SpecificResourceConfigMonitor<Integer> monitor = new SpecificResourceConfigMonitor<>(
            BROKER_NUM_QUUX,
            "1",
            newValue -> this.setValue(newValue),
            0);

        void setValue(int newValue) {
            this.numCallbacks++;
            this.value = newValue;
        }
    }

    @Test
    public void testKey() {
        TestContext context = new TestContext();
        assertEquals(BROKER_NUM_QUUX, context.monitor.key());
    }

    @Test
    public void testNoUpdates() {
        TestContext context = new TestContext();
        assertEquals(0, context.numCallbacks);
    }

    @Test
    public void testUpdateSpecificResource() {
        TestContext context = new TestContext();
        context.monitor.update("1", 123);
        assertEquals(123, context.value);
        assertEquals(1, context.numCallbacks);
    }

    @Test
    public void testUpdateSpecificResourceTwiceHasNoEffect() {
        TestContext context = new TestContext();
        context.monitor.update("1", 123);
        context.monitor.update("1", 123);
        assertEquals(123, context.value);
        assertEquals(1, context.numCallbacks);
    }

    @Test
    public void testUpdateOtherResource() {
        TestContext context = new TestContext();
        context.monitor.update("2", 123);
        assertEquals(-1, context.value);
        assertEquals(0, context.numCallbacks);
    }

    @Test
    public void testUpdateDefaultResource() {
        TestContext context = new TestContext();
        context.monitor.update("", 123);
        assertEquals(123, context.value);
        assertEquals(1, context.numCallbacks);
    }

    @Test
    public void testUpdateAndRemoveDefaultResource() {
        TestContext context = new TestContext();
        context.monitor.update("", 123);
        context.monitor.update("", null);
        assertEquals(0, context.value);
        assertEquals(2, context.numCallbacks);
    }

    @Test
    public void testUpdateSpecificResourceAndDefault() {
        TestContext context = new TestContext();
        context.monitor.update("", 123);
        context.monitor.update("1", 456);
        context.monitor.update("2", 789);
        assertEquals(456, context.value);
        assertEquals(2, context.numCallbacks);
    }

    @Test
    public void testUpdateSpecificResourceAndRemove() {
        TestContext context = new TestContext();
        context.monitor.update("", 123);
        context.monitor.update("1", 456);
        context.monitor.update("1", null);
        assertEquals(123, context.value);
        assertEquals(3, context.numCallbacks);
    }
}
