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

import org.apache.kafka.common.utils.LogContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.apache.kafka.metadata.config.ConfigRegistryTestConstants.SCHEMA;
import static org.apache.kafka.metadata.config.ConfigRegistryTestConstants.STATIC_CONFIG_MAP;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Timeout(120)
public final class ConfigRegistryTest {
    static class TestContext {
        ConfigRegistry.Builder builder;

        short curNumFoobars = (short) -1;

        int curNumFoobarsCallbacks = 0;

        int curNumQuux = -1;

        int curNumQuuxCallbacks = 0;

        TestContext() {
            builder = new ConfigRegistry.Builder(
                new LogContext(),
                SCHEMA,
                STATIC_CONFIG_MAP,
                1);
            builder.addShortNodeMonitor("num.foobars", newNumFoobars -> this.setNumFoobars(newNumFoobars));
            builder.addIntNodeMonitor("num.quux", newNumQuux -> this.setNumQuux(newNumQuux));
        }

        void setNumFoobars(short newNumFoobars) {
            curNumFoobars = newNumFoobars;
            curNumFoobarsCallbacks++;
        }

        void setNumQuux(int newNumQuux) {
            curNumQuux = newNumQuux;
            curNumQuuxCallbacks++;
        }
    }

    @Test
    public void testGetStaticNodeShort() {
        TestContext context = new TestContext();
        assertEquals((short) 3,
            context.builder.getStaticNodeShort("num.foobars"));
    }

    @Test
    public void testGetStaticNodeInt() {
        TestContext context = new TestContext();
        assertEquals(5,
            context.builder.getStaticNodeInt("num.quux"));
    }

    @Test
    public void testBeforeInitialCallbacks() {
        TestContext context = new TestContext();
        assertEquals((short) -1, context.curNumFoobars);
        assertEquals(0, context.curNumFoobarsCallbacks);
        assertEquals(-1, context.curNumQuux);
        assertEquals(0, context.curNumFoobarsCallbacks);
    }

    @Test
    public void testInitialCallbacks() {
        TestContext context = new TestContext();
        context.builder.build();
        assertEquals((short) 3, context.curNumFoobars);
        assertEquals(1, context.curNumFoobarsCallbacks);
        assertEquals(5, context.curNumQuux);
        assertEquals(1, context.curNumFoobarsCallbacks);
    }
}
