/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.state;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class StoresTest {

    @Test
    public void shouldCreateInMemoryStoreSupplierWithLoggedConfig() throws Exception {
        final StateStoreSupplier supplier = Stores.create("store")
                .withKeys(Serdes.String())
                .withValues(Serdes.String())
                .inMemory()
                .enableLogging(Collections.singletonMap("retention.ms", "1000"))
                .build();

        final Map<String, String> config = supplier.logConfig();
        assertTrue(supplier.loggingEnabled());
        assertEquals("1000", config.get("retention.ms"));
    }

    @Test
    public void shouldCreateInMemoryStoreSupplierNotLogged() throws Exception {
        final StateStoreSupplier supplier = Stores.create("store")
                .withKeys(Serdes.String())
                .withValues(Serdes.String())
                .inMemory()
                .disableLogging()
                .build();

        assertFalse(supplier.loggingEnabled());
    }

    @Test
    public void shouldCreatePersistenStoreSupplierWithLoggedConfig() throws Exception {
        final StateStoreSupplier supplier = Stores.create("store")
                .withKeys(Serdes.String())
                .withValues(Serdes.String())
                .persistent()
                .enableLogging(Collections.singletonMap("retention.ms", "1000"))
                .build();

        final Map<String, String> config = supplier.logConfig();
        assertTrue(supplier.loggingEnabled());
        assertEquals("1000", config.get("retention.ms"));
    }

    @Test
    public void shouldCreatePersistenStoreSupplierNotLogged() throws Exception {
        final StateStoreSupplier supplier = Stores.create("store")
                .withKeys(Serdes.String())
                .withValues(Serdes.String())
                .persistent()
                .disableLogging()
                .build();

        assertFalse(supplier.loggingEnabled());
    }
}