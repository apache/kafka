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
package org.apache.kafka.clients.admin.internals;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class AdminBootstrapAddressesTest {
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testNoBootstrapSet(boolean nullValue) {
        Map<String, Object> map = new HashMap<>();
        if (nullValue) {
            map.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, null);
            map.put(AdminClientConfig.BOOTSTRAP_CONTROLLERS_CONFIG, null);
        } else {
            map.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "");
            map.put(AdminClientConfig.BOOTSTRAP_CONTROLLERS_CONFIG, "");
        }
        AdminClientConfig config = new AdminClientConfig(map);
        assertEquals("You must set either bootstrap.servers or bootstrap.controllers",
            assertThrows(ConfigException.class, () -> AdminBootstrapAddresses.fromConfig(config)).
                getMessage());
    }

    @Test
    public void testTwoBootstrapsSet() {
        Map<String, Object> map = new HashMap<>();
        map.put(AdminClientConfig.BOOTSTRAP_CONTROLLERS_CONFIG, "localhost:9092");
        map.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        AdminClientConfig config = new AdminClientConfig(map);
        assertEquals("You cannot set both bootstrap.servers and bootstrap.controllers",
                assertThrows(ConfigException.class, () -> AdminBootstrapAddresses.fromConfig(config)).
                        getMessage());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testFromConfig(boolean usingBootstrapControllers) {
        Map<String, Object> map = new HashMap<>();
        String connectString = "localhost:9092,localhost:9093,localhost:9094";
        if (usingBootstrapControllers) {
            map.put(AdminClientConfig.BOOTSTRAP_CONTROLLERS_CONFIG, connectString);
        } else {
            map.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, connectString);
        }
        AdminClientConfig config = new AdminClientConfig(map);
        AdminBootstrapAddresses addresses = AdminBootstrapAddresses.fromConfig(config);
        assertEquals(usingBootstrapControllers, addresses.usingBootstrapControllers());
        assertEquals(Arrays.asList(
            new InetSocketAddress("localhost", 9092),
            new InetSocketAddress("localhost", 9093),
            new InetSocketAddress("localhost", 9094)),
                addresses.addresses());
    }
}
