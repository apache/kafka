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

package org.apache.kafka.clients;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Optional;

import org.junit.Before;
import org.junit.BeforeClass;

public class ClientIdTest {
  
    private static String version;
  
    @BeforeClass
    public static void setup() {
        PrivilegedAction<String> pa = () -> System.getProperty("java.version");
        version = AccessController.doPrivileged(pa);
    }

    @Before
    public void setupEach() {
        ClientId.resetSequence();
    }

    @Test
    public void testStaticUserAgent() {
        assertEquals("test", ClientId.newBuilder().setUserAgent("test").build());
    }

    @Test
    public void testNullUserAgent() {
        assertEquals("Java-kafka-client-1/" + version, ClientId.newBuilder().setUserAgent(null).build());
    }

    @Test
    public void testSequencedUserAgent() {
        assertEquals("test-1", ClientId.newBuilder().setUserAgent("test-%d").build());
        assertEquals("test-2", ClientId.newBuilder().setUserAgent("test-%d").build());
        assertEquals("test-3", ClientId.newBuilder().setUserAgent("test-%d").build());
    }

    @Test
    public void testSetSequence() {
        assertEquals("test-8",
            ClientId.newBuilder().setUserAgent("test-%d").setSequenceNumber(8).build());
    }

    @Test
    public void testDefault() {
        assertEquals("Java-kafka-client-1/" + version, ClientId.newBuilder().build());
        assertEquals("Java-kafka-client-2/" + version, ClientId.newBuilder().build());
    }

    @Test
    public void testGroupRebalanceConfig() {
        GroupRebalanceConfig grc =
            new GroupRebalanceConfig(0, 0, 0, "Group-ABC", Optional.of("instanceID"), 0, false);
        assertEquals("Java-kafka-client-Group-ABC-instanceID/" + version,
            ClientId.newBuilder().setGroupRebalanceConfig(grc).build());
    }

    @Test
    public void testGroupRebalanceConfigNoInstanceId() {
        GroupRebalanceConfig grc =
            new GroupRebalanceConfig(0, 0, 0, "Group-ABC", Optional.empty(), 0, false);
        assertEquals("Java-kafka-client-Group-ABC-1/" + version,
            ClientId.newBuilder().setGroupRebalanceConfig(grc).build());
        assertEquals("Java-kafka-client-Group-ABC-2/" + version,
            ClientId.newBuilder().setGroupRebalanceConfig(grc).build());
    }

    @Test
    public void testGroupRebalanceConfigNullGroupId() {
        // Expected to return default string since no group ID was specified

        GroupRebalanceConfig grc =
            new GroupRebalanceConfig(0, 0, 0, null, Optional.empty(), 0, false);
        assertEquals("Java-kafka-client-1/" + version,
            ClientId.newBuilder().setGroupRebalanceConfig(grc).build());
        assertEquals("Java-kafka-client-2/" + version,
            ClientId.newBuilder().setGroupRebalanceConfig(grc).build());
    }

    @Test
    public void testGroupRebalanceConfigEmptyGroupId() {
        // Expected to return default string since no group ID was specified

        GroupRebalanceConfig grc =
            new GroupRebalanceConfig(0, 0, 0, "", Optional.empty(), 0, false);
        assertEquals("Java-kafka-client-1/" + version,
            ClientId.newBuilder().setGroupRebalanceConfig(grc).build());
        assertEquals("Java-kafka-client-2/" + version,
            ClientId.newBuilder().setGroupRebalanceConfig(grc).build());
    }

    @Test
    public void testStaticUserAgentOverride() {
        // Setting the user agent manually should override everything else

        GroupRebalanceConfig grc =
            new GroupRebalanceConfig(0, 0, 0, "Group-ABC", Optional.of("instanceID"), 0, false);
        assertEquals("test-1",
            ClientId.newBuilder().setUserAgent("test-%d").setGroupRebalanceConfig(grc).build());
    }
}
