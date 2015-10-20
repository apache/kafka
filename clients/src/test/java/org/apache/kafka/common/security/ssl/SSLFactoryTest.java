/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.common.security.ssl;

import javax.net.ssl.*;

import java.io.File;
import java.util.Map;

import org.apache.kafka.test.TestSSLUtils;
import org.apache.kafka.common.network.Mode;

import org.junit.Test;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;


/**
 * A set of tests for the selector over ssl. These use a test harness that runs a simple socket server that echos back responses.
 */

public class SSLFactoryTest {

    @Test
    public void testSSLFactoryConfiguration() throws Exception {
        File trustStoreFile = File.createTempFile("truststore", ".jks");
        Map<String, Object> serverSSLConfig = TestSSLUtils.createSSLConfig(false, true, Mode.SERVER, trustStoreFile, "server");
        SSLFactory sslFactory = new SSLFactory(Mode.SERVER);
        sslFactory.configure(serverSSLConfig);
        //host and port are hints
        SSLEngine engine = sslFactory.createSSLEngine("localhost", 0);
        assertNotNull(engine);
        String[] expectedProtocols = {"TLSv1.2"};
        assertArrayEquals(expectedProtocols, engine.getEnabledProtocols());
        assertEquals(false, engine.getUseClientMode());
    }

    @Test
    public void testClientMode() throws Exception {
        File trustStoreFile = File.createTempFile("truststore", ".jks");
        Map<String, Object> clientSSLConfig = TestSSLUtils.createSSLConfig(false, true, Mode.CLIENT, trustStoreFile, "client");
        SSLFactory sslFactory = new SSLFactory(Mode.CLIENT);
        sslFactory.configure(clientSSLConfig);
        //host and port are hints
        SSLEngine engine = sslFactory.createSSLEngine("localhost", 0);
        assertTrue(engine.getUseClientMode());
    }

}
