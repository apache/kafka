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
package org.apache.kafka.common.network;

import javax.net.ssl.*;

import java.util.Map;

import org.apache.kafka.test.TestSSLUtils;

import org.junit.Test;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


/**
 * A set of tests for the selector over ssl. These use a test harness that runs a simple socket server that echos back responses.
 */

public class SSLFactoryTest {

    @Test
    public void testSSLFactoryConfiguration() throws Exception {
        Map<SSLFactory.Mode, Map<String, ?>> sslConfigs = TestSSLUtils.createSSLConfigs(false, true);
        Map<String, ?> serverSSLConfig = sslConfigs.get(SSLFactory.Mode.SERVER);
        SSLFactory sslFactory = new SSLFactory(SSLFactory.Mode.SERVER);
        sslFactory.configure(serverSSLConfig);
        SSLEngine engine = sslFactory.createSSLEngine("localhost", 9093);
        assertNotNull(engine);
        String[] expectedProtocols = {"TLSv1.2"};
        assertEquals(expectedProtocols, engine.getEnabledProtocols());
        assertEquals(false, engine.getUseClientMode());
    }

    @Test
    public void testClientMode() throws Exception {
        Map<SSLFactory.Mode, Map<String, ?>> sslConfigs = TestSSLUtils.createSSLConfigs(false, true);
        Map<String, ?> clientSSLConfig = sslConfigs.get(SSLFactory.Mode.CLIENT);
        SSLFactory sslFactory = new SSLFactory(SSLFactory.Mode.CLIENT);
        sslFactory.configure(clientSSLConfig);
        SSLEngine engine = sslFactory.createSSLEngine("localhost", 9093);
        assertTrue(engine.getUseClientMode());
    }

}
