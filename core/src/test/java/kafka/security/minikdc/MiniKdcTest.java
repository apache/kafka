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
package kafka.security.minikdc;

import kafka.utils.TestUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Properties;

public class MiniKdcTest {

    @Test
    public void shouldNotStopImmediatelyWhenStarted() throws Exception {
        Properties config = new Properties();
        config.setProperty(MiniKdc.KDC_BIND_ADDRESS, "0.0.0.0");
        config.setProperty(MiniKdc.TRANSPORT, "TCP");
        config.setProperty(MiniKdc.MAX_TICKET_LIFETIME, "86400000");
        config.setProperty(MiniKdc.ORG_NAME, "Example");
        config.setProperty(MiniKdc.KDC_PORT, "0");
        config.setProperty(MiniKdc.ORG_DOMAIN, "COM");
        config.setProperty(MiniKdc.MAX_RENEWABLE_LIFETIME, "604800000");
        config.setProperty(MiniKdc.INSTANCE, "DefaultKrbServer");

        MiniKdc minikdc = MiniKdc.start(TestUtils.tempDir(), config, TestUtils.tempFile(), Collections.singletonList("foo"));
        boolean running = System.getProperty(MiniKdc.JAVA_SECURITY_KRB5_CONF) != null;
        try {
            Assertions.assertTrue(running, "MiniKdc stopped immediately; it should not have");
        } finally {
            if (running) {
                minikdc.stop();
            }
        }
    }
}