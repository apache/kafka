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
package org.apache.kafka.common.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.UnsupportedEncodingException;
import java.lang.management.ManagementFactory;

import javax.management.MBeanException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.OperationsException;

import org.junit.Test;

public class SanitizerTest {

    @Test
    public void testSanitize() throws UnsupportedEncodingException {
        String principal = "CN=Some characters !@#$%&*()_-+=';:,/~";
        String sanitizedPrincipal = Sanitizer.sanitize(principal);
        assertTrue(sanitizedPrincipal.replace('%', '_').matches("[a-zA-Z0-9\\._\\-]+"));
        assertEquals(principal, Sanitizer.desanitize(sanitizedPrincipal));
    }

    @Test
    public void testJmxSanitize() throws MalformedObjectNameException {
        int unquoted = 0;
        for (int i = 0; i < 65536; i++) {
            char c = (char) i;
            String value = "value" + c;
            String jmxSanitizedValue = Sanitizer.jmxSanitize(value);
            if (jmxSanitizedValue.equals(value))
                unquoted++;
            verifyJmx(jmxSanitizedValue, i);
            String encodedValue = Sanitizer.sanitize(value);
            verifyJmx(encodedValue, i);
            // jmxSanitize should not sanitize URL-encoded values
            assertEquals(encodedValue, Sanitizer.jmxSanitize(encodedValue));
        }
        assertEquals(68, unquoted); // a-zA-Z0-9-_% space and tab
    }

    private void verifyJmx(String sanitizedValue, int c) throws MalformedObjectNameException {
        Object mbean = new TestStat();
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        ObjectName objectName = new ObjectName("test:key=" + sanitizedValue);
        try {
            server.registerMBean(mbean, objectName);
            server.unregisterMBean(objectName);
        } catch (OperationsException | MBeanException e) {
            fail("Could not register char=\\u" + c);
        }
    }

    public interface TestStatMBean {
        int getValue();
    }

    public class TestStat implements TestStatMBean {
        public int getValue() {
            return 1;
        }
    }
}
