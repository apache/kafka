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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class JavaTest {

    private String javaVendor;
    private String javaRuntimeName;

    @BeforeEach
    public void before() {
        javaVendor = System.getProperty("java.vendor");
        javaRuntimeName = System.getProperty("java.runtime.name");
    }

    @AfterEach
    public void after() {
        System.setProperty("java.vendor", javaVendor);
        System.setProperty("java.runtime.name", javaRuntimeName);
    }

    @Test
    public void testIsIBMJdk() {
        System.setProperty("java.vendor", "Oracle Corporation");
        assertFalse(Java.isIbmJdk());
        System.setProperty("java.vendor", "IBM Corporation");
        assertTrue(Java.isIbmJdk());
    }

    @Test
    public void testIsIBMJdkSemeru() {
        System.setProperty("java.vendor", "Oracle Corporation");
        assertFalse(Java.isIbmJdkSemeru());
        System.setProperty("java.vendor", "IBM Corporation");
        System.setProperty("java.runtime.name", "Java(TM) SE Runtime Environment");
        assertFalse(Java.isIbmJdkSemeru());
        System.setProperty("java.vendor", "IBM Corporation");
        System.setProperty("java.runtime.name", "IBM Semeru Runtime Certified Edition");
        assertTrue(Java.isIbmJdkSemeru());
    }

    @Test
    public void testLoadKerberosLoginModule() throws ClassNotFoundException {
        // IBM Semeru JDKs use the OpenJDK security providers
        String clazz = Java.isIbmJdk() && !Java.isIbmJdkSemeru()
                ? "com.ibm.security.auth.module.Krb5LoginModule"
                : "com.sun.security.auth.module.Krb5LoginModule";
        Class.forName(clazz);
    }

    @Test
    public void testJavaVersion() {
        Java.Version v = Java.parseVersion("9");
        assertEquals(9, v.majorVersion);
        assertEquals(0, v.minorVersion);
        assertTrue(v.isJava9Compatible());

        v = Java.parseVersion("9.0.1");
        assertEquals(9, v.majorVersion);
        assertEquals(0, v.minorVersion);
        assertTrue(v.isJava9Compatible());

        v = Java.parseVersion("9.0.0.15"); // Azul Zulu
        assertEquals(9, v.majorVersion);
        assertEquals(0, v.minorVersion);
        assertTrue(v.isJava9Compatible());

        v = Java.parseVersion("9.1");
        assertEquals(9, v.majorVersion);
        assertEquals(1, v.minorVersion);
        assertTrue(v.isJava9Compatible());

        v = Java.parseVersion("1.8.0_152");
        assertEquals(1, v.majorVersion);
        assertEquals(8, v.minorVersion);
        assertFalse(v.isJava9Compatible());

        v = Java.parseVersion("1.7.0_80");
        assertEquals(1, v.majorVersion);
        assertEquals(7, v.minorVersion);
        assertFalse(v.isJava9Compatible());
    }
}
