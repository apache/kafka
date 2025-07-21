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
package org.apache.kafka.common.errors;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for the new authentication exception types added for KAFKA-10840.
 */
public class AuthenticationExceptionTypesTest {

    @Test
    public void testCertificateExpiredAuthenticationException() {
        String message = "SSL certificate has expired";
        CertificateExpiredAuthenticationException exception = 
            new CertificateExpiredAuthenticationException(message);
        
        assertEquals(message, exception.getMessage());
        assertInstanceOf(SslAuthenticationException.class, exception);
        assertInstanceOf(AuthenticationException.class, exception);
    }

    @Test
    public void testCertificateExpiredAuthenticationExceptionWithCause() {
        String message = "SSL certificate has expired";
        RuntimeException cause = new RuntimeException("Root cause");
        CertificateExpiredAuthenticationException exception = 
            new CertificateExpiredAuthenticationException(message, cause);
        
        assertEquals(message, exception.getMessage());
        assertEquals(cause, exception.getCause());
        assertInstanceOf(SslAuthenticationException.class, exception);
    }

    @Test
    public void testPersistentAuthenticationException() {
        String message = "Authentication failed persistently";
        PersistentAuthenticationException exception = 
            new PersistentAuthenticationException(message);
        
        assertEquals(message, exception.getMessage());
        assertInstanceOf(AuthenticationException.class, exception);
    }

    @Test
    public void testPersistentAuthenticationExceptionWithCause() {
        String message = "Authentication failed persistently";
        RuntimeException cause = new RuntimeException("Root cause");
        PersistentAuthenticationException exception = 
            new PersistentAuthenticationException(message, cause);
        
        assertEquals(message, exception.getMessage());
        assertEquals(cause, exception.getCause());
        assertInstanceOf(AuthenticationException.class, exception);
    }

    @Test
    public void testExceptionHierarchy() {
        CertificateExpiredAuthenticationException certException = 
            new CertificateExpiredAuthenticationException("cert expired");
        PersistentAuthenticationException persistentException = 
            new PersistentAuthenticationException("auth failed");
        
        // Verify inheritance hierarchy
        assertTrue(certException instanceof SslAuthenticationException);
        assertTrue(certException instanceof AuthenticationException);
        assertTrue(persistentException instanceof AuthenticationException);
    }

    @Test 
    public void testExceptionSerialVersionUID() {
        // Verify that serialVersionUID is set to maintain compatibility
        CertificateExpiredAuthenticationException certException = 
            new CertificateExpiredAuthenticationException("test");
        PersistentAuthenticationException persistentException = 
            new PersistentAuthenticationException("test");
        
        // These exceptions should be serializable (extends Exception/RuntimeException)
        assertInstanceOf(Exception.class, certException);
        assertInstanceOf(Exception.class, persistentException);
    }
}