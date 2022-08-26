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
package org.apache.kafka.common.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.HashSet;
import java.util.Set;

import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.Test;

public class ErrorsTest {

    @Test
    public void testUniqueErrorCodes() {
        Set<Short> codeSet = new HashSet<>();
        for (Errors error : Errors.values()) {
            codeSet.add(error.code());
        }
        assertEquals(codeSet.size(), Errors.values().length, "Error codes must be unique");
    }

    @Test
    public void testUniqueExceptions() {
        Set<Class<?>> exceptionSet = new HashSet<>();
        for (Errors error : Errors.values()) {
            if (error != Errors.NONE)
                exceptionSet.add(error.exception().getClass());
        }
        assertEquals(exceptionSet.size(), Errors.values().length - 1, "Exceptions must be unique"); // Ignore NONE
    }

    @Test
    public void testExceptionsAreNotGeneric() {
        for (Errors error : Errors.values()) {
            if (error != Errors.NONE)
                assertNotEquals(error.exception().getClass(), ApiException.class, "Generic ApiException should not be used");
        }
    }

    @Test
    public void testNoneException() {
        assertNull(Errors.NONE.exception(), "The NONE error should not have an exception");
    }

    @Test
    public void testForExceptionInheritance() {
        class ExtendedTimeoutException extends TimeoutException { }

        Errors expectedError = Errors.forException(new TimeoutException());
        Errors actualError = Errors.forException(new ExtendedTimeoutException());

        assertEquals(expectedError, actualError, "forException should match super classes");
    }

    @Test
    public void testForExceptionDefault() {
        Errors error = Errors.forException(new ApiException());
        assertEquals(Errors.UNKNOWN_SERVER_ERROR, error, "forException should default to unknown");
    }

    @Test
    public void testExceptionName() {
        String exceptionName = Errors.UNKNOWN_SERVER_ERROR.exceptionName();
        assertEquals("org.apache.kafka.common.errors.UnknownServerException", exceptionName);
        exceptionName = Errors.NONE.exceptionName();
        assertNull(exceptionName);
        exceptionName = Errors.INVALID_TOPIC_EXCEPTION.exceptionName();
        assertEquals("org.apache.kafka.common.errors.InvalidTopicException", exceptionName);
    }

}
