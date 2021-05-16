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

package org.apache.kafka.controller;

import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiError;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Timeout(value = 40)
public class ResultOrErrorTest {
    @Test
    public void testError() {
        ResultOrError<Integer> resultOrError =
            new ResultOrError<>(Errors.INVALID_REQUEST, "missing foobar");
        assertTrue(resultOrError.isError());
        assertFalse(resultOrError.isResult());
        assertEquals(null, resultOrError.result());
        assertEquals(new ApiError(Errors.INVALID_REQUEST, "missing foobar"),
            resultOrError.error());
    }

    @Test
    public void testResult() {
        ResultOrError<Integer> resultOrError = new ResultOrError<>(123);
        assertFalse(resultOrError.isError());
        assertTrue(resultOrError.isResult());
        assertEquals(123, resultOrError.result());
        assertEquals(null, resultOrError.error());
    }

    @Test
    public void testEquals() {
        ResultOrError<String> a = new ResultOrError<>(Errors.INVALID_REQUEST, "missing foobar");
        ResultOrError<String> b = new ResultOrError<>("abcd");
        assertFalse(a.equals(b));
        assertFalse(b.equals(a));
        assertTrue(a.equals(a));
        assertTrue(b.equals(b));
        ResultOrError<String> c = new ResultOrError<>(Errors.INVALID_REQUEST, "missing baz");
        assertFalse(a.equals(c));
        assertFalse(c.equals(a));
        assertTrue(c.equals(c));
    }
}
