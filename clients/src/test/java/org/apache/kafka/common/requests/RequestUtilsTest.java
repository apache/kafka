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

package org.apache.kafka.common.requests;

import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.MismatchedEndpointTypeException;
import org.apache.kafka.common.errors.SecurityDisabledException;
import org.apache.kafka.common.errors.UnsupportedEndpointTypeException;
import org.apache.kafka.common.errors.UnsupportedForMessageFormatException;
import org.apache.kafka.common.errors.UnsupportedVersionException;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RequestUtilsTest {
    @Test
    public void testIsFatalException() {
        assertTrue(RequestUtils.isFatalException(new AuthenticationException("")));
        assertTrue(RequestUtils.isFatalException(new AuthorizationException("")));
        assertTrue(RequestUtils.isFatalException(new MismatchedEndpointTypeException("")));
        assertTrue(RequestUtils.isFatalException(new SecurityDisabledException("")));
        assertTrue(RequestUtils.isFatalException(new UnsupportedEndpointTypeException("")));
        assertTrue(RequestUtils.isFatalException(new UnsupportedForMessageFormatException("")));
        assertTrue(RequestUtils.isFatalException(new UnsupportedVersionException("")));

        // retriable exceptions
        assertFalse(RequestUtils.isFatalException(new DisconnectException("")));
    }
}
