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

public class UnsupportedProtocolFieldExceptionTest {

    @Test
    public void testFieldConstructorFormatsMessage() {
        UnsupportedProtocolFieldException exception =
            new UnsupportedProtocolFieldException("validateOnly", "CREATE_TOPICS", 0, 1);
        assertEquals("The cluster does not support [validateOnly] in CREATE_TOPICS API version 0. "
                + "Upgrade the cluster to CREATE_TOPICS API version >= 1 to enable [validateOnly].",
            exception.getMessage());
    }

    @Test
    public void testMessageConstructorPassesMessageThrough() {
        UnsupportedProtocolFieldException exception =
            new UnsupportedProtocolFieldException("some custom message");
        assertEquals("some custom message", exception.getMessage());
    }

    @Test
    public void testIsUnsupportedVersionException() {
        assertInstanceOf(UnsupportedVersionException.class,
            new UnsupportedProtocolFieldException("field", "SOME_API", 0, 1));
    }
}
