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
package org.apache.kafka.server;

import org.apache.kafka.common.config.ConfigException;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ProcessRoleTest {

    @Test
    public void testFromString() {
        assertEquals(ProcessRole.BrokerRole, ProcessRole.fromString("broker"));
        assertEquals(ProcessRole.ControllerRole, ProcessRole.fromString("controller"));
    }

    @Test
    public void testFromStringWithInvalidRole() {
        ConfigException exception = assertThrows(ConfigException.class, () -> ProcessRole.fromString("unknown"));
        assertEquals("Unknown process role 'unknown' (only 'broker' and 'controller' are allowed roles)", exception.getMessage());
    }
}
