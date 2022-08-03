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

package org.apache.kafka.metadata;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Timeout(40)
public class BrokerRegistrationInControlledShutdownChangeTest {

    @Test
    public void testValues() {
        assertEquals((byte) 0, BrokerRegistrationInControlledShutdownChange.NONE.value());
        assertEquals((byte) 1, BrokerRegistrationInControlledShutdownChange.IN_CONTROLLED_SHUTDOWN.value());
    }

    @Test
    public void testAsBoolean() {
        assertEquals(Optional.empty(), BrokerRegistrationInControlledShutdownChange.NONE.asBoolean());
        assertEquals(Optional.of(true), BrokerRegistrationInControlledShutdownChange.IN_CONTROLLED_SHUTDOWN.asBoolean());
    }

    @Test
    public void testValueRoundTrip() {
        for (BrokerRegistrationInControlledShutdownChange change : BrokerRegistrationInControlledShutdownChange.values()) {
            assertEquals(Optional.of(change), BrokerRegistrationInControlledShutdownChange.fromValue(change.value()));
        }
    }
}
