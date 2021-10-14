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

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Timeout(value = 40)
public class BrokerStateTest {
    private static final Logger log = LoggerFactory.getLogger(BrokerStateTest.class);

    @Test
    public void testFromValue() {
        for (BrokerState state : BrokerState.values()) {
            BrokerState state2 = BrokerState.fromValue(state.value());
            assertEquals(state, state2);
        }
    }

    @Test
    public void testUnknownValues() {
        assertEquals(BrokerState.UNKNOWN, BrokerState.fromValue((byte) 126));
    }
}
