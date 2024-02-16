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

package org.apache.kafka.clients.admin;

import org.apache.kafka.common.ConsumerGroupState;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConsumerGroupListingTest {

    @Test
    void testEquals() {
        ConsumerGroupListing consumerGroupSTABLE = new ConsumerGroupListing("test", true, Optional.of(ConsumerGroupState.STABLE));
        ConsumerGroupListing consumerGroupDEADAnother = new ConsumerGroupListing("test", true, Optional.of(ConsumerGroupState.DEAD));
        ConsumerGroupListing consumerGroupSTABLEAnotherGroup = new ConsumerGroupListing("test2", true, Optional.of(ConsumerGroupState.STABLE));
        ConsumerGroupListing consumerGroupEMPTY = new ConsumerGroupListing("", true);
        ConsumerGroupListing consumerGroupEMPTYAnother = new ConsumerGroupListing("", true);

        assertFalse(consumerGroupEMPTY.equals(consumerGroupSTABLE));
        assertFalse(consumerGroupSTABLE.equals(consumerGroupEMPTY));

        assertTrue(consumerGroupSTABLE.equals(consumerGroupSTABLE));
        assertTrue(consumerGroupSTABLE.equals(consumerGroupSTABLE));

        assertTrue(consumerGroupEMPTY.equals(consumerGroupEMPTYAnother));
        assertFalse(consumerGroupSTABLE.equals(consumerGroupDEADAnother));
        assertFalse(consumerGroupSTABLE.equals(consumerGroupSTABLEAnotherGroup));
    }
}
