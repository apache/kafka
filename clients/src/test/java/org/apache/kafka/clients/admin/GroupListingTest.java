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

import org.apache.kafka.clients.consumer.internals.ConsumerProtocol;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.GroupType;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class GroupListingTest {

    private static final String GROUP_ID = "mygroup";

    @Test
    public void testSimpleConsumerGroup() {
        GroupListing gl = new GroupListing(GROUP_ID, Optional.of(GroupType.CLASSIC), "", Optional.of(GroupState.EMPTY));
        assertTrue(gl.isSimpleConsumerGroup());

        gl = new GroupListing(GROUP_ID, Optional.of(GroupType.CLASSIC), ConsumerProtocol.PROTOCOL_TYPE, Optional.of(GroupState.STABLE));
        assertFalse(gl.isSimpleConsumerGroup());

        gl = new GroupListing(GROUP_ID, Optional.of(GroupType.CONSUMER), "", Optional.of(GroupState.EMPTY));
        assertFalse(gl.isSimpleConsumerGroup());

        gl = new GroupListing(GROUP_ID, Optional.empty(), "", Optional.empty());
        assertFalse(gl.isSimpleConsumerGroup());
    }
}
