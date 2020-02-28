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

import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertTrue;

public class RemoveMembersFromConsumerGroupOptionsTest {

    @Test
    public void testConstructor() {
        MemberToRemove staticMember1 = new MemberToRemove().withGroupInstanceId("instance-1");
        MemberToRemove staticMember2 = new MemberToRemove().withGroupInstanceId("instance-2").withMemberId("member-2");
        MemberToRemove dynamicMember = new MemberToRemove().withMemberId("member-1");
        RemoveMembersFromConsumerGroupOptions options = new RemoveMembersFromConsumerGroupOptions(
                Arrays.asList(staticMember1, dynamicMember, staticMember2));

        assertTrue(options.members().contains(staticMember1));
        assertTrue(options.members().contains(dynamicMember));
        assertTrue(options.members().contains(staticMember2));
    }
}
