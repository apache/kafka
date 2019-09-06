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

import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.message.LeaveGroupResponseData;
import org.apache.kafka.common.requests.LeaveGroupResponse;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class MembershipChangeResultTest {

    @Test
    public void testConstructor() {
        KafkaFutureImpl<RemoveMemberFromGroupResult> removeMemberFuture = new KafkaFutureImpl<>();

        MembershipChangeResult changeResult = new MembershipChangeResult(removeMemberFuture);
        assertEquals(removeMemberFuture, changeResult.future());
        RemoveMemberFromGroupResult removeMemberFromGroupResult = new RemoveMemberFromGroupResult(
            new LeaveGroupResponse(new LeaveGroupResponseData()),
            Collections.emptyList()
        );

        removeMemberFuture.complete(removeMemberFromGroupResult);
        try {
            assertEquals(removeMemberFromGroupResult, changeResult.all());
        } catch (ExecutionException | InterruptedException e) {
            fail("Unexpected exception " + e + " when trying to get remove member result");
        }
    }
}
