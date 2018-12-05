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
package org.apache.kafka.common.protocol;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.EnumSet;

public class ProtoUtilsTest {
    @Test
    public void testRequestDelayedAllocationSchemaDetection() throws Exception {
        //verifies that request schemas known to retain a reference to the underlying byte buffer are correctly detected.
        EnumSet<ApiKeys> requestRequiresDelayedAllocation = EnumSet.of(
                ApiKeys.PRODUCE,
                ApiKeys.JOIN_GROUP,
                ApiKeys.SYNC_GROUP,
                ApiKeys.SASL_AUTHENTICATE,
                ApiKeys.EXPIRE_DELEGATION_TOKEN,
                ApiKeys.RENEW_DELEGATION_TOKEN);
        for (ApiKeys key : ApiKeys.values()) {
            if (requestRequiresDelayedAllocation.contains(key)) {
                assertTrue(key + " should require delayed allocation", key.requestRequiresDelayedAllocation);
            } else {
                assertFalse(key + " should not require delayed allocation", key.requestRequiresDelayedAllocation);
            }
        }
    }

    @Test
    public void testResponseDelayedAllocationSchemaDetection() throws Exception {
        //verifies that response schemas known to retain a reference to the underlying byte buffer are correctly detected.
        EnumSet<ApiKeys> responseRequiresDelayedAllocation = EnumSet.of(
                ApiKeys.FETCH,
                ApiKeys.JOIN_GROUP,
                ApiKeys.SYNC_GROUP,
                ApiKeys.DESCRIBE_GROUPS,
                ApiKeys.SASL_AUTHENTICATE,
                ApiKeys.CREATE_DELEGATION_TOKEN,
                ApiKeys.DESCRIBE_DELEGATION_TOKEN);
        for (ApiKeys key : ApiKeys.values()) {
            if (responseRequiresDelayedAllocation.contains(key)) {
                assertTrue(key + " should require delayed allocation", key.responseRequiresDelayedAllocation);
            } else {
                assertFalse(key + " should not require delayed allocation", key.responseRequiresDelayedAllocation);
            }
        }
    }
}
