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

package org.apache.kafka.metadata.authorizer;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.AccessControlEntryRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Arrays;
import java.util.List;

import static org.apache.kafka.metadata.authorizer.StandardAuthorizerTestConstants.ALL;
import static org.apache.kafka.metadata.authorizer.StandardAuthorizerTestConstants.WILDCARD_ACLS;
import static org.apache.kafka.metadata.authorizer.StandardAuthorizerTestConstants.withIds;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;


@Timeout(value = 40)
public class StandardAclWithIdTest {
    @Test
    public void testToRecordRoundTrips() {
        withIds(ALL).forEach(acl -> {
            AccessControlEntryRecord record = acl.toRecord();
            StandardAclWithId acl2 = StandardAclWithId.fromRecord(record);
            assertEquals(acl2, acl);
        });
    }

    @Test
    public void testEquals() {
        List<StandardAclWithId> allWithId = withIds(ALL);
        allWithId.forEach(acl1 -> {
            allWithId.forEach(acl2 -> {
                if (acl1 == acl2) {
                    assertEquals(acl1, acl2);
                } else {
                    assertNotEquals(acl1, acl2);
                }
            });
        });
    }

    static final List<Uuid> TEST_UUIDS = Arrays.asList(
        Uuid.fromString("QZDDv-R7SyaPgetDPGd0Mw"),
        Uuid.fromString("SdDjEdlbRmy2__WFKe3RMg"));

    @Test
    public void testNotEqualsIfIdIsDifferent() {
        assertNotEquals(new StandardAclWithId(TEST_UUIDS.get(0), WILDCARD_ACLS.get(0)),
            new StandardAclWithId(TEST_UUIDS.get(1), WILDCARD_ACLS.get(0)));
    }

    @Test
    public void testNotEqualsIfAclIsDifferent() {
        assertNotEquals(new StandardAclWithId(TEST_UUIDS.get(0), WILDCARD_ACLS.get(0)),
                new StandardAclWithId(TEST_UUIDS.get(0), WILDCARD_ACLS.get(1)));
    }
}
