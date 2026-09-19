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

import org.apache.kafka.common.acl.AclBinding;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;


@Timeout(value = 40)
public class StandardAclTest {
    public static final List<StandardAcl> TEST_ACLS = StandardAclFixtures.TEST_ACLS;

    private static int signum(int input) {
        return Integer.compare(input, 0);
    }

    @Test
    public void testCompareTo() {
        assertEquals(1, signum(TEST_ACLS.get(0).compareTo(TEST_ACLS.get(1))));
        assertEquals(-1, signum(TEST_ACLS.get(1).compareTo(TEST_ACLS.get(0))));
        assertEquals(-1, signum(TEST_ACLS.get(2).compareTo(TEST_ACLS.get(3))));
        assertEquals(1, signum(TEST_ACLS.get(4).compareTo(TEST_ACLS.get(3))));
        assertEquals(-1, signum(TEST_ACLS.get(3).compareTo(TEST_ACLS.get(4))));
    }

    @Test
    public void testToBindingRoundTrips() {
        for (StandardAcl acl : TEST_ACLS) {
            AclBinding binding = acl.toBinding();
            StandardAcl acl2 = StandardAcl.fromAclBinding(binding);
            assertEquals(acl2, acl);
        }
    }

    @Test
    public void testEquals() {
        for (int i = 0; i != TEST_ACLS.size(); i++) {
            for (int j = 0; j != TEST_ACLS.size(); j++) {
                if (i == j) {
                    assertEquals(TEST_ACLS.get(i), TEST_ACLS.get(j));
                } else {
                    assertNotEquals(TEST_ACLS.get(i), TEST_ACLS.get(j));
                }
            }
        }
    }
}
