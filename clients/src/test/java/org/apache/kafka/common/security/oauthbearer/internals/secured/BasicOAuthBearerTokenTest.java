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

package org.apache.kafka.common.security.oauthbearer.internals.secured;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.junit.jupiter.api.Test;

public class BasicOAuthBearerTokenTest {

    @Test
    public void smoke() {
        OAuthBearerToken token = new BasicOAuthBearerToken("not.valid.token",
            Collections.emptySet(),
            0L,
            "jdoe",
            0L);
        assertEquals("not.valid.token", token.value());
        assertTrue(token.scope().isEmpty());
        assertEquals(0L, token.lifetimeMs());
        assertEquals("jdoe", token.principalName());
        assertEquals(0L, token.startTimeMs());
    }

    @Test
    public void errorIfModifyScope() {
        // Start with a basic set created by the caller.
        SortedSet<String> callerSet = new TreeSet<>(Arrays.asList("a", "b", "c"));
        OAuthBearerToken token = new BasicOAuthBearerToken("not.valid.token",
            callerSet,
            0L,
            "jdoe",
            0L);

        // Make sure it all looks good
        assertNotNull(token.scope());
        assertEquals(3, token.scope().size());

        // Add a value to the caller's set but make sure it doesn't find its way into the token's
        // scope set.
        callerSet.add("d");
        assertFalse(token.scope().contains("d"));

        // Similarly, removing a value from the caller's set shouldn't affect the token's scope set.
        callerSet.remove("c");
        assertTrue(token.scope().contains("c"));

        // Ensure that attempting to change the token's scope set directly will throw the
        // appropriate error.
        assertThrows(UnsupportedOperationException.class, () -> token.scope().clear());
    }

}
