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

import org.apache.kafka.common.protocol.types.BoundField;
import org.apache.kafka.common.protocol.types.Schema;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ApiKeysTest {

    @Test
    public void testForIdWithInvalidIdLow() {
        assertThrows(IllegalArgumentException.class, () -> ApiKeys.forId(-1));
    }

    @Test
    public void testForIdWithInvalidIdHigh() {
        assertThrows(IllegalArgumentException.class, () -> ApiKeys.forId(10000));
    }

    @Test
    public void testAlterIsrIsClusterAction() {
        assertTrue(ApiKeys.ALTER_ISR.clusterAction);
    }

    /**
     * All valid client responses which may be throttled should have a field named
     * 'throttle_time_ms' to return the throttle time to the client. Exclusions are
     * <ul>
     *   <li> Cluster actions used only for inter-broker are throttled only if unauthorized
     *   <li> SASL_HANDSHAKE and SASL_AUTHENTICATE are not throttled when used for authentication
     *        when a connection is established or for re-authentication thereafter; these requests
     *        return an error response that may be throttled if they are sent otherwise.
     * </ul>
     */
    @Test
    public void testResponseThrottleTime() {
        Set<ApiKeys> authenticationKeys = EnumSet.of(ApiKeys.SASL_HANDSHAKE, ApiKeys.SASL_AUTHENTICATE);
        // Newer protocol apis include throttle time ms even for cluster actions
        Set<ApiKeys> clusterActionsWithThrottleTimeMs = EnumSet.of(ApiKeys.ALTER_ISR);
        for (ApiKeys apiKey: ApiKeys.zkBrokerApis()) {
            Schema responseSchema = apiKey.messageType.responseSchemas()[apiKey.latestVersion()];
            BoundField throttleTimeField = responseSchema.get("throttle_time_ms");
            if ((apiKey.clusterAction && !clusterActionsWithThrottleTimeMs.contains(apiKey))
                || authenticationKeys.contains(apiKey))
                assertNull(throttleTimeField, "Unexpected throttle time field: " + apiKey);
            else
                assertNotNull(throttleTimeField, "Throttle time field missing: " + apiKey);
        }
    }

    @Test
    public void testApiScope() {
        Set<ApiKeys> apisMissingScope = new HashSet<>();
        for (ApiKeys apiKey : ApiKeys.values()) {
            if (apiKey.messageType.listeners().isEmpty()) {
                apisMissingScope.add(apiKey);
            }
        }
        assertEquals(Collections.emptySet(), apisMissingScope,
            "Found some APIs missing scope definition");
    }

}
