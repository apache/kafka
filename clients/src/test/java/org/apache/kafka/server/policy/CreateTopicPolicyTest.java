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
package org.apache.kafka.server.policy;

import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.server.policy.CreateTopicPolicy.RequestMetadata;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CreateTopicPolicyTest {

    @Test
    public void testPrincipalDefaultsToEmpty() {
        RequestMetadata requestMetadata = new RequestMetadata(
            "topic", 1, (short) 1, null, Map.of());
        assertEquals(Optional.empty(), requestMetadata.principal());
    }

    @Test
    public void testPrincipalIsExposed() {
        KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "alice");
        RequestMetadata requestMetadata = new RequestMetadata(
            "topic", 1, (short) 1, null, Map.of(), principal);
        assertEquals(Optional.of(principal), requestMetadata.principal());
    }

    @Test
    public void testPrincipalExcludedFromEqualsAndHashCode() {
        RequestMetadata withoutPrincipal = new RequestMetadata(
            "topic", 1, (short) 1, null, Map.of());
        RequestMetadata withPrincipal = new RequestMetadata(
            "topic", 1, (short) 1, null, Map.of(),
            new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "alice"));
        // The principal is request-scoped metadata and must not affect equality of the requested topic.
        assertEquals(withoutPrincipal, withPrincipal);
        assertEquals(withoutPrincipal.hashCode(), withPrincipal.hashCode());
    }

    @Test
    public void testToStringIncludesPrincipal() {
        RequestMetadata requestMetadata = new RequestMetadata(
            "topic", 1, (short) 1, null, Map.of(),
            new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "alice"));
        assertTrue(requestMetadata.toString().contains("principal=User:alice"),
            "Expected toString to contain the principal, but was: " + requestMetadata);
    }
}
