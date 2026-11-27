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

package org.apache.kafka.common.requests;

import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.MismatchedEndpointTypeException;
import org.apache.kafka.common.errors.SecurityDisabledException;
import org.apache.kafka.common.errors.UnsupportedEndpointTypeException;
import org.apache.kafka.common.errors.UnsupportedForMessageFormatException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.resource.ResourceType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.EnumSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RequestUtilsTest {
    // Resource types supported by every CreateAcls, DescribeAcls and DeleteAcls version. USER was added in version 3
    // (KIP-373) but has never been validated.
    private static final Set<ResourceType> BASELINE_ACLS_RESOURCE_TYPES = EnumSet.of(ResourceType.UNKNOWN, ResourceType.ANY,
        ResourceType.TOPIC, ResourceType.GROUP, ResourceType.CLUSTER, ResourceType.TRANSACTIONAL_ID, ResourceType.DELEGATION_TOKEN,
        ResourceType.USER);

    @Test
    public void testIsFatalException() {
        assertTrue(RequestUtils.isFatalException(new AuthenticationException("")));
        assertTrue(RequestUtils.isFatalException(new AuthorizationException("")));
        assertTrue(RequestUtils.isFatalException(new MismatchedEndpointTypeException("")));
        assertTrue(RequestUtils.isFatalException(new SecurityDisabledException("")));
        assertTrue(RequestUtils.isFatalException(new UnsupportedEndpointTypeException("")));
        assertTrue(RequestUtils.isFatalException(new UnsupportedForMessageFormatException("")));
        assertTrue(RequestUtils.isFatalException(new UnsupportedVersionException("")));

        // retriable exceptions
        assertFalse(RequestUtils.isFatalException(new DisconnectException("")));
    }

    // Catches a new ResourceType added without deciding which CreateAcls, DescribeAcls and DeleteAcls version
    // supports it, which would otherwise be sent to brokers that do not understand it.
    @ParameterizedTest
    @EnumSource(ResourceType.class)
    public void testEveryResourceTypeIsBaselineOrHasMinAclsVersion(ResourceType resourceType) {
        Short minVersion = RequestUtils.MIN_ACLS_VERSIONS.get(resourceType.code());
        assertTrue(BASELINE_ACLS_RESOURCE_TYPES.contains(resourceType) || minVersion != null,
            resourceType + " is neither a baseline ACL resource type nor in RequestUtils.MIN_ACLS_VERSIONS");
        if (minVersion != null) {
            for (ApiKeys apiKey : new ApiKeys[] {ApiKeys.CREATE_ACLS, ApiKeys.DESCRIBE_ACLS, ApiKeys.DELETE_ACLS}) {
                assertTrue(apiKey.isVersionSupported(minVersion), apiKey + " does not support version " + minVersion);
            }
        }
    }
}
