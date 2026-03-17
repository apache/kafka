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

package org.apache.kafka.controller;

import org.apache.kafka.clients.admin.FeatureUpdate;
import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.NotControllerException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.common.metadata.AccessControlEntryRecord;
import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.common.metadata.RemoveAccessControlEntryRecord;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.test.api.Flaky;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.metadata.RecordTestUtils;
import org.apache.kafka.metadata.authorizer.AclMutator;
import org.apache.kafka.metadata.authorizer.ClusterMetadataAuthorizer;
import org.apache.kafka.metadata.authorizer.StandardAcl;
import org.apache.kafka.metadata.authorizer.StandardAclTest;
import org.apache.kafka.metadata.authorizer.StandardAclWithId;
import org.apache.kafka.metadata.authorizer.StandardAclWithIdTest;
import org.apache.kafka.server.authorizer.AclCreateResult;
import org.apache.kafka.server.authorizer.AclDeleteResult;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.apache.kafka.server.authorizer.AuthorizerServerInfo;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.mutable.BoundedListTooLongException;
import org.apache.kafka.timeline.SnapshotRegistry;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import static org.apache.kafka.common.acl.AclOperation.ALTER;
import static org.apache.kafka.common.acl.AclPermissionType.ALLOW;
import static org.apache.kafka.common.resource.PatternType.LITERAL;
import static org.apache.kafka.common.resource.PatternType.MATCH;
import static org.apache.kafka.common.resource.ResourceType.TOPIC;
import static org.apache.kafka.controller.QuorumController.MAX_RECORDS_PER_USER_OP;
import static org.apache.kafka.metadata.authorizer.StandardAclWithIdTest.TEST_ACLS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Timeout(value = 40)
public class AclControlManagerTest {
    /**
     * Verify that validateNewAcl catches invalid ACLs.
     */
    @Test
    public void testValidateNewAcl() {
        AclControlManager manager = new AclControlManager.Builder().build();
        manager.validateNewAcl(new AclBinding(
            new ResourcePattern(TOPIC, "*", LITERAL),
            new AccessControlEntry("User:*", "*", ALTER, ALLOW)));
        assertEquals("Invalid patternType UNKNOWN",
            assertThrows(InvalidRequestException.class, () ->
                manager.validateNewAcl(new AclBinding(
                    new ResourcePattern(TOPIC, "*", PatternType.UNKNOWN),
                    new AccessControlEntry("User:*", "*", ALTER, ALLOW)))).
                getMessage());
        assertEquals("Invalid resourceType UNKNOWN",
            assertThrows(InvalidRequestException.class, () ->
                manager.validateNewAcl(new AclBinding(
                    new ResourcePattern(ResourceType.UNKNOWN, "*", LITERAL),
                    new AccessControlEntry("User:*", "*", ALTER, ALLOW)))).
                getMessage());
        assertEquals("Invalid operation UNKNOWN",
            assertThrows(InvalidRequestException.class, () ->
                manager.validateNewAcl(new AclBinding(
                    new ResourcePattern(TOPIC, "*", LITERAL),
                    new AccessControlEntry("User:*", "*", AclOperation.UNKNOWN, ALLOW)))).
                getMessage());
        assertEquals("Invalid permissionType UNKNOWN",
            assertThrows(InvalidRequestException.class, () ->
                manager.validateNewAcl(new AclBinding(
                    new ResourcePattern(TOPIC, "*", LITERAL),
                    new AccessControlEntry("User:*", "*", ALTER, AclPermissionType.UNKNOWN)))).
                getMessage());
    }

    /**
     * Verify that validateNewAcl catches invalid ACLs with principals that do not contain a colon.
     */
    @Test
    public void testValidateAclWithBadPrincipal() {
        AclControlManager manager = new AclControlManager.Builder().build();
        assertEquals("Could not parse principal from `invalid` (no colon is present " +
                "separating the principal type from the principal name)",
            assertThrows(InvalidRequestException.class, () ->
                manager.validateNewAcl(new AclBinding(
                    new ResourcePattern(TOPIC, "*", LITERAL),
                    new AccessControlEntry("invalid", "*", ALTER, ALLOW)))).
                getMessage());
    }

    /**
     * Verify that validateNewAcl catches invalid ACLs with principals that do not contain a colon.
     */
    @Test
    public void testValidateAclWithEmptyPrincipal() {
        AclControlManager manager = new AclControlManager.Builder().build();
        assertEquals("Could not parse principal from `` (no colon is present " +
                "separating the principal type from the principal name)",
            assertThrows(InvalidRequestException.class, () ->
                manager.validateNewAcl(new AclBinding(
                    new ResourcePattern(TOPIC, "*", LITERAL),
                    new AccessControlEntry("", "*", ALTER, ALLOW)))).
                        getMessage());
    }

    /**
     * Verify that validateFilter catches invalid filters.
     */
    @Test
    public void testValidateFilter() {
        AclControlManager.validateFilter(new AclBindingFilter(
            new ResourcePatternFilter(ResourceType.ANY, "*", LITERAL),
            new AccessControlEntryFilter("User:*", "*", AclOperation.ANY, AclPermissionType.ANY)));
        assertEquals("Unknown patternFilter.",
            assertThrows(InvalidRequestException.class, () ->
                AclControlManager.validateFilter(new AclBindingFilter(
                    new ResourcePatternFilter(ResourceType.ANY, "*", PatternType.UNKNOWN),
                    new AccessControlEntryFilter("User:*", "*", AclOperation.ANY, AclPermissionType.ANY)))).
                getMessage());
        assertEquals("Unknown entryFilter.",
            assertThrows(InvalidRequestException.class, () ->
                AclControlManager.validateFilter(new AclBindingFilter(
                    new ResourcePatternFilter(ResourceType.ANY, "*", MATCH),
                    new AccessControlEntryFilter("User:*", "*", AclOperation.ANY, AclPermissionType.UNKNOWN)))).
                getMessage());
    }

    static class MockClusterMetadataAuthorizer implements ClusterMetadataAuthorizer {
        Map<Uuid, StandardAcl> acls = Map.of();

        @Override
        public void setAclMutator(AclMutator aclMutator) {
            // do nothing
        }

        @Override
        public AclMutator aclMutatorOrException() {
            throw new NotControllerException("The current node is not the active controller.");
        }

        @Override
        public void completeInitialLoad() {
            // do nothing
        }

        @Override
        public void completeInitialLoad(Exception e) {
            // do nothing
        }

        @Override
        public void loadSnapshot(Map<Uuid, StandardAcl> acls) {
            this.acls = new HashMap<>(acls);
        }

        @Override
        public void addAcl(Uuid id, StandardAcl acl) {
            // do nothing
        }

        @Override
        public void removeAcl(Uuid id) {
            // do nothing
        }

        @Override
        public Map<Endpoint, ? extends CompletionStage<Void>> start(AuthorizerServerInfo serverInfo) {
            return null; // do nothing
        }

        @Override
        public List<AuthorizationResult> authorize(AuthorizableRequestContext requestContext, List<Action> actions) {
            return null; // do nothing
        }

        @Override
        public Iterable<AclBinding> acls(AclBindingFilter filter) {
            return null; // do nothing
        }

        @Override
        public void close() throws IOException {
            // do nothing
        }

        @Override
        public void configure(Map<String, ?> configs) {
            // do nothing
        }
    }

    @Test
    public void testLoadSnapshot() {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(new LogContext());
        snapshotRegistry.idempotentCreateSnapshot(0);
        AclControlManager manager = new AclControlManager.Builder().
            setSnapshotRegistry(snapshotRegistry).
            build();

        // Load TEST_ACLS into the AclControlManager.
        Set<ApiMessageAndVersion> loadedAcls = new HashSet<>();
        for (StandardAclWithId acl : TEST_ACLS) {
            AccessControlEntryRecord record = acl.toRecord();
            assertTrue(loadedAcls.add(new ApiMessageAndVersion(record, (short) 0)));
            manager.replay(acl.toRecord());
        }

        // Verify that the ACLs stored in the AclControlManager match the ones we expect.
        Set<ApiMessageAndVersion> foundAcls = new HashSet<>();
        for (Map.Entry<Uuid, StandardAcl> entry : manager.idToAcl().entrySet()) {
            foundAcls.add(new ApiMessageAndVersion(
                    new StandardAclWithId(entry.getKey(), entry.getValue()).toRecord(), (short) 0));
        }
        assertEquals(loadedAcls, foundAcls);

        // Once we complete the snapshot load, the ACLs should be reflected in the authorizer.
        MockClusterMetadataAuthorizer authorizer = new MockClusterMetadataAuthorizer();
        authorizer.loadSnapshot(manager.idToAcl());
        assertEquals(new HashSet<>(StandardAclTest.TEST_ACLS), new HashSet<>(authorizer.acls.values()));

        // Test reverting to an empty state and then completing the snapshot load without
        // setting an authorizer. This simulates the case where the user didn't configure
        // a cluster metadata authorizer.
        snapshotRegistry.revertToSnapshot(0);
        authorizer.loadSnapshot(manager.idToAcl());
        assertTrue(manager.idToAcl().isEmpty());
    }

    @Test
    public void testAddAndDelete() {
        AclControlManager manager = new AclControlManager.Builder().build();
        MockClusterMetadataAuthorizer authorizer = new MockClusterMetadataAuthorizer();
        authorizer.loadSnapshot(manager.idToAcl());
        manager.replay(StandardAclWithIdTest.TEST_ACLS.get(0).toRecord());
        manager.replay(new RemoveAccessControlEntryRecord().
            setId(TEST_ACLS.get(0).id()));
        assertTrue(manager.idToAcl().isEmpty());
    }

    @Test
    public void testCreateAclDeleteAcl() {
        AclControlManager manager = new AclControlManager.Builder().build();
        MockClusterMetadataAuthorizer authorizer = new MockClusterMetadataAuthorizer();
        authorizer.loadSnapshot(manager.idToAcl());

        List<AclBinding> toCreate = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            toCreate.add(TEST_ACLS.get(i).toBinding());
        }
        toCreate.add(new AclBinding(
            new ResourcePattern(TOPIC, "*", PatternType.UNKNOWN),
            new AccessControlEntry("User:*", "*", ALTER, ALLOW)));

        ControllerResult<List<AclCreateResult>> createResult = manager.createAcls(toCreate);

        List<AclCreateResult> expectedResults = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            expectedResults.add(AclCreateResult.SUCCESS);
        }
        expectedResults.add(new AclCreateResult(
            new InvalidRequestException("Invalid patternType UNKNOWN")));

        for (int i = 0; i < expectedResults.size(); i++) {
            AclCreateResult expectedResult = expectedResults.get(i);
            if (expectedResult.exception().isPresent()) {
                assertEquals(expectedResult.exception().get().getMessage(),
                    createResult.response().get(i).exception().get().getMessage());
            } else {
                assertFalse(createResult.response().get(i).exception().isPresent());
            }
        }
        RecordTestUtils.replayAll(manager, createResult.records());
        assertFalse(manager.idToAcl().isEmpty());

        ControllerResult<List<AclDeleteResult>> deleteResult =
            manager.deleteAcls(List.of(
                new AclBindingFilter(
                    new ResourcePatternFilter(ResourceType.ANY, null, LITERAL),
                        AccessControlEntryFilter.ANY),
                new AclBindingFilter(
                    new ResourcePatternFilter(ResourceType.UNKNOWN, null, LITERAL),
                        AccessControlEntryFilter.ANY)));
        assertEquals(2, deleteResult.response().size());
        Set<AclBinding> deleted = new HashSet<>();
        for (AclDeleteResult.AclBindingDeleteResult result :
                deleteResult.response().get(0).aclBindingDeleteResults()) {
            assertEquals(Optional.empty(), result.exception());
            deleted.add(result.aclBinding());
        }
        assertEquals(Set.of(
            TEST_ACLS.get(0).toBinding(),
                TEST_ACLS.get(2).toBinding()), deleted);
        assertEquals(InvalidRequestException.class,
            deleteResult.response().get(1).exception().get().getClass());
        RecordTestUtils.replayAll(manager, deleteResult.records());

        Iterator<Map.Entry<Uuid, StandardAcl>> iterator = manager.idToAcl().entrySet().iterator();
        assertEquals(TEST_ACLS.get(1).acl(), iterator.next().getValue());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testCreateDedupe() {
        AclControlManager manager = new AclControlManager.Builder().build();

        AclBinding aclBinding = new AclBinding(new ResourcePattern(TOPIC, "topic-1", LITERAL),
                new AccessControlEntry("User:user", "10.0.0.1", AclOperation.ALL, ALLOW));

        ControllerResult<List<AclCreateResult>> createResult = manager.createAcls(List.of(aclBinding, aclBinding));
        RecordTestUtils.replayAll(manager, createResult.records());
        assertEquals(1, createResult.records().size());
        assertEquals(1, manager.idToAcl().size());

        createResult = manager.createAcls(List.of(aclBinding));
        assertEquals(0, createResult.records().size());
        assertEquals(1, manager.idToAcl().size());
    }

    @Test
    public void testDeleteDedupe() {
        AclControlManager manager = new AclControlManager.Builder().build();

        AclBinding aclBinding = new AclBinding(new ResourcePattern(TOPIC, "topic-1", LITERAL),
                new AccessControlEntry("User:user", "10.0.0.1", AclOperation.ALL, ALLOW));

        ControllerResult<List<AclCreateResult>> createResult = manager.createAcls(List.of(aclBinding));
        RecordTestUtils.replayAll(manager, createResult.records());
        Uuid id = ((AccessControlEntryRecord) createResult.records().get(0).message()).id();
        assertEquals(1, createResult.records().size());

        ControllerResult<List<AclDeleteResult>> deleteAclResultsAnyFilter = manager.deleteAcls(List.of(AclBindingFilter.ANY));
        assertEquals(1, deleteAclResultsAnyFilter.records().size());
        assertEquals(id, ((RemoveAccessControlEntryRecord) deleteAclResultsAnyFilter.records().get(0).message()).id());
        assertEquals(1, deleteAclResultsAnyFilter.response().size());

        ControllerResult<List<AclDeleteResult>> deleteAclResultsSpecificFilter = manager.deleteAcls(List.of(aclBinding.toFilter()));
        assertEquals(1, deleteAclResultsSpecificFilter.records().size());
        assertEquals(id, ((RemoveAccessControlEntryRecord) deleteAclResultsSpecificFilter.records().get(0).message()).id());
        assertEquals(1, deleteAclResultsSpecificFilter.response().size());

        ControllerResult<List<AclDeleteResult>> deleteAclResultsBothFilters = manager.deleteAcls(List.of(AclBindingFilter.ANY, aclBinding.toFilter()));
        assertEquals(1, deleteAclResultsBothFilters.records().size());
        assertEquals(id, ((RemoveAccessControlEntryRecord) deleteAclResultsBothFilters.records().get(0).message()).id());
        assertEquals(2, deleteAclResultsBothFilters.response().size());
    }

    @Flaky("KAFKA-19513")
    @Test
    public void testDeleteExceedsMaxRecords() {
        AclControlManager manager = new AclControlManager.Builder().build();
        MockClusterMetadataAuthorizer authorizer = new MockClusterMetadataAuthorizer();
        authorizer.loadSnapshot(manager.idToAcl());

        List<AclBinding> firstCreate = new ArrayList<>();
        List<AclBinding> secondCreate = new ArrayList<>();

        // create MAX_RECORDS_PER_USER_OP + 2 ACLs
        for (int i = 0; i < MAX_RECORDS_PER_USER_OP + 2; i++) {
            StandardAclWithId acl = new StandardAclWithId(Uuid.randomUuid(),
                new StandardAcl(
                    ResourceType.TOPIC,
                    "mytopic_" + i,
                    PatternType.LITERAL,
                    "User:alice",
                    "127.0.0.1",
                    AclOperation.READ,
                    AclPermissionType.ALLOW));

            // split acl creations between two create requests
            if (i % 2 == 0) {
                firstCreate.add(acl.toBinding());
            } else {
                secondCreate.add(acl.toBinding());
            }
        }
        ControllerResult<List<AclCreateResult>> firstCreateResult = manager.createAcls(firstCreate);
        assertEquals((MAX_RECORDS_PER_USER_OP / 2) + 1, firstCreateResult.response().size());
        for (AclCreateResult result : firstCreateResult.response()) {
            assertTrue(result.exception().isEmpty());
        }

        ControllerResult<List<AclCreateResult>> secondCreateResult = manager.createAcls(secondCreate);
        assertEquals((MAX_RECORDS_PER_USER_OP / 2) + 1, secondCreateResult.response().size());
        for (AclCreateResult result : secondCreateResult.response()) {
            assertTrue(result.exception().isEmpty());
        }

        RecordTestUtils.replayAll(manager, firstCreateResult.records());
        RecordTestUtils.replayAll(manager, secondCreateResult.records());
        assertFalse(manager.idToAcl().isEmpty());

        ArrayList<AclBindingFilter> filters = new ArrayList<>();
        for (int i = 0; i < MAX_RECORDS_PER_USER_OP + 2; i++) {
            filters.add(new AclBindingFilter(
                new ResourcePatternFilter(ResourceType.TOPIC, "mytopic_" + i, PatternType.LITERAL),
                AccessControlEntryFilter.ANY));
        }

        Exception exception = assertThrows(InvalidRequestException.class, () -> manager.deleteAcls(filters));
        assertEquals(BoundedListTooLongException.class, exception.getCause().getClass());
        assertEquals("Cannot remove more than " + MAX_RECORDS_PER_USER_OP + " acls in a single delete operation.", exception.getCause().getMessage());
    }

    @Test
    public void testValidateHostPatternValid() {
        // Wildcard, this works with or without CIDR support
        AclControlManager.validateHostPattern("*", false);
        AclControlManager.validateHostPattern("*", true);

        // Regular IPv4 addresses this works with or without CIDR support
        AclControlManager.validateHostPattern("192.168.1.1", false);
        AclControlManager.validateHostPattern("10.0.0.1", false);
        AclControlManager.validateHostPattern("127.0.0.1", true);

        // Regular IPv6 addresses, this works with or without CIDR support
        AclControlManager.validateHostPattern("2001:db8::1", false);
        AclControlManager.validateHostPattern("::1", false);
        AclControlManager.validateHostPattern("fe80::1", true);

        // Valid IPv4 CIDR notations (require CIDR support)
        AclControlManager.validateHostPattern("192.168.0.0/24", true);
        AclControlManager.validateHostPattern("10.0.0.0/8", true);
        AclControlManager.validateHostPattern("172.16.0.0/16", true);
        AclControlManager.validateHostPattern("192.168.1.1/32", true);
        AclControlManager.validateHostPattern("0.0.0.0/0", true);

        // Valid IPv6 CIDR notations (require CIDR support)
        AclControlManager.validateHostPattern("2001:db8::/32", true);
        AclControlManager.validateHostPattern("2001:db8:abcd::/48", true);
        AclControlManager.validateHostPattern("::1/128", true);
        AclControlManager.validateHostPattern("::/0", true);
    }

    @Test
    public void testValidateHostPatternInvalid() {
        // Null or empty
        assertThrows(InvalidRequestException.class, () ->
            AclControlManager.validateHostPattern(null, true));
        assertThrows(InvalidRequestException.class, () ->
            AclControlManager.validateHostPattern("", true));

        // Invalid IPv4 CIDR, prefix too large
        InvalidRequestException e = assertThrows(InvalidRequestException.class, () ->
            AclControlManager.validateHostPattern("192.168.0.0/33", true));
        assertTrue(e.getMessage().contains("Invalid CIDR notation"));

        // Invalid IPv4 CIDR, negative prefix
        e = assertThrows(InvalidRequestException.class, () ->
            AclControlManager.validateHostPattern("192.168.0.0/-1", true));
        assertTrue(e.getMessage().contains("Invalid CIDR notation"));

        // Invalid IPv4 CIDR, malformed address
        e = assertThrows(InvalidRequestException.class, () ->
            AclControlManager.validateHostPattern("192.168.0.256/24", true));
        assertTrue(e.getMessage().contains("Invalid CIDR notation"));

        // Invalid IPv4 CIDR, non-numeric prefix
        e = assertThrows(InvalidRequestException.class, () ->
            AclControlManager.validateHostPattern("192.168.0.0/abc", true));
        assertTrue(e.getMessage().contains("Invalid CIDR notation"));

        // Invalid IPv6 CIDR, prefix too large
        e = assertThrows(InvalidRequestException.class, () ->
            AclControlManager.validateHostPattern("2001:db8::/129", true));
        assertTrue(e.getMessage().contains("Invalid CIDR notation"));

        // Invalid, just a slash with no prefix
        e = assertThrows(InvalidRequestException.class, () ->
            AclControlManager.validateHostPattern("192.168.0.0/", true));
        assertTrue(e.getMessage().contains("Invalid CIDR notation"));
    }

    @Test
    public void testValidateHostPatternCidrNotSupported() {
        // CIDR patterns should be rejected when cidrSupported is false
        UnsupportedVersionException e = assertThrows(UnsupportedVersionException.class, () ->
            AclControlManager.validateHostPattern("192.168.0.0/24", false));
        assertTrue(e.getMessage().contains("CIDR-based ACL host patterns require metadata version"));

        e = assertThrows(UnsupportedVersionException.class, () ->
            AclControlManager.validateHostPattern("2001:db8::/32", false));
        assertTrue(e.getMessage().contains("CIDR-based ACL host patterns require metadata version"));
    }

    @Test
    public void testValidateCidrNotationIpv4() {
        // Valid patterns
        AclControlManager.validateCidrNotation("192.168.0.0/24");
        AclControlManager.validateCidrNotation("10.0.0.0/8");
        AclControlManager.validateCidrNotation("192.168.1.1/32");
        AclControlManager.validateCidrNotation("0.0.0.0/0");

        // Invalid patterns
        assertThrows(InvalidRequestException.class, () ->
            AclControlManager.validateCidrNotation("192.168.0.0/33"));
        assertThrows(InvalidRequestException.class, () ->
            AclControlManager.validateCidrNotation("not.an.ip/24"));
        assertThrows(InvalidRequestException.class, () ->
            AclControlManager.validateCidrNotation("192.168.0.256/24"));
    }

    @Test
    public void testValidateCidrNotationIpv6() {
        // Valid patterns
        AclControlManager.validateCidrNotation("2001:db8::/32");
        AclControlManager.validateCidrNotation("2001:db8:abcd::/48");
        AclControlManager.validateCidrNotation("::1/128");
        AclControlManager.validateCidrNotation("::/0");
        AclControlManager.validateCidrNotation("fe80::/10");

        // Invalid patterns
        assertThrows(InvalidRequestException.class, () ->
            AclControlManager.validateCidrNotation("2001:db8::/129"));
        assertThrows(InvalidRequestException.class, () ->
            AclControlManager.validateCidrNotation("not:valid:ipv6::/32"));
    }

    private static AclControlManager createManagerWithMetadataVersion(MetadataVersion version) {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(new LogContext());
        FeatureControlManager featureControl = new FeatureControlManager.Builder()
            .setSnapshotRegistry(snapshotRegistry)
            .setQuorumFeatures(new QuorumFeatures(0,
                QuorumFeatures.defaultSupportedFeatureMap(true),
                List.of()))
            .build();
        featureControl.replay(new FeatureLevelRecord()
            .setName(MetadataVersion.FEATURE_NAME)
            .setFeatureLevel(version.featureLevel()));
        return new AclControlManager.Builder()
            .setSnapshotRegistry(snapshotRegistry)
            .setFeatureControl(featureControl)
            .build();
    }

    @Test
    public void testCreateAclWithValidCidrHost() {
        // Use a metadata version that supports CIDR ACLs
        AclControlManager manager = createManagerWithMetadataVersion(MetadataVersion.IBP_4_3_IV0);

        // Create ACL with valid IPv4 CIDR
        AclBinding ipv4CidrAcl = new AclBinding(
            new ResourcePattern(TOPIC, "test-topic", LITERAL),
            new AccessControlEntry("User:test", "192.168.0.0/24", ALTER, ALLOW));

        ControllerResult<List<AclCreateResult>> result = manager.createAcls(List.of(ipv4CidrAcl));
        assertEquals(1, result.response().size());
        assertFalse(result.response().get(0).exception().isPresent());

        // Create ACL with valid IPv6 CIDR
        AclBinding ipv6CidrAcl = new AclBinding(
            new ResourcePattern(TOPIC, "test-topic-2", LITERAL),
            new AccessControlEntry("User:test", "2001:db8::/32", ALTER, ALLOW));

        result = manager.createAcls(List.of(ipv6CidrAcl));
        assertEquals(1, result.response().size());
        assertFalse(result.response().get(0).exception().isPresent());
    }

    @Test
    public void testCreateAclWithInvalidCidrHost() {
        // Use a metadata version that supports CIDR ACLs
        AclControlManager manager = createManagerWithMetadataVersion(MetadataVersion.IBP_4_3_IV0);

        // Create ACL with invalid IPv4 CIDR (prefix too large)
        AclBinding invalidCidrAcl = new AclBinding(
            new ResourcePattern(TOPIC, "test-topic", LITERAL),
            new AccessControlEntry("User:test", "192.168.0.0/33", ALTER, ALLOW));

        ControllerResult<List<AclCreateResult>> result = manager.createAcls(List.of(invalidCidrAcl));
        assertEquals(1, result.response().size());
        assertTrue(result.response().get(0).exception().isPresent());
        assertTrue(result.response().get(0).exception().get().getMessage().contains("Invalid CIDR notation"));
    }

    @Test
    public void testCreateAclWithCidrHostUnsupportedVersion() {
        // Use a metadata version that does not support CIDR ACLs
        AclControlManager manager = createManagerWithMetadataVersion(MetadataVersion.IBP_4_0_IV0);

        // Create ACL with valid CIDR but unsupported metadata version
        AclBinding cidrAcl = new AclBinding(
            new ResourcePattern(TOPIC, "test-topic", LITERAL),
            new AccessControlEntry("User:test", "192.168.0.0/24", ALTER, ALLOW));

        ControllerResult<List<AclCreateResult>> result = manager.createAcls(List.of(cidrAcl));
        assertEquals(1, result.response().size());
        assertTrue(result.response().get(0).exception().isPresent());
        assertTrue(result.response().get(0).exception().get() instanceof UnsupportedVersionException);
        assertTrue(result.response().get(0).exception().get().getMessage().contains("CIDR-based ACL host patterns require metadata version"));
    }

    @Test
    public void testCreateAclWithRegularHostOlderVersion() {
        // Use a metadata version that doesn't support CIDR ACLs
        AclControlManager manager = createManagerWithMetadataVersion(MetadataVersion.IBP_4_0_IV0);

        // Create ACL with regular IP address should still work
        AclBinding regularAcl = new AclBinding(
            new ResourcePattern(TOPIC, "test-topic", LITERAL),
            new AccessControlEntry("User:test", "192.168.0.1", ALTER, ALLOW));

        ControllerResult<List<AclCreateResult>> result = manager.createAcls(List.of(regularAcl));
        assertEquals(1, result.response().size());
        assertFalse(result.response().get(0).exception().isPresent());

        // Create ACL with wildcard should also work
        AclBinding wildcardAcl = new AclBinding(
            new ResourcePattern(TOPIC, "test-topic-2", LITERAL),
            new AccessControlEntry("User:test", "*", ALTER, ALLOW));

        result = manager.createAcls(List.of(wildcardAcl));
        assertEquals(1, result.response().size());
        assertFalse(result.response().get(0).exception().isPresent());
    }

    @Test
    public void testDowngradeBelowCidrVersionWithExistingCidrAcls() {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(new LogContext());
        FeatureControlManager featureControl = new FeatureControlManager.Builder()
            .setSnapshotRegistry(snapshotRegistry)
            .setQuorumFeatures(new QuorumFeatures(0,
                QuorumFeatures.defaultSupportedFeatureMap(true),
                List.of()))
            .build();
        featureControl.replay(new FeatureLevelRecord()
            .setName(MetadataVersion.FEATURE_NAME)
            .setFeatureLevel(MetadataVersion.IBP_4_3_IV0.featureLevel()));
        AclControlManager aclManager = new AclControlManager.Builder()
            .setSnapshotRegistry(snapshotRegistry)
            .setFeatureControl(featureControl)
            .build();

        featureControl.setPreDowngradeValidator(newVersion -> {
            if (!newVersion.isCidrAclSupported() && aclManager.hasCidrAcls()) {
                return Optional.of("Cannot downgrade below " + MetadataVersion.IBP_4_3_IV0 +
                    " while CIDR-based ACL host patterns exist. Remove all CIDR ACLs first.");
            }
            return Optional.empty();
        });

        AclBinding cidrAcl = new AclBinding(
            new ResourcePattern(TOPIC, "test-topic", LITERAL),
            new AccessControlEntry("User:test", "192.168.0.0/24", ALTER, ALLOW));
        ControllerResult<List<AclCreateResult>> createResult = aclManager.createAcls(List.of(cidrAcl));
        assertEquals(1, createResult.response().size());
        assertFalse(createResult.response().get(0).exception().isPresent(),
            "CIDR ACL creation should succeed on IBP_4_3_IV0");

        for (ApiMessageAndVersion record : createResult.records()) {
            aclManager.replay((AccessControlEntryRecord) record.message());
        }

        assertTrue(aclManager.hasCidrAcls(), "hasCidrAcls() should detect CIDR ACL");

        ControllerResult<ApiError> downgradeResult = featureControl.updateFeatures(
            Map.of(MetadataVersion.FEATURE_NAME, MetadataVersion.IBP_4_2_IV1.featureLevel()),
            Map.of(MetadataVersion.FEATURE_NAME, FeatureUpdate.UpgradeType.SAFE_DOWNGRADE),
            false, 0);
        assertEquals(Errors.INVALID_UPDATE_VERSION, downgradeResult.response().error());
        assertTrue(downgradeResult.response().message().contains("CIDR-based ACL host patterns exist"),
            "Downgrade should be blocked with CIDR ACL error message");

        ControllerResult<List<AclDeleteResult>> deleteResult = aclManager.deleteAcls(
            List.of(cidrAcl.toFilter()));
        for (ApiMessageAndVersion record : deleteResult.records()) {
            aclManager.replay((RemoveAccessControlEntryRecord) record.message());
        }
        assertFalse(aclManager.hasCidrAcls(), "hasCidrAcls() should return false after removal");

        downgradeResult = featureControl.updateFeatures(
            Map.of(MetadataVersion.FEATURE_NAME, MetadataVersion.IBP_4_2_IV1.featureLevel()),
            Map.of(MetadataVersion.FEATURE_NAME, FeatureUpdate.UpgradeType.SAFE_DOWNGRADE),
            false, 0);
        assertEquals(Errors.NONE, downgradeResult.response().error(),
            "Downgrade should succeed after CIDR ACLs are removed");
    }
}
