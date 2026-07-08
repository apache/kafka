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
import org.apache.kafka.common.metadata.AccessControlEntryRecord;
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
import org.apache.kafka.server.common.MetadataVersion;
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
        AclControlManager.validateNewAcl(new AclBinding(
            new ResourcePattern(TOPIC, "*", LITERAL),
            new AccessControlEntry("User:*", "*", ALTER, ALLOW)), false);
        assertEquals("Invalid patternType UNKNOWN",
            assertThrows(InvalidRequestException.class, () ->
                AclControlManager.validateNewAcl(new AclBinding(
                    new ResourcePattern(TOPIC, "*", PatternType.UNKNOWN),
                    new AccessControlEntry("User:*", "*", ALTER, ALLOW)), false)).
                getMessage());
        assertEquals("Invalid resourceType UNKNOWN",
            assertThrows(InvalidRequestException.class, () ->
                AclControlManager.validateNewAcl(new AclBinding(
                    new ResourcePattern(ResourceType.UNKNOWN, "*", LITERAL),
                    new AccessControlEntry("User:*", "*", ALTER, ALLOW)), false)).
                getMessage());
        assertEquals("Invalid operation UNKNOWN",
            assertThrows(InvalidRequestException.class, () ->
                AclControlManager.validateNewAcl(new AclBinding(
                    new ResourcePattern(TOPIC, "*", LITERAL),
                    new AccessControlEntry("User:*", "*", AclOperation.UNKNOWN, ALLOW)), false)).
                getMessage());
        assertEquals("Invalid permissionType UNKNOWN",
            assertThrows(InvalidRequestException.class, () ->
                AclControlManager.validateNewAcl(new AclBinding(
                    new ResourcePattern(TOPIC, "*", LITERAL),
                    new AccessControlEntry("User:*", "*", ALTER, AclPermissionType.UNKNOWN)), false)).
                getMessage());
    }

    /**
     * Verify that validateNewAcl catches invalid ACLs with principals that do not contain a colon.
     */
    @Test
    public void testValidateAclWithBadPrincipal() {
        assertEquals("Could not parse principal from `invalid` (no colon is present " +
                "separating the principal type from the principal name)",
            assertThrows(InvalidRequestException.class, () ->
                AclControlManager.validateNewAcl(new AclBinding(
                    new ResourcePattern(TOPIC, "*", LITERAL),
                    new AccessControlEntry("invalid", "*", ALTER, ALLOW)), false)).
                getMessage());
    }

    /**
     * Verify that validateNewAcl catches invalid ACLs with principals that do not contain a colon.
     */
    @Test
    public void testValidateAclWithEmptyPrincipal() {
        assertEquals("Could not parse principal from `` (no colon is present " +
                "separating the principal type from the principal name)",
            assertThrows(InvalidRequestException.class, () ->
                AclControlManager.validateNewAcl(new AclBinding(
                    new ResourcePattern(TOPIC, "*", LITERAL),
                    new AccessControlEntry("", "*", ALTER, ALLOW)), false)).
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

        ControllerResult<List<AclCreateResult>> createResult = manager.createAcls(toCreate, MetadataVersion.IBP_4_0_IV0);

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

        ControllerResult<List<AclCreateResult>> createResult = manager.createAcls(List.of(aclBinding, aclBinding), MetadataVersion.IBP_4_0_IV0);
        RecordTestUtils.replayAll(manager, createResult.records());
        assertEquals(1, createResult.records().size());
        assertEquals(1, manager.idToAcl().size());

        createResult = manager.createAcls(List.of(aclBinding), MetadataVersion.IBP_4_0_IV0);
        assertEquals(0, createResult.records().size());
        assertEquals(1, manager.idToAcl().size());
    }

    @Test
    public void testDeleteDedupe() {
        AclControlManager manager = new AclControlManager.Builder().build();

        AclBinding aclBinding = new AclBinding(new ResourcePattern(TOPIC, "topic-1", LITERAL),
                new AccessControlEntry("User:user", "10.0.0.1", AclOperation.ALL, ALLOW));

        ControllerResult<List<AclCreateResult>> createResult = manager.createAcls(List.of(aclBinding), MetadataVersion.IBP_4_0_IV0);
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
        ControllerResult<List<AclCreateResult>> firstCreateResult = manager.createAcls(firstCreate, MetadataVersion.IBP_4_0_IV0);
        assertEquals((MAX_RECORDS_PER_USER_OP / 2) + 1, firstCreateResult.response().size());
        for (AclCreateResult result : firstCreateResult.response()) {
            assertTrue(result.exception().isEmpty());
        }

        ControllerResult<List<AclCreateResult>> secondCreateResult = manager.createAcls(secondCreate, MetadataVersion.IBP_4_0_IV0);
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
        // Wildcard
        AclControlManager.validateHostPattern("*", false);
        AclControlManager.validateHostPattern("*", true);

        // Plain IPv4 addresses
        AclControlManager.validateHostPattern("192.168.1.1", false);
        AclControlManager.validateHostPattern("127.0.0.1", true);

        // Plain IPv6 addresses
        AclControlManager.validateHostPattern("2001:db8::1", false);
        AclControlManager.validateHostPattern("::1", true);

        // Hostnames (no slash, so validateHostPattern accepts them as literal hosts)
        AclControlManager.validateHostPattern("example.com", false);
        AclControlManager.validateHostPattern("example.com", true);
    }

    @Test
    public void testValidateHostPatternInvalid() {
        // Null or empty
        assertThrows(InvalidRequestException.class, () ->
            AclControlManager.validateHostPattern(null, true));
        assertThrows(InvalidRequestException.class, () ->
            AclControlManager.validateHostPattern("", true));

        // Invalid CIDR wraps CidrUtils.validate() IllegalArgumentException into InvalidRequestException
        InvalidRequestException e = assertThrows(InvalidRequestException.class, () ->
            AclControlManager.validateHostPattern("192.168.0.0/33", true));
        assertTrue(e.getMessage().contains("Invalid CIDR notation"));

        // Hostname with path is not a valid CIDR
        e = assertThrows(InvalidRequestException.class, () ->
            AclControlManager.validateHostPattern("example.com/test", true));
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
    public void testCreateAclWithCidrHosts() {
        AclControlManager manager = new AclControlManager.Builder().build();

        // Valid CIDR with supported version
        assertAclCreateSucceeds(manager, "192.168.0.0/24", MetadataVersion.IBP_4_4_IV1);
        assertAclCreateSucceeds(manager, "2001:db8::/32", MetadataVersion.IBP_4_4_IV1);

        // Regular hosts with older version
        assertAclCreateSucceeds(manager, "192.168.0.1", MetadataVersion.IBP_4_0_IV0);
        assertAclCreateSucceeds(manager, "*", MetadataVersion.IBP_4_0_IV0);

        // Invalid CIDR notation
        assertAclCreateFails(manager, "192.168.0.0/33", MetadataVersion.IBP_4_4_IV1,
            InvalidRequestException.class, "Invalid CIDR notation");

        // Valid CIDR with unsupported version
        assertAclCreateFails(manager, "192.168.0.0/24", MetadataVersion.IBP_4_0_IV0,
            UnsupportedVersionException.class, "CIDR-based ACL host patterns require metadata version");
    }

    private static void assertAclCreateSucceeds(AclControlManager manager, String host, MetadataVersion version) {
        AclBinding acl = new AclBinding(
            new ResourcePattern(TOPIC, "topic-" + host, LITERAL),
            new AccessControlEntry("User:test", host, ALTER, ALLOW));
        ControllerResult<List<AclCreateResult>> result = manager.createAcls(List.of(acl), version);
        assertEquals(1, result.response().size());
        assertFalse(result.response().get(0).exception().isPresent(),
            "Expected success for host=" + host + " version=" + version);
    }

    private static void assertAclCreateFails(AclControlManager manager, String host, MetadataVersion version,
                                              Class<? extends Exception> exClass, String messageContains) {
        AclBinding acl = new AclBinding(
            new ResourcePattern(TOPIC, "topic-" + host, LITERAL),
            new AccessControlEntry("User:test", host, ALTER, ALLOW));
        ControllerResult<List<AclCreateResult>> result = manager.createAcls(List.of(acl), version);
        assertEquals(1, result.response().size());
        assertTrue(result.response().get(0).exception().isPresent());
        assertTrue(exClass.isInstance(result.response().get(0).exception().get()));
        assertTrue(result.response().get(0).exception().get().getMessage().contains(messageContains));
    }
}
