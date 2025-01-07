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
import org.apache.kafka.common.metadata.AccessControlEntryRecord;
import org.apache.kafka.common.metadata.RemoveAccessControlEntryRecord;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.utils.LogContext;
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
import org.apache.kafka.timeline.SnapshotRegistry;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
            new AccessControlEntry("User:*", "*", ALTER, ALLOW)));
        assertEquals("Invalid patternType UNKNOWN",
            assertThrows(InvalidRequestException.class, () ->
                AclControlManager.validateNewAcl(new AclBinding(
                    new ResourcePattern(TOPIC, "*", PatternType.UNKNOWN),
                    new AccessControlEntry("User:*", "*", ALTER, ALLOW)))).
                getMessage());
        assertEquals("Invalid resourceType UNKNOWN",
            assertThrows(InvalidRequestException.class, () ->
                AclControlManager.validateNewAcl(new AclBinding(
                    new ResourcePattern(ResourceType.UNKNOWN, "*", LITERAL),
                    new AccessControlEntry("User:*", "*", ALTER, ALLOW)))).
                getMessage());
        assertEquals("Invalid operation UNKNOWN",
            assertThrows(InvalidRequestException.class, () ->
                AclControlManager.validateNewAcl(new AclBinding(
                    new ResourcePattern(TOPIC, "*", LITERAL),
                    new AccessControlEntry("User:*", "*", AclOperation.UNKNOWN, ALLOW)))).
                getMessage());
        assertEquals("Invalid permissionType UNKNOWN",
            assertThrows(InvalidRequestException.class, () ->
                AclControlManager.validateNewAcl(new AclBinding(
                    new ResourcePattern(TOPIC, "*", LITERAL),
                    new AccessControlEntry("User:*", "*", ALTER, AclPermissionType.UNKNOWN)))).
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
                    new AccessControlEntry("invalid", "*", ALTER, ALLOW)))).
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
        Map<Uuid, StandardAcl> acls = Collections.emptyMap();

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
            manager.deleteAcls(Arrays.asList(
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
        assertEquals(new HashSet<>(Arrays.asList(
            TEST_ACLS.get(0).toBinding(),
                TEST_ACLS.get(2).toBinding())), deleted);
        assertEquals(InvalidRequestException.class,
            deleteResult.response().get(1).exception().get().getClass());
        RecordTestUtils.replayAll(manager, deleteResult.records());

        Iterator<Map.Entry<Uuid, StandardAcl>> iterator = manager.idToAcl().entrySet().iterator();
        assertEquals(TEST_ACLS.get(1).acl(), iterator.next().getValue());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testDeleteDedupe() {
        AclControlManager manager = new AclControlManager.Builder().build();
        MockClusterMetadataAuthorizer authorizer = new MockClusterMetadataAuthorizer();
        authorizer.loadSnapshot(manager.idToAcl());

        AclBinding aclBinding = new AclBinding(new ResourcePattern(TOPIC, "topic-1", LITERAL),
                new AccessControlEntry("User:user", "10.0.0.1", AclOperation.ALL, ALLOW));

        ControllerResult<List<AclCreateResult>> createResult = manager.createAcls(Collections.singletonList(aclBinding));
        Uuid id = ((AccessControlEntryRecord) createResult.records().get(0).message()).id();
        assertEquals(1, createResult.records().size());

        ControllerResult<List<AclDeleteResult>> deleteAclResultsAnyFilter = manager.deleteAcls(Collections.singletonList(AclBindingFilter.ANY));
        assertEquals(1, deleteAclResultsAnyFilter.records().size());
        assertEquals(id, ((RemoveAccessControlEntryRecord) deleteAclResultsAnyFilter.records().get(0).message()).id());
        assertEquals(1, deleteAclResultsAnyFilter.response().size());

        ControllerResult<List<AclDeleteResult>> deleteAclResultsSpecificFilter = manager.deleteAcls(Collections.singletonList(aclBinding.toFilter()));
        assertEquals(1, deleteAclResultsSpecificFilter.records().size());
        assertEquals(id, ((RemoveAccessControlEntryRecord) deleteAclResultsSpecificFilter.records().get(0).message()).id());
        assertEquals(1, deleteAclResultsSpecificFilter.response().size());

        ControllerResult<List<AclDeleteResult>> deleteAclResultsBothFilters = manager.deleteAcls(Arrays.asList(AclBindingFilter.ANY, aclBinding.toFilter()));
        assertEquals(1, deleteAclResultsBothFilters.records().size());
        assertEquals(id, ((RemoveAccessControlEntryRecord) deleteAclResultsBothFilters.records().get(0).message()).id());
        assertEquals(2, deleteAclResultsBothFilters.response().size());
    }
}
