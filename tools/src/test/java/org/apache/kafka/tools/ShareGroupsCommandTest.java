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
package org.apache.kafka.tools;

import joptsimple.OptionException;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListShareGroupsOptions;
import org.apache.kafka.clients.admin.ListShareGroupsResult;
import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.clients.admin.ShareGroupListing;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.ShareGroupState;
import org.apache.kafka.test.TestUtils;
import org.apache.kafka.tools.ShareGroupsCommand.ShareGroupService;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ShareGroupsCommandTest {

    @Test
    public void testListShareGroups() throws Exception {
        String firstGroup = "first-group";
        String secondGroup = "second-group";
        String bootstrapServer = "localhost:9092";

        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServer, "--list"};
        Admin adminClient = mock(KafkaAdminClient.class);
        ListShareGroupsResult result = mock(ListShareGroupsResult.class);
        when(result.all()).thenReturn(KafkaFuture.completedFuture(Arrays.asList(
                new ShareGroupListing(firstGroup, Optional.of(ShareGroupState.STABLE)),
                new ShareGroupListing(secondGroup, Optional.of(ShareGroupState.EMPTY))
        )));
        when(adminClient.listShareGroups(any(ListShareGroupsOptions.class))).thenReturn(result);
        ShareGroupService service = getShareGroupService(cgcArgs, adminClient);
        Set<String> expectedGroups = new HashSet<>(Arrays.asList(firstGroup, secondGroup));

        final Set[] foundGroups = new Set[]{Collections.emptySet()};
        TestUtils.waitForCondition(() -> {
            foundGroups[0] = new HashSet<>(service.listShareGroups());
            return Objects.equals(expectedGroups, foundGroups[0]);
        }, "Expected --list to show groups " + expectedGroups + ", but found " + foundGroups[0] + ".");
        service.close();
    }

    @Test
    public void testListWithUnrecognizedNewConsumerOption() {
        String bootstrapServer = "localhost:9092";
        String[] cgcArgs = new String[]{"--new-consumer", "--bootstrap-server", bootstrapServer, "--list"};
        assertThrows(OptionException.class, () -> getShareGroupService(cgcArgs, new MockAdminClient()));
    }

    @Test
    public void testListShareGroupsWithStates() throws Exception {
        String firstGroup = "first-group";
        String secondGroup = "second-group";
        String bootstrapServer = "localhost:9092";

        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServer, "--list", "--state"};
        Admin adminClient = mock(KafkaAdminClient.class);
        ListShareGroupsResult resultWithAllStates = mock(ListShareGroupsResult.class);
        when(resultWithAllStates.all()).thenReturn(KafkaFuture.completedFuture(Arrays.asList(
                new ShareGroupListing(firstGroup, Optional.of(ShareGroupState.STABLE)),
                new ShareGroupListing(secondGroup, Optional.of(ShareGroupState.EMPTY))
        )));
        when(adminClient.listShareGroups(any(ListShareGroupsOptions.class))).thenReturn(resultWithAllStates);
        ShareGroupService service = getShareGroupService(cgcArgs, adminClient);
        Set<ShareGroupListing> expectedListing = new HashSet<>(Arrays.asList(
                new ShareGroupListing(firstGroup, Optional.of(ShareGroupState.STABLE)),
                new ShareGroupListing(secondGroup, Optional.of(ShareGroupState.EMPTY))));

        final Set[] foundListing = new Set[]{Collections.emptySet()};
        TestUtils.waitForCondition(() -> {
            foundListing[0] = new HashSet<>(service.listShareGroupsWithState(new HashSet<>(Arrays.asList(ShareGroupState.values()))));
            return Objects.equals(expectedListing, foundListing[0]);
        }, "Expected to show groups " + expectedListing + ", but found " + foundListing[0]);

        ListShareGroupsResult resultWithStableState = mock(ListShareGroupsResult.class);
        when(resultWithStableState.all()).thenReturn(KafkaFuture.completedFuture(Arrays.asList(
                new ShareGroupListing(firstGroup, Optional.of(ShareGroupState.STABLE))
        )));
        when(adminClient.listShareGroups(any(ListShareGroupsOptions.class))).thenReturn(resultWithStableState);
        Set<ShareGroupListing> expectedListingStable = Collections.singleton(
                new ShareGroupListing(firstGroup, Optional.of(ShareGroupState.STABLE)));

        foundListing[0] = Collections.emptySet();

        TestUtils.waitForCondition(() -> {
            foundListing[0] = new HashSet<>(service.listShareGroupsWithState(Collections.singleton(ShareGroupState.STABLE)));
            return Objects.equals(expectedListingStable, foundListing[0]);
        }, "Expected to show groups " + expectedListingStable + ", but found " + foundListing[0]);
        service.close();
    }

    @Test
    public void testShareGroupStatesFromString() {
        Set<ShareGroupState> result = ShareGroupsCommand.shareGroupStatesFromString("Stable");
        assertEquals(Collections.singleton(ShareGroupState.STABLE), result);

        result = ShareGroupsCommand.shareGroupStatesFromString("stable");
        assertEquals(new HashSet<>(Arrays.asList(ShareGroupState.STABLE)), result);

        result = ShareGroupsCommand.shareGroupStatesFromString("dead");
        assertEquals(new HashSet<>(Arrays.asList(ShareGroupState.DEAD)), result);

        result = ShareGroupsCommand.shareGroupStatesFromString("empty");
        assertEquals(new HashSet<>(Arrays.asList(ShareGroupState.EMPTY)), result);

        assertThrows(IllegalArgumentException.class, () -> ShareGroupsCommand.shareGroupStatesFromString("bad, wrong"));

        assertThrows(IllegalArgumentException.class, () -> ShareGroupsCommand.shareGroupStatesFromString("  bad, Stable"));

        assertThrows(IllegalArgumentException.class, () -> ShareGroupsCommand.shareGroupStatesFromString("   ,   ,"));
    }

    ShareGroupService getShareGroupService(String[] args, Admin adminClient) {
        ShareGroupCommandOptions opts = new ShareGroupCommandOptions(args);
        ShareGroupService service = new ShareGroupService(
                opts,
                Collections.singletonMap(AdminClientConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE)),
                adminClient
        );
        return service;
    }
}
