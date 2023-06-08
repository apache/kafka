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

package org.apache.kafka.metadata.migration;

import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.resource.ResourcePattern;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;

public class CapturingAclMigrationClient implements AclMigrationClient {

    public List<ResourcePattern> deletedResources = new ArrayList<>();
    public LinkedHashMap<ResourcePattern, Collection<AccessControlEntry>> updatedResources = new LinkedHashMap<>();

    public void reset() {
        deletedResources.clear();
        updatedResources.clear();
    }

    @Override
    public ZkMigrationLeadershipState deleteResource(ResourcePattern resourcePattern, ZkMigrationLeadershipState state) {
        deletedResources.add(resourcePattern);
        return state;
    }

    @Override
    public ZkMigrationLeadershipState writeResourceAcls(ResourcePattern resourcePattern, Collection<AccessControlEntry> aclsToWrite, ZkMigrationLeadershipState state) {
        updatedResources.put(resourcePattern, aclsToWrite);
        return state;
    }

    @Override
    public void iterateAcls(BiConsumer<ResourcePattern, Set<AccessControlEntry>> aclConsumer) {

    }
}
