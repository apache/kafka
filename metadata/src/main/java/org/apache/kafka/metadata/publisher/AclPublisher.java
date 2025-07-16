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
package org.apache.kafka.metadata.publisher;

import org.apache.kafka.common.internals.Plugin;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.loader.LoaderManifest;
import org.apache.kafka.image.loader.LoaderManifestType;
import org.apache.kafka.image.publisher.MetadataPublisher;
import org.apache.kafka.metadata.authorizer.ClusterMetadataAuthorizer;
import org.apache.kafka.server.authorizer.Authorizer;
import org.apache.kafka.server.fault.FaultHandler;

import org.slf4j.Logger;

import java.util.Optional;
import java.util.concurrent.TimeoutException;

public class AclPublisher implements MetadataPublisher {
    private final Logger log;
    private final int nodeId;
    private final FaultHandler faultHandler;
    private final String nodeType;
    private final Optional<Plugin<Authorizer>> authorizer;
    private boolean completedInitialLoad = false;

    public AclPublisher(int nodeId, FaultHandler faultHandler, String nodeType, Optional<Plugin<Authorizer>> authorizer) {
        this.nodeId = nodeId;
        this.faultHandler = faultHandler;
        this.nodeType = nodeType;
        this.authorizer = authorizer.filter(plugin -> plugin.get() instanceof ClusterMetadataAuthorizer);
        this.log = new LogContext(name()).logger(AclPublisher.class);
    }

    @Override
    public final String name() {
        return "AclPublisher " + nodeType + " id=" + nodeId;
    }

    @Override
    public void onMetadataUpdate(MetadataDelta delta, MetadataImage newImage, LoaderManifest manifest) {
        String deltaName = "MetadataDelta up to " + newImage.offset();

        // Apply changes to ACLs. This needs to be handled carefully because while we are
        // applying these changes, the Authorizer is continuing to return authorization
        // results in other threads. We never want to expose an invalid state. For example,
        // if the user created a DENY ALL acl and then created an ALLOW ACL for topic foo,
        // we want to apply those changes in that order, not the reverse order! Otherwise
        // there could be a window during which incorrect authorization results are returned.
        Optional.ofNullable(delta.aclsDelta()).ifPresent(aclsDelta -> {
            authorizer.ifPresent(authorizer -> {
                ClusterMetadataAuthorizer clusterMetadataAuthorizer = (ClusterMetadataAuthorizer) authorizer.get();
                if (manifest.type().equals(LoaderManifestType.SNAPSHOT)) {
                    try {
                        // If the delta resulted from a snapshot load, we want to apply the new changes
                        // all at once using ClusterMetadataAuthorizer#loadSnapshot. If this is the
                        // first snapshot load, it will also complete the futures returned by
                        // Authorizer#start (which we wait for before processing RPCs).
                        log.info("Loading authorizer snapshot at offset {}", newImage.offset());
                        clusterMetadataAuthorizer.loadSnapshot(newImage.acls().acls());
                    } catch (Throwable t) {
                        faultHandler.handleFault("Error loading authorizer snapshot in " + deltaName, t);
                    }
                } else {
                    try {
                        // Because the changes map is a LinkedHashMap, the deltas will be returned in
                        // the order they were performed.
                        aclsDelta.changes().forEach((key, value) -> {
                            if (value.isPresent()) {
                                clusterMetadataAuthorizer.addAcl(key, value.get());
                            } else {
                                clusterMetadataAuthorizer.removeAcl(key);
                            }
                        });
                    } catch (Throwable t) {
                        faultHandler.handleFault("Error loading authorizer changes in " + deltaName, t);
                    }
                }
                if (!completedInitialLoad) {
                    // If we are receiving this onMetadataUpdate call, that means the MetadataLoader has
                    // loaded up to the local high water mark. So we complete the initial load, enabling
                    // the authorizer.
                    completedInitialLoad = true;
                    clusterMetadataAuthorizer.completeInitialLoad();
                }
            });
        });
    }

    @Override
    public void close() {
        authorizer.ifPresent(authorizer -> {
            ClusterMetadataAuthorizer clusterMetadataAuthorizer = (ClusterMetadataAuthorizer) authorizer.get();
            clusterMetadataAuthorizer.completeInitialLoad(new TimeoutException());
        });
    }
}
