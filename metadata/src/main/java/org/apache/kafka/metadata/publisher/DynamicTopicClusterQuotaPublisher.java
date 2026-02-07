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

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.internals.Plugin;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.loader.LoaderManifest;
import org.apache.kafka.image.publisher.MetadataPublisher;
import org.apache.kafka.metadata.MetadataCache;
import org.apache.kafka.server.fault.FaultHandler;
import org.apache.kafka.server.quota.ClientQuotaCallback;

import java.util.Optional;

public class DynamicTopicClusterQuotaPublisher implements MetadataPublisher {
    private final String clusterId;
    private final int nodeId;
    private final FaultHandler faultHandler;
    private final String nodeType;
    private final Optional<Plugin<ClientQuotaCallback>> clientQuotaCallbackPlugin;
    private final QuotaConfigChangeListener quotaConfigChangeListener;

    public DynamicTopicClusterQuotaPublisher(
        String clusterId,
        int nodeId,
        FaultHandler faultHandler,
        String nodeType,
        Optional<Plugin<ClientQuotaCallback>> clientQuotaCallbackPlugin,
        QuotaConfigChangeListener quotaConfigChangeListener
    ) {
        this.clusterId = clusterId;
        this.nodeId = nodeId;
        this.faultHandler = faultHandler;
        this.nodeType = nodeType;
        this.clientQuotaCallbackPlugin = clientQuotaCallbackPlugin;
        this.quotaConfigChangeListener = quotaConfigChangeListener;
    }

    @Override
    public String name() {
        return "DynamicTopicClusterQuotaPublisher " + nodeType + " id=" + nodeId;
    }

    @Override
    public void onMetadataUpdate(MetadataDelta delta, MetadataImage newImage, LoaderManifest manifest) {
        try {
            clientQuotaCallbackPlugin.ifPresent(plugin -> {
                if (delta.topicsDelta() != null || delta.clusterDelta() != null) {
                    Cluster cluster = MetadataCache.toCluster(clusterId, newImage);
                    if (plugin.get().updateClusterMetadata(cluster)) {
                        quotaConfigChangeListener.onChange();
                    }
                }
            });
        } catch (Throwable e) {
            String deltaName = "MetadataDelta up to " + newImage.highestOffsetAndEpoch().offset();
            faultHandler.handleFault("Uncaught exception while publishing dynamic topic or cluster changes from " + deltaName, e);
        }
    }
}
