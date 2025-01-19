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

package org.apache.kafka.controller.metrics;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.TopicDelta;
import org.apache.kafka.image.TopicImage;
import org.apache.kafka.image.loader.LoaderManifest;
import org.apache.kafka.image.publisher.MetadataPublisher;
import org.apache.kafka.metadata.BrokerRegistration;
import org.apache.kafka.metadata.PartitionRegistration;
import org.apache.kafka.server.fault.FaultHandler;

import java.util.Map.Entry;
import java.util.Optional;


/**
 * This publisher translates metadata updates sent by MetadataLoader into changes to controller
 * metrics. Like all MetadataPublisher objects, it only receives notifications about events that
 * have been persisted to the metadata log. So on the active controller, it will run slightly
 * behind the latest in-memory state which has not yet been fully persisted to the log. This is
 * reasonable for metrics, which don't need up-to-the-millisecond update latency.
 *
 * NOTE: the ZK controller has some special rules for calculating preferredReplicaImbalanceCount
 * which we haven't implemented here. Specifically, the ZK controller considers reassigning
 * partitions to always have their preferred leader, even if they don't.
 * All other metrics should be the same, as far as is possible.
 */
public class ControllerMetadataMetricsPublisher implements MetadataPublisher {
    private final ControllerMetadataMetrics metrics;
    private final FaultHandler faultHandler;
    private MetadataImage prevImage = MetadataImage.EMPTY;

    public ControllerMetadataMetricsPublisher(
        ControllerMetadataMetrics metrics,
        FaultHandler faultHandler
    ) {
        this.metrics = metrics;
        this.faultHandler = faultHandler;
    }

    @Override
    public String name() {
        return "ControllerMetadataMetricsPublisher";
    }

    @Override
    public void onMetadataUpdate(
        MetadataDelta delta,
        MetadataImage newImage,
        LoaderManifest manifest
    ) {
        switch (manifest.type()) {
            case LOG_DELTA:
                try {
                    publishDelta(delta);
                } catch (Throwable e) {
                    faultHandler.handleFault("Failed to publish controller metrics from log delta " +
                            " ending at offset " + manifest.provenance().lastContainedOffset(), e);
                } finally {
                    prevImage = newImage;
                }
                break;
            case SNAPSHOT:
                try {
                    publishSnapshot(newImage);
                } catch (Throwable e) {
                    faultHandler.handleFault("Failed to publish controller metrics from " +
                            manifest.provenance().snapshotName(), e);
                } finally {
                    prevImage = newImage;
                }
                break;
        }
    }

    private void publishDelta(MetadataDelta delta) {
        ControllerMetricsChanges changes = new ControllerMetricsChanges();
        if (delta.clusterDelta() != null) {
            for (Entry<Integer, Optional<BrokerRegistration>> entry :
                    delta.clusterDelta().changedBrokers().entrySet()) {
                changes.handleBrokerChange(prevImage.cluster().brokers().get(entry.getKey()),
                        entry.getValue().orElse(null));
            }
        }
        if (delta.topicsDelta() != null) {
            for (Uuid topicId : delta.topicsDelta().deletedTopicIds()) {
                TopicImage prevTopic = prevImage.topics().topicsById().get(topicId);
                if (prevTopic == null) {
                    throw new RuntimeException("Unable to find deleted topic id " + topicId +
                            " in previous topics image.");
                }
                changes.handleDeletedTopic(prevTopic);
            }
            for (Entry<Uuid, TopicDelta> entry : delta.topicsDelta().changedTopics().entrySet()) {
                changes.handleTopicChange(prevImage.topics().getTopic(entry.getKey()), entry.getValue());
            }
        }
        changes.apply(metrics);
    }

    private void publishSnapshot(MetadataImage newImage) {
        metrics.setGlobalTopicCount(newImage.topics().topicsById().size());
        int fencedBrokers = 0;
        int activeBrokers = 0;
        int zkBrokers = 0;
        for (BrokerRegistration broker : newImage.cluster().brokers().values()) {
            if (broker.fenced()) {
                fencedBrokers++;
            } else {
                activeBrokers++;
            }
            if (broker.isMigratingZkBroker()) {
                zkBrokers++;
            }
        }
        metrics.setFencedBrokerCount(fencedBrokers);
        metrics.setActiveBrokerCount(activeBrokers);
        metrics.setMigratingZkBrokerCount(zkBrokers);

        int totalPartitions = 0;
        int offlinePartitions = 0;
        int partitionsWithoutPreferredLeader = 0;
        for (TopicImage topicImage : newImage.topics().topicsById().values()) {
            for (PartitionRegistration partition : topicImage.partitions().values()) {
                if (!partition.hasLeader()) {
                    offlinePartitions++;
                }
                if (!partition.hasPreferredLeader()) {
                    partitionsWithoutPreferredLeader++;
                }
                totalPartitions++;
            }
        }
        metrics.setGlobalPartitionCount(totalPartitions);
        metrics.setOfflinePartitionCount(offlinePartitions);
        metrics.setPreferredReplicaImbalanceCount(partitionsWithoutPreferredLeader);
    }

    @Override
    public void close() {
        metrics.close();
    }
}
