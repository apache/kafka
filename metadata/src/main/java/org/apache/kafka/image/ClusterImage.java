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

package org.apache.kafka.image;

import org.apache.kafka.common.metadata.ClusterIdRecord;
import org.apache.kafka.image.node.ClusterImageNode;
import org.apache.kafka.image.writer.ImageWriter;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.metadata.BrokerRegistration;
import org.apache.kafka.metadata.ControllerRegistration;
import org.apache.kafka.server.common.ApiMessageAndVersion;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

/**
 * Represents the cluster in the metadata image.
 * <p>
 * This class is thread-safe.
 */
public record ClusterImage(
    Optional<String> clusterId,
    Map<Integer, BrokerRegistration> brokers,
    Map<Integer, ControllerRegistration> controllers
) {
    public static final ClusterImage EMPTY = new ClusterImage(
        Optional.empty(),
        Map.of(),
        Map.of());

    public ClusterImage(
        Optional<String> clusterId,
        Map<Integer, BrokerRegistration> brokers,
        Map<Integer, ControllerRegistration> controllers
    ) {
        this.clusterId = clusterId;
        this.brokers = Collections.unmodifiableMap(brokers);
        this.controllers = Collections.unmodifiableMap(controllers);
    }

    public boolean isEmpty() {
        return brokers.isEmpty();
    }

    public BrokerRegistration broker(int nodeId) {
        return brokers.get(nodeId);
    }

    public long brokerEpoch(int brokerId) {
        BrokerRegistration brokerRegistration = broker(brokerId);
        if (brokerRegistration == null) {
            return -1L;
        }
        return brokerRegistration.epoch();
    }

    public void write(ImageWriter writer, ImageWriterOptions options) {
        if (clusterId.isPresent()) {
            if (!options.metadataVersion().isClusterIdSupported()) {
                options.handleLoss("cluster id data");
            } else {
                writer.write(new ApiMessageAndVersion(
                    new ClusterIdRecord().setClusterId(clusterId.get()),
                    (short) 0));
            }
        }
        for (BrokerRegistration broker : brokers.values()) {
            writer.write(broker.toRecord(options));
        }
        if (!controllers.isEmpty()) {
            if (!options.metadataVersion().isControllerRegistrationSupported()) {
                options.handleLoss("controller registration data");
            } else {
                for (ControllerRegistration controller : controllers.values()) {
                    writer.write(controller.toRecord(options));
                }
            }
        }
    }

    @Override
    public String toString() {
        return new ClusterImageNode(this).stringify();
    }
}
