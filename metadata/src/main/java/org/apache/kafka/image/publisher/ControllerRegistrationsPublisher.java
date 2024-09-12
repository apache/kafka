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

package org.apache.kafka.image.publisher;

import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.message.DescribeClusterResponseData.DescribeClusterBroker;
import org.apache.kafka.common.message.DescribeClusterResponseData.DescribeClusterBrokerCollection;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.loader.LoaderManifest;
import org.apache.kafka.image.loader.LoaderManifestType;
import org.apache.kafka.metadata.ControllerRegistration;

import java.util.Collections;
import java.util.Map;


/**
 * A publisher to track controller registrations.
 */
public class ControllerRegistrationsPublisher implements MetadataPublisher {
    private volatile Map<Integer, ControllerRegistration> controllers;

    public ControllerRegistrationsPublisher() {
        this.controllers = Collections.emptyMap();
    }

    @Override
    public String name() {
        return "ControllerRegistrationsPublisher";
    }

    @Override
    public void onMetadataUpdate(
        MetadataDelta delta,
        MetadataImage newImage,
        LoaderManifest manifest
    ) {
        if (manifest.type() == LoaderManifestType.LOG_DELTA || delta.clusterDelta() != null) {
            controllers = newImage.cluster().controllers();
        }
    }

    public DescribeClusterBrokerCollection describeClusterControllers(
        String endpointName
    ) {
        DescribeClusterBrokerCollection results = new DescribeClusterBrokerCollection();
        for (ControllerRegistration registration : controllers.values()) {
            Endpoint endpoint = registration.listeners().get(endpointName);
            if (endpoint != null) {
                results.add(new DescribeClusterBroker().
                        setBrokerId(registration.id()).
                        setHost(endpoint.host()).
                        setPort(endpoint.port()).
                        setRack(null));
            }
        }
        return results;
    }

    public Map<Integer, ControllerRegistration> controllers() {
        return controllers;
    }

    @Override
    public void close() {
    }
}
