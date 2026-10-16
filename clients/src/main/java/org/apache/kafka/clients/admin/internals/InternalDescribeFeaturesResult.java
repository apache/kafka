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
package org.apache.kafka.clients.admin.internals;

import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.clients.admin.DescribeFeaturesResult;
import org.apache.kafka.clients.admin.FeatureMetadata;
import org.apache.kafka.common.KafkaFuture;

/**
 * Internal result class for describeFeatures that exposes API version information.
 * This class is intended for use by internal Kafka tools that need access to the raw API versions
 * returned in the ApiVersionsResponse.
 */
public class InternalDescribeFeaturesResult extends DescribeFeaturesResult {

    private final KafkaFuture<NodeApiVersions> nodeApiVersions;

    public InternalDescribeFeaturesResult(KafkaFuture<FeatureMetadata> featureMetadata, KafkaFuture<NodeApiVersions> nodeApiVersions) {
        super(featureMetadata);
        this.nodeApiVersions = nodeApiVersions;
    }

    /**
     * Returns the node API versions future. This contains the API keys and version ranges
     * supported by the broker, as returned in the ApiVersionsResponse.
     *
     * @return KafkaFuture containing the node API versions
     */
    public KafkaFuture<NodeApiVersions> nodeApiVersions() {
        return nodeApiVersions;
    }
}
