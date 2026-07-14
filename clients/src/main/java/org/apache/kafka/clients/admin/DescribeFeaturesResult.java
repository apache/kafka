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

package org.apache.kafka.clients.admin;

import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.annotation.InterfaceAudience;

/**
 * The result of the {@link Admin#describeFeatures(DescribeFeaturesOptions)} call.
 *
 * The API of this class is evolving, see {@link Admin} for details.
 */
@InterfaceAudience.Public
public class DescribeFeaturesResult {

    private final KafkaFuture<FeatureMetadata> future;

    DescribeFeaturesResult(KafkaFuture<FeatureMetadata> future) {
        this.future = future;
    }

    public KafkaFuture<FeatureMetadata> featureMetadata() {
        return future;
    }

    /**
     * This class is NOT part of the public API. It is only intended for internal Kafka tools that
     * additionally need access to the raw node API versions returned in the {@code ApiVersionsResponse}.
     */
    @InterfaceAudience.Private
    public static class Internal extends DescribeFeaturesResult {

        private final KafkaFuture<NodeApiVersions> nodeApiVersions;

        public Internal(KafkaFuture<FeatureMetadata> featureMetadata, KafkaFuture<NodeApiVersions> nodeApiVersions) {
            super(featureMetadata);
            this.nodeApiVersions = nodeApiVersions;
        }

        public KafkaFuture<NodeApiVersions> nodeApiVersions() {
            return nodeApiVersions;
        }
    }
}
