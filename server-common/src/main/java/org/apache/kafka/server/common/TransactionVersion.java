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
package org.apache.kafka.server.common;

import org.apache.kafka.common.requests.AddPartitionsToTxnRequest;
import org.apache.kafka.common.requests.EndTxnRequest;

import java.util.Collections;
import java.util.Map;

public enum TransactionVersion implements FeatureVersion {

    // Version 0 is the original transaction coordinator with no extra features enabled.
    TV_0(0, MetadataVersion.MINIMUM_KRAFT_VERSION, Collections.emptyMap()),
    // Version 1 enables flexible transactional state records. (KIP-890)
    TV_1(1, MetadataVersion.IBP_4_0_IV2, Collections.emptyMap()),
    // Version 2 enables epoch bump per transaction and optimizations. (KIP-890)
    TV_2(2, MetadataVersion.IBP_4_0_IV2, Collections.emptyMap());

    public static final String FEATURE_NAME = "transaction.version";

    public static final TransactionVersion LATEST_PRODUCTION = TV_2;

    private final short featureLevel;
    private final MetadataVersion bootstrapMetadataVersion;
    private final Map<String, Short> dependencies;

    TransactionVersion(
        int featureLevel,
        MetadataVersion bootstrapMetadataVersion,
        Map<String, Short> dependencies
    ) {
        this.featureLevel = (short) featureLevel;
        this.bootstrapMetadataVersion = bootstrapMetadataVersion;
        this.dependencies = dependencies;
    }

    @Override
    public short featureLevel() {
        return featureLevel;
    }

    public static TransactionVersion fromFeatureLevel(short version) {
        return (TransactionVersion) Feature.TRANSACTION_VERSION.fromFeatureLevel(version, true);
    }

    public static TransactionVersion transactionVersionForAddPartitionsToTxn(AddPartitionsToTxnRequest request) {
        // If the request is greater than version 3, we know the client supports transaction version 2.
        return request.version() > 3 ? TV_2 : TV_0;
    }

    public static TransactionVersion transactionVersionForEndTxn(EndTxnRequest request) {
        // If the request is greater than version 4, we know the client supports transaction version 2.
        return request.version() > 4 ? TV_2 : TV_0;
    }

    @Override
    public String featureName() {
        return FEATURE_NAME;
    }

    @Override
    public MetadataVersion bootstrapMetadataVersion() {
        return bootstrapMetadataVersion;
    }

    @Override
    public Map<String, Short> dependencies() {
        return dependencies;
    }

    // Transactions V1 enables log version 0 (flexible fields)
    public short transactionLogValueVersion() {
        return (short) (featureLevel >= 1 ? 1 : 0);
    }

    // Transactions V2 enables epoch bump on commit/abort.
    public boolean supportsEpochBump() {
        return featureLevel >= 2;
    }
}
