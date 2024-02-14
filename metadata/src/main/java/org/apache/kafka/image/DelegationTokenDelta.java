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

import org.apache.kafka.common.metadata.DelegationTokenRecord;
import org.apache.kafka.common.metadata.RemoveDelegationTokenRecord;
import org.apache.kafka.metadata.DelegationTokenData;
import org.apache.kafka.server.common.MetadataVersion;

import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Map;
import java.util.Optional;


/**
 * Represents changes to delegation tokens in the metadata image.
 */
public final class DelegationTokenDelta {
    private final DelegationTokenImage image;

    // Key is TokenID which is contained in the value TokenInformation
    private final Map<String, Optional<DelegationTokenData>> changes = new HashMap<>();

    public DelegationTokenDelta(DelegationTokenImage image) {
        this.image = image;
    }

    public void finishSnapshot() {
        for (String tokenId : image.tokens().keySet()) {
            if (!changes.containsKey(tokenId)) {
                // If the tokenId from the image did not appear in the snapshot, mark it as removed
                changes.put(tokenId, Optional.empty());

            }
        }
    }

    public DelegationTokenImage image() {
        return image;
    }

    public Map<String, Optional<DelegationTokenData>> changes() {
        return changes;
    }

    public void replay(DelegationTokenRecord record) {
        changes.put(record.tokenId(), Optional.of(DelegationTokenData.fromRecord(record)));
    }

    public void replay(RemoveDelegationTokenRecord record) {
        changes.put(record.tokenId(), Optional.empty());
    }

    public void handleMetadataVersionChange(MetadataVersion changedMetadataVersion) {
        // nothing to do
    }

    public DelegationTokenImage apply() {
        Map<String, DelegationTokenData> newTokens = new HashMap<>();

        // Add image entries to newTokens if there isn't a change for that entry.
        // A snapshot of the deltas will call finishSnapshot() first which will add a
        // removal change for all entries in the base image which did not have a change record.
        for (Entry<String, DelegationTokenData> entry : image.tokens().entrySet()) {
            Optional<DelegationTokenData> change = changes.get(entry.getKey());
            if (change == null) {
                newTokens.put(entry.getKey(), entry.getValue());
            } else if (change.isPresent()) {
                newTokens.put(entry.getKey(), change.get());
            }
        }

        // Add changed entries to newTokens that aren't already in newTokens.
        for (Entry<String, Optional<DelegationTokenData>> entry : changes.entrySet()) {
            if (!newTokens.containsKey(entry.getKey())) {
                if (entry.getValue().isPresent()) {
                    newTokens.put(entry.getKey(), entry.getValue().get());
                }
            }
        }
        return new DelegationTokenImage(newTokens);
    }

    @Override
    public String toString() {
        return "DelegationTokenDelta(" +
            "changes=" + changes +
            ')';
    }
}
