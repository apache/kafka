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

import org.apache.kafka.image.DelegationTokenImage;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.loader.LoaderManifest;
import org.apache.kafka.image.publisher.MetadataPublisher;
import org.apache.kafka.security.DelegationTokenManager;
import org.apache.kafka.server.fault.FaultHandler;

public class DelegationTokenPublisher implements MetadataPublisher {
    private final int nodeId;
    private final FaultHandler faultHandler;
    private final String nodeType;
    private final DelegationTokenManager tokenManager;
    private boolean firstPublish = true;

    public DelegationTokenPublisher(int nodeId, FaultHandler faultHandler, String nodeType, DelegationTokenManager tokenManager) {
        this.nodeId = nodeId;
        this.faultHandler = faultHandler;
        this.nodeType = nodeType;
        this.tokenManager = tokenManager;
    }

    @Override
    public final String name() {
        return "DelegationTokenPublisher " + nodeType + " id=" + nodeId;
    }

    @Override
    public void onMetadataUpdate(MetadataDelta delta, MetadataImage newImage, LoaderManifest manifest) {
        var first = firstPublish;
        try {
            if (firstPublish) {
                // Initialize the tokenCache with the Image
                DelegationTokenImage delegationTokenImage = newImage.delegationTokens();
                for (var token : delegationTokenImage.tokens().entrySet()) {
                    tokenManager.updateToken(tokenManager.getDelegationToken(token.getValue().tokenInformation()));
                }
                firstPublish = false;
            }
            // Apply changes to DelegationTokens.
            for (var token : delta.getOrCreateDelegationTokenDelta().changes().entrySet()) {
                var tokenId = token.getKey();
                var delegationTokenData = token.getValue();
                if (delegationTokenData.isPresent())
                    tokenManager.updateToken(tokenManager.getDelegationToken(delegationTokenData.get().tokenInformation()));
                else
                    tokenManager.removeToken(tokenId);
            }
        } catch (Throwable t) {
            var msg = String.format("Uncaught exception while publishing DelegationToken changes from %s MetadataDelta up to %s",
                first ? "initial" : "update", newImage.highestOffsetAndEpoch().offset());
            faultHandler.handleFault(msg, t);
        }
    }
}