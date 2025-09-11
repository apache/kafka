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

import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.ScramDelta;
import org.apache.kafka.image.loader.LoaderManifest;
import org.apache.kafka.image.publisher.MetadataPublisher;
import org.apache.kafka.security.CredentialProvider;
import org.apache.kafka.server.fault.FaultHandler;

public class ScramPublisher implements MetadataPublisher {
    private final int nodeId;
    private final FaultHandler faultHandler;
    private final String nodeType;
    private final CredentialProvider credentialProvider;

    public ScramPublisher(int nodeId, FaultHandler faultHandler, String nodeType, CredentialProvider credentialProvider) {
        this.nodeId = nodeId;
        this.faultHandler = faultHandler;
        this.nodeType = nodeType;
        this.credentialProvider = credentialProvider;
    }

    @Override
    public final String name() {
        return "ScramPublisher " + nodeType + " id=" + nodeId;
    }

    @Override
    public void onMetadataUpdate(MetadataDelta delta, MetadataImage newImage, LoaderManifest manifest) {
        try {
            // Apply changes to SCRAM credentials.
            ScramDelta scramDelta = delta.scramDelta();
            if (scramDelta != null) {
                scramDelta.changes().forEach((mechanism, userChanges) -> {
                    userChanges.forEach((userName, change) -> {
                        if (change.isPresent())
                            credentialProvider.updateCredential(mechanism, userName, change.get().toCredential());
                        else
                            credentialProvider.removeCredentials(mechanism, userName);
                    });
                });
            }
        } catch (Throwable t) {
            faultHandler.handleFault("Uncaught exception while publishing SCRAM changes from MetadataDelta up to "
                + newImage.highestOffsetAndEpoch().offset(), t);
        }
    }
}
