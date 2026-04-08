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

import org.apache.kafka.image.ClientQuotasDelta;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.loader.LoaderManifest;
import org.apache.kafka.image.publisher.MetadataPublisher;
import org.apache.kafka.server.fault.FaultHandler;

import java.util.function.Consumer;

/**
 * Publishes dynamic client quota changes to the client quota metadata manager.
 */
public class DynamicClientQuotaPublisher implements MetadataPublisher {
    private final int nodeId;
    private final FaultHandler faultHandler;
    private final String nodeType;
    private final Consumer<ClientQuotasDelta> clientQuotaUpdater;

    public DynamicClientQuotaPublisher(
        int nodeId,
        FaultHandler faultHandler,
        String nodeType,
        Consumer<ClientQuotasDelta> clientQuotaUpdater
    ) {
        this.nodeId = nodeId;
        this.faultHandler = faultHandler;
        this.nodeType = nodeType;
        this.clientQuotaUpdater = clientQuotaUpdater;
    }

    @Override
    public String name() {
        return "DynamicClientQuotaPublisher " + nodeType + " id=" + nodeId;
    }

    @Override
    public void onMetadataUpdate(MetadataDelta delta, MetadataImage newImage, LoaderManifest manifest) {
        ClientQuotasDelta clientQuotasDelta = delta.clientQuotasDelta();
        if (clientQuotasDelta != null) {
            try {
                clientQuotaUpdater.accept(clientQuotasDelta);
            } catch (Throwable t) {
                faultHandler.handleFault("Uncaught exception while publishing dynamic client quota changes from MetadataDelta up to " + newImage.highestOffsetAndEpoch().offset(), t);
            }
        }
    }
}
