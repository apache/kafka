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

package org.apache.kafka.metadata;

import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.loader.LoaderManifest;
import org.apache.kafka.image.publisher.MetadataPublisher;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.fault.FaultHandler;

import java.util.function.Consumer;

public class MetadataVersionConfigValidator implements MetadataPublisher {
    private final String name;
    private final Consumer<MetadataVersion> validator;
    private final FaultHandler faultHandler;

    public MetadataVersionConfigValidator(
        int brokerId,
        Consumer<MetadataVersion> validator,
        FaultHandler faultHandler
    ) {
        this.name = "MetadataVersionPublisher(id=" + brokerId + ")";
        this.validator = validator;
        this.faultHandler = faultHandler;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void onMetadataUpdate(
        MetadataDelta delta,
        MetadataImage newImage,
        LoaderManifest manifest
    ) {
        if (delta.featuresDelta() != null) {
            if (delta.metadataVersionChanged().isPresent()) {
                onMetadataVersionChanged(newImage.features().metadataVersionOrThrow());
            }
        }
    }

    @SuppressWarnings("ThrowableNotThrown")
    private void onMetadataVersionChanged(MetadataVersion metadataVersion) {
        try {
            this.validator.accept(metadataVersion);
        } catch (Throwable t) {
            this.faultHandler.handleFault(
                "Broker configuration does not support the cluster MetadataVersion", t);
        }
    }
}
