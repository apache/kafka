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

import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.loader.LoaderManifest;
import org.apache.kafka.image.publisher.MetadataPublisher;
import org.apache.kafka.server.common.FinalizedFeatures;
import org.apache.kafka.server.fault.FaultHandler;

import org.slf4j.Logger;

import static org.apache.kafka.server.common.MetadataVersion.MINIMUM_VERSION;


public class FeaturesPublisher implements MetadataPublisher {
    private final Logger log;
    private final FaultHandler faultHandler;
    private volatile FinalizedFeatures finalizedFeatures = FinalizedFeatures.fromKRaftVersion(MINIMUM_VERSION);

    public FeaturesPublisher(
        LogContext logContext,
        FaultHandler faultHandler
    ) {
        this.log = logContext.logger(FeaturesPublisher.class);
        this.faultHandler = faultHandler;
    }

    public FinalizedFeatures features() {
        return finalizedFeatures;
    }

    @Override
    public String name() {
        return "FeaturesPublisher";
    }

    @Override
    public void onMetadataUpdate(
        MetadataDelta delta,
        MetadataImage newImage,
        LoaderManifest manifest
    ) {
        try {
            if (delta.featuresDelta() != null) {
                FinalizedFeatures newFinalizedFeatures = new FinalizedFeatures(newImage.features().metadataVersionOrThrow(),
                    newImage.features().finalizedVersions(),
                    newImage.provenance().lastContainedOffset()
                );
                if (!newFinalizedFeatures.equals(finalizedFeatures)) {
                    log.info("Loaded new metadata {}.", newFinalizedFeatures);
                    finalizedFeatures = newFinalizedFeatures;
                }
            }
        } catch (Throwable t) {
            faultHandler.handleFault("Uncaught exception while publishing SCRAM changes from MetadataDelta up to "
                + newImage.highestOffsetAndEpoch().offset(), t);
        }
    }
}
