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
import org.apache.kafka.server.common.Features;
import org.slf4j.Logger;

import static org.apache.kafka.server.common.MetadataVersion.MINIMUM_KRAFT_VERSION;


public class FeaturesPublisher implements MetadataPublisher {
    private final Logger log;
    private volatile Features features = Features.fromKRaftVersion(MINIMUM_KRAFT_VERSION);

    public FeaturesPublisher(
        LogContext logContext
    ) {
        log = logContext.logger(FeaturesPublisher.class);
    }

    public Features features() {
        return features;
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
        if (delta.featuresDelta() != null) {
            Features newFeatures = new Features(newImage.features().metadataVersion(),
                    newImage.features().finalizedVersions(),
                    newImage.provenance().lastContainedOffset(),
                    true);
            if (!newFeatures.equals(features)) {
                log.info("Loaded new metadata {}.", newFeatures);
                features = newFeatures;
            }
        }
    }
}
