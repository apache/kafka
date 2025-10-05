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

package org.apache.kafka.image.writer;

import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.server.common.MetadataVersion;

import java.util.function.Consumer;


/**
 * The options to use when writing an image.
 */
public final class ImageWriterOptions {
    public static class Builder {
        private MetadataVersion metadataVersion;
        private Consumer<UnwritableMetadataException> lossHandler = e -> {
            throw e;
        };
        private boolean isEligibleLeaderReplicasEnabled = false;

        public Builder(MetadataVersion metadataVersion) {
            this.metadataVersion = metadataVersion;
        }

        public Builder(MetadataImage image) {
            this.metadataVersion = image.features().metadataVersionOrThrow();
            this.isEligibleLeaderReplicasEnabled = image.features().isElrEnabled();
        }

        public Builder setMetadataVersion(MetadataVersion metadataVersion) {
            this.metadataVersion = metadataVersion;
            return this;
        }

        public Builder setEligibleLeaderReplicasEnabled(boolean isEligibleLeaderReplicasEnabled) {
            this.isEligibleLeaderReplicasEnabled = isEligibleLeaderReplicasEnabled;
            return this;
        }

        public MetadataVersion metadataVersion() {
            return metadataVersion;
        }

        public boolean isEligibleLeaderReplicasEnabled() {
            return isEligibleLeaderReplicasEnabled;
        }

        public Builder setLossHandler(Consumer<UnwritableMetadataException> lossHandler) {
            this.lossHandler = lossHandler;
            return this;
        }

        public ImageWriterOptions build() {
            return new ImageWriterOptions(metadataVersion, lossHandler, isEligibleLeaderReplicasEnabled);
        }
    }

    private final MetadataVersion metadataVersion;
    private final Consumer<UnwritableMetadataException> lossHandler;
    private final boolean isEligibleLeaderReplicasEnabled;

    private ImageWriterOptions(
        MetadataVersion metadataVersion,
        Consumer<UnwritableMetadataException> lossHandler,
        boolean isEligibleLeaderReplicasEnabled
    ) {
        this.metadataVersion = metadataVersion;
        this.lossHandler = lossHandler;
        this.isEligibleLeaderReplicasEnabled = isEligibleLeaderReplicasEnabled;
    }

    public MetadataVersion metadataVersion() {
        return metadataVersion;
    }
    public boolean isEligibleLeaderReplicasEnabled() {
        return isEligibleLeaderReplicasEnabled;
    }

    public void handleLoss(String loss) {
        lossHandler.accept(new UnwritableMetadataException(metadataVersion, loss));
    }
}
