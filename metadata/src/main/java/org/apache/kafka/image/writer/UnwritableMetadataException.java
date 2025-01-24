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

import org.apache.kafka.server.common.MetadataVersion;


/**
 * Indicates that an ImageWriter can't write some specific metadata
 */
public final class UnwritableMetadataException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    private final MetadataVersion metadataVersion;
    private final String loss;

    public UnwritableMetadataException(
        MetadataVersion metadataVersion,
        String loss
    ) {
        super("Metadata has been lost because the following could not be represented " +
            "in metadata.version " + metadataVersion + ": " + loss);
        this.metadataVersion = metadataVersion;
        this.loss = loss;
    }

    public MetadataVersion metadataVersion() {
        return metadataVersion;
    }

    public String loss() {
        return loss;
    }
}
