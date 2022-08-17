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

import org.apache.kafka.image.MetadataImage;

import java.util.Objects;


public class SnapshotCreationRequest {
    private final long lastContainedLogTimeMs;
    private final MetadataImage image;
    private final boolean force;

    public SnapshotCreationRequest(
        long lastContainedLogTimeMs,
        MetadataImage image,
        boolean force
    ) {
        this.lastContainedLogTimeMs = lastContainedLogTimeMs;
        this.image = image;
        this.force = force;
    }

    public long lastContainedLogTimeMs() {
        return lastContainedLogTimeMs;
    }

    public MetadataImage image() {
        return image;
    }

    public boolean force() {
        return force;
    }

    public long offset() {
        return image.highestOffsetAndEpoch().offset;
    }

    public int epoch() {
        return image.highestOffsetAndEpoch().epoch;
    }

    @Override
    public int hashCode() {
        return Objects.hash(lastContainedLogTimeMs,
                image,
                force);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !(o.getClass().equals(this.getClass()))) return false;
        SnapshotCreationRequest other = (SnapshotCreationRequest) o;
        return lastContainedLogTimeMs == other.lastContainedLogTimeMs &&
            image.equals(other.image) &&
            force == other.force;
    }

    @Override
    public String toString() {
        return "SnapshotCreationRequest(" +
            "lastContainedLogTimeMs=" + lastContainedLogTimeMs +
            ", image=" + image +
            ", force=" + force +
            ")";
    }
}
