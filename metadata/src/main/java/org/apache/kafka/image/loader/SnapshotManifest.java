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

package org.apache.kafka.image.loader;

import org.apache.kafka.image.MetadataProvenance;

import java.util.Objects;


/**
 * Contains information about a snapshot that was loaded.
 */
public class SnapshotManifest implements LoaderManifest {
    /**
     * The source of this snapshot.
     */
    private final MetadataProvenance provenance;

    /**
     * The time in microseconds that it took to load the snapshot.
     */
    private final long elapsedNs;

    public SnapshotManifest(
        MetadataProvenance provenance,
        long elapsedNs
    ) {
        this.provenance = provenance;
        this.elapsedNs = elapsedNs;
    }

    @Override
    public LoaderManifestType type() {
        return LoaderManifestType.SNAPSHOT;
    }

    @Override
    public MetadataProvenance provenance() {
        return provenance;
    }

    public long elapsedNs() {
        return elapsedNs;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                provenance,
                elapsedNs);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !o.getClass().equals(this.getClass())) return false;
        SnapshotManifest other = (SnapshotManifest) o;
        return provenance.equals(other.provenance) &&
                elapsedNs == other.elapsedNs;
    }

    @Override
    public String toString() {
        return "SnapshotManifest(" +
                "provenance=" + provenance +
                ", elapsedNs=" + elapsedNs +
                ")";
    }
}
