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
import org.apache.kafka.image.MetadataVersionChange;

import java.util.Objects;


/**
 * Contains information about a snapshot that we want to publish right before changing the metadata
 * version.
 */
public class PreVersionChangeManifest {
    /**
     * The source of this snapshot.
     */
    private final MetadataProvenance provenance;

    /**
     * The new metadata version.
     */
    private final MetadataVersionChange change;

    public PreVersionChangeManifest(
        MetadataProvenance provenance,
        MetadataVersionChange change
    ) {
        this.provenance = provenance;
        this.change = change;
    }

    public MetadataProvenance provenance() {
        return provenance;
    }

    public MetadataVersionChange change() {
        return change;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                provenance,
                change);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !o.getClass().equals(this.getClass())) return false;
        PreVersionChangeManifest other = (PreVersionChangeManifest) o;
        return provenance.equals(other.provenance) &&
                change.equals(other.change);
    }

    @Override
    public String toString() {
        return "PreVersionChangeManifest(" +
                "provenance=" + provenance +
                ", change=" + change +
                ")";
    }
}
