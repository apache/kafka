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

package org.apache.kafka.image;

import org.apache.kafka.server.common.MetadataVersion;

import java.util.Objects;


/**
 * A change in the MetadataVersion.
 */
public final class MetadataVersionChange {
    private final MetadataVersion oldVersion;
    private final MetadataVersion newVersion;

    public MetadataVersionChange(
            MetadataVersion oldVersion,
            MetadataVersion newVersion
    ) {
        this.oldVersion = Objects.requireNonNull(oldVersion);
        this.newVersion = Objects.requireNonNull(newVersion);
    }

    public MetadataVersion oldVersion() {
        return oldVersion;
    }

    public MetadataVersion newVersion() {
        return newVersion;
    }

    public boolean isUpgrade() {
        return oldVersion.isLessThan(newVersion);
    }

    public boolean isDowngrade() {
        return newVersion.isLessThan(oldVersion);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !o.getClass().equals(this.getClass())) return false;
        MetadataVersionChange other = (MetadataVersionChange) o;
        return oldVersion.equals(other.oldVersion) &&
                newVersion.equals(other.newVersion);
    }

    @Override
    public int hashCode() {
        return Objects.hash(oldVersion,
                newVersion);
    }

    @Override
    public String toString() {
        return "MetadataVersionChange(" +
                "oldVersion=" + oldVersion +
                ", newVersion=" + newVersion +
                ")";
    }
}
