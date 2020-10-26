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

import org.apache.kafka.common.errors.InvalidUpdateVersionException;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A class which stores supported and finalized features.
 */
public class FeatureManager {
    private final Map<String, VersionRange> supportedFeatures;
    volatile private Map<String, VersionRange> finalizedFeatures = Collections.emptyMap();

    public FeatureManager(Map<String, VersionRange> supportedFeatures,
                          Map<String, VersionRange> finalizedFeatures) {
        this.supportedFeatures = Collections.unmodifiableMap(new HashMap<>(supportedFeatures));
        updateFinalizedFeatures(finalizedFeatures);
    }

    public void updateFinalizedFeatures(Map<String, VersionRange> newFinalizedFeatures) {
        validateFinalizedFeatures(newFinalizedFeatures);
        synchronized (this) {
            this.finalizedFeatures = Collections.unmodifiableMap(
                    new HashMap<>(newFinalizedFeatures));
        }
    }

    public void validateFinalizedFeatures(Map<String, VersionRange> newFinalizedFeatures) {
        for (Map.Entry<String, VersionRange> entry : newFinalizedFeatures.entrySet()) {
            String key = entry.getKey();
            VersionRange finalizedRange = entry.getValue();
            VersionRange supportedRange = supportedFeatures.get(key);
            if (supportedRange == null) {
                throw new InvalidUpdateVersionException("Unable to finalize " + key +
                    " because that feature is not supported by this node.");
            }
            if (!supportedRange.contains(finalizedRange)) {
                throw new InvalidUpdateVersionException("Unable to finalize " + key +
                    " because the requested range " + finalizedRange + " is outside the " +
                    "supported range " + supportedRange + ".");
            }
        }
    }

    public Map<String, VersionRange> finalizedFeatures() {
        return finalizedFeatures;
    }

    @Override
    public int hashCode() {
        return Objects.hash(supportedFeatures, finalizedFeatures);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof FeatureManager)) return false;
        FeatureManager other = (FeatureManager) o;
        return supportedFeatures.equals(other.supportedFeatures) &&
            finalizedFeatures.equals(other.finalizedFeatures);
    }

    @Override
    public String toString() {
        StringBuilder bld = new StringBuilder();
        bld.append("FeatureManager(supportedFeatures={");
        bld.append(supportedFeatures.keySet().stream().sorted().
            map(k -> k + ": " + supportedFeatures.get(k)).
            collect(Collectors.joining(", ")));
        bld.append("}, finalizedFeatures={");
        bld.append(finalizedFeatures.keySet().stream().sorted().
                map(k -> k + ": " + finalizedFeatures.get(k)).
                collect(Collectors.joining(", ")));
        bld.append("})");
        return bld.toString();
    }
}
