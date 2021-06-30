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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;


/**
 * A map of feature names to their supported versions.
 */
public class FeatureMap {
    private final Map<String, VersionRange> features;

    public FeatureMap(Map<String, VersionRange> features) {
        this.features = Collections.unmodifiableMap(new HashMap<>(features));
    }

    public Optional<VersionRange> get(String name) {
        return Optional.ofNullable(features.get(name));
    }

    public Map<String, VersionRange> features() {
        return features;
    }

    @Override
    public int hashCode() {
        return features.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof FeatureMap)) return false;
        FeatureMap other = (FeatureMap) o;
        return features.equals(other.features);
    }

    @Override
    public String toString() {
        StringBuilder bld = new StringBuilder();
        bld.append("{");
        bld.append(features.keySet().stream().sorted().
            map(k -> k + ": " + features.get(k)).
            collect(Collectors.joining(", ")));
        bld.append("}");
        return bld.toString();
    }
}
