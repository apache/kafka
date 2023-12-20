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
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;


/**
 * A map of feature names to their supported versions.
 */
public class FinalizedControllerFeatures {
    private final Map<String, Short> featureMap;
    private final long epoch;

    public FinalizedControllerFeatures(Map<String, Short> featureMap, long epoch) {
        this.featureMap = Collections.unmodifiableMap(featureMap);
        this.epoch = epoch;
    }

    public Optional<Short> get(String name) {
        return Optional.ofNullable(featureMap.get(name));
    }

    public Set<String> featureNames() {
        return featureMap.keySet();
    }

    public Map<String, Short> featureMap() {
        return featureMap;
    }

    public long epoch() {
        return epoch;
    }

    @Override
    public int hashCode() {
        return Objects.hash(featureMap, epoch);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof FinalizedControllerFeatures)) return false;
        FinalizedControllerFeatures other = (FinalizedControllerFeatures) o;
        return featureMap.equals(other.featureMap) && epoch == other.epoch;
    }

    @Override
    public String toString() {
        StringBuilder bld = new StringBuilder();
        bld.append("{");
        bld.append("featureMap=").append(featureMap.toString());
        bld.append(", epoch=").append(epoch);
        bld.append("}");
        return bld.toString();
    }
}
