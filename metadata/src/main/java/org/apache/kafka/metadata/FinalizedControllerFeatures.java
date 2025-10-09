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
import java.util.Optional;
import java.util.Set;

/**
 * A map of feature names to their supported versions.
 */
public record FinalizedControllerFeatures(Map<String, Short> featureMap, long epoch) {
    public FinalizedControllerFeatures(Map<String, Short> featureMap, long epoch) {
        this.featureMap = Collections.unmodifiableMap(featureMap);
        this.epoch = epoch;
    }

    public Optional<Short> get(String name) {
        return Optional.ofNullable(featureMap.get(name));
    }

    public short versionOrDefault(String name, short defaultValue) {
        return featureMap.getOrDefault(name, defaultValue);
    }

    public Set<String> featureNames() {
        return featureMap.keySet();
    }
}
