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

import java.util.Objects;


/**
 * A map of feature names to their supported versions.
 */
public class FeatureMapAndEpoch {
    private final FeatureMap map;
    private final long epoch;

    public FeatureMapAndEpoch(FeatureMap map, long epoch) {
        this.map = map;
        this.epoch = epoch;
    }

    public FeatureMap map() {
        return map;
    }

    public long epoch() {
        return epoch;
    }

    @Override
    public int hashCode() {
        return Objects.hash(map, epoch);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof FeatureMapAndEpoch)) return false;
        FeatureMapAndEpoch other = (FeatureMapAndEpoch) o;
        return map.equals(other.map) && epoch == other.epoch;
    }

    @Override
    public String toString() {
        StringBuilder bld = new StringBuilder();
        bld.append("{");
        bld.append("map=").append(map.toString());
        bld.append(", epoch=").append(epoch);
        bld.append("}");
        return bld.toString();
    }
}
