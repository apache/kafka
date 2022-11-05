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

import org.apache.kafka.image.writer.ImageWriter;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.metadata.BrokerRegistration;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Represents the cluster in the metadata image.
 *
 * This class is thread-safe.
 */
public final class ClusterImage {
    public static final ClusterImage EMPTY = new ClusterImage(Collections.emptyMap());

    private final Map<Integer, BrokerRegistration> brokers;

    public ClusterImage(Map<Integer, BrokerRegistration> brokers) {
        this.brokers = Collections.unmodifiableMap(brokers);
    }

    public boolean isEmpty() {
        return brokers.isEmpty();
    }

    public Map<Integer, BrokerRegistration> brokers() {
        return brokers;
    }

    public BrokerRegistration broker(int nodeId) {
        return brokers.get(nodeId);
    }

    public void write(ImageWriter writer, ImageWriterOptions options) {
        for (BrokerRegistration broker : brokers.values()) {
            writer.write(broker.toRecord(options));
        }
    }

    @Override
    public int hashCode() {
        return brokers.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ClusterImage)) return false;
        ClusterImage other = (ClusterImage) o;
        return brokers.equals(other.brokers);
    }

    @Override
    public String toString() {
        return brokers.entrySet().stream().
            map(e -> e.getKey() + ":" + e.getValue()).collect(Collectors.joining(", "));
    }
}
