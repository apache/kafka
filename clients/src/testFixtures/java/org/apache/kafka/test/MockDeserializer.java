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
package org.apache.kafka.test;

import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.ClusterResourceListener;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class MockDeserializer implements ClusterResourceListener, Deserializer<String> {
    public static AtomicInteger initCount = new AtomicInteger(0);
    public static AtomicInteger closeCount = new AtomicInteger(0);
    public static AtomicReference<ClusterResource> clusterMeta = new AtomicReference<>();
    public static ClusterResource noClusterId = new ClusterResource("no_cluster_id");
    public static AtomicReference<ClusterResource> clusterIdBeforeDeserialize = new AtomicReference<>(noClusterId);

    public boolean isKey;
    public Map<String, ?> configs;

    public static void resetStaticVariables() {
        initCount = new AtomicInteger(0);
        closeCount = new AtomicInteger(0);
        clusterMeta = new AtomicReference<>();
        clusterIdBeforeDeserialize = new AtomicReference<>(noClusterId);
    }

    public MockDeserializer() {
        initCount.incrementAndGet();
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.configs = configs;
        this.isKey = isKey;
    }

    @Override
    public String deserialize(String topic, byte[] data) {
        // This will ensure that we get the cluster metadata when deserialize is called for the first time
        // as subsequent compareAndSet operations will fail.
        clusterIdBeforeDeserialize.compareAndSet(noClusterId, clusterMeta.get());
        if (data == null) return null;
        return new String(data, StandardCharsets.UTF_8);
    }

    @Override
    public void close() {
        closeCount.incrementAndGet();
    }

    @Override
    public void onUpdate(ClusterResource clusterResource) {
        clusterMeta.set(clusterResource);
    }
}