/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class MockDeserializer implements ClusterResourceListener, Deserializer<byte[]> {
    public static final AtomicInteger INIT_COUNT = new AtomicInteger(0);
    public static final AtomicInteger CLOSE_COUNT = new AtomicInteger(0);
    public static final AtomicReference<ClusterResource> CLUSTER_META = new AtomicReference<>();
    public static final ClusterResource NO_CLUSTER_ID = new ClusterResource("no_cluster_id");
    public static final AtomicReference<ClusterResource> CLUSTER_ID_BEFORE_DESERIALIZE = new AtomicReference<>(NO_CLUSTER_ID);

    public MockDeserializer() {
        INIT_COUNT.incrementAndGet();
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] deserialize(String topic, byte[] data) {
        // This will ensure that we get the cluster metadata when deserialize is called for the first time
        // as subsequent compareAndSet operations will fail.
        CLUSTER_ID_BEFORE_DESERIALIZE.compareAndSet(NO_CLUSTER_ID, CLUSTER_META.get());
        return data;
    }

    @Override
    public void close() {
        CLOSE_COUNT.incrementAndGet();
    }

    @Override
    public void onUpdate(ClusterResource clusterResource) {
        CLUSTER_META.set(clusterResource);
    }
}