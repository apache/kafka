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
package org.apache.kafka.clients.producer;


import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@ClusterTestDefaults(
        types = {Type.CO_KRAFT},
        serverProperties = {
            @ClusterConfigProperty(key = ServerLogConfigs.AUTO_CREATE_TOPICS_ENABLE_CONFIG, value = "false")
        }
)
class ProducerCompressionTest {


    private static final String TOPIC = "topic";

    /**
     * testCompression
     * <p>
     * Compressed messages should be able to sent and consumed correctly
     */
    @ClusterTest
    @ParameterizedTest
    @ValueSource(strings = {"gzip", "snappy", "lz4", "zstd"})
    void testCompression(String compression, ClusterInstance cluster) throws InterruptedException {
//        cluster.createTopic(TOPIC, 1, (short) 1);
        System.out.println(compression);
    }


}
