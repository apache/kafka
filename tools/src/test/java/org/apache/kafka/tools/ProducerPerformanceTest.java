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
package org.apache.kafka.tools;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class ProducerPerformanceTest {

    @Test
    public void testBootstrapServerOption() throws Exception {

        String[] args = {
            "--bootstrap-server", "server:9092",
            "--topic", "test",
            "--throughput", "10",
            "--num-records", "10",
            "--record-size", "10",
            "--producer-props", "acks=1"
        };

        ProducerPerformance producerPerformance = new ProducerPerformance();
        ProducerPerformance.ProducerPerformanceOptions opts = new ProducerPerformance.ProducerPerformanceOptions(args);
        Properties props = producerPerformance.getProducerProps(opts);

        assertEquals(props.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG), "server:9092");
    }

    @Test
    public void testBootstrapServerOverriddenOption() throws Exception {

        String[] args = {
            "--bootstrap-server", "server:9092",
            "--topic", "test",
            "--throughput", "10",
            "--num-records", "10",
            "--record-size", "10",
            "--producer-props", "acks=1", "bootstrap.servers=differentserver:9092"
        };

        ProducerPerformance producerPerformance = new ProducerPerformance();
        ProducerPerformance.ProducerPerformanceOptions opts = new ProducerPerformance.ProducerPerformanceOptions(args);
        Properties props = producerPerformance.getProducerProps(opts);

        assertEquals(props.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG), "differentserver:9092");
    }
}
