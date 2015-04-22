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
package org.apache.kafka.clients.producer;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.test.MockMetricsReporter;
import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;

public class KafkaProducerTest {


    @Test
    public void testConstructorClose() throws Exception {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "testConstructorClose");
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "some.invalid.hostname.foo.bar:9999");
        props.setProperty(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG, MockMetricsReporter.class.getName());

        MockMetricsReporter.CLOSE_COUNT.set(0);
        try {
            KafkaProducer<byte[], byte[]> producer = new KafkaProducer<byte[], byte[]>(
                    props, new ByteArraySerializer(), new ByteArraySerializer());
        } catch (KafkaException e) {
            Assert.assertEquals(1, MockMetricsReporter.CLOSE_COUNT.get());
            MockMetricsReporter.CLOSE_COUNT.set(0);
            Assert.assertEquals("Failed to construct kafka producer", e.getMessage());
            return;
        }
        Assert.fail("should have caught an exception and returned");
    }
}
