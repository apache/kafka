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
package org.apache.kafka.log4jappender;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.test.MockSerializer;
import org.apache.log4j.spi.LoggingEvent;

import java.util.List;
import java.util.Properties;

public class MockKafkaLog4jAppender extends KafkaLog4jAppender {
    private MockProducer<byte[], byte[]> mockProducer =
            new MockProducer<>(false, new MockSerializer(), new MockSerializer());

    private Properties producerProperties;

    @Override
    protected Producer<byte[], byte[]> getKafkaProducer(Properties props) {
        producerProperties = props;
        return mockProducer;
    }

    void setKafkaProducer(MockProducer<byte[], byte[]> producer) {
        this.mockProducer = producer;
    }

    @Override
    protected void append(LoggingEvent event) {
        if (super.getProducer() == null) {
            activateOptions();
        }
        super.append(event);
    }

    List<ProducerRecord<byte[], byte[]>> getHistory() {
        return mockProducer.history();
    }

    public Properties getProducerProperties() {
        return producerProperties;
    }
}
