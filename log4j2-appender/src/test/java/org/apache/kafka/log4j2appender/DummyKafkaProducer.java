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
package org.apache.kafka.log4j2appender;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.serialization.Serdes;

import java.util.Properties;

/**
 * {@link MockProducer} class for test.
 */
public class DummyKafkaProducer extends MockProducer<byte[], byte[]> {
    public DummyKafkaProducer(final Properties properties) {
        super(true, Serdes.ByteArray().serializer(), Serdes.ByteArray().serializer());
    }
}
