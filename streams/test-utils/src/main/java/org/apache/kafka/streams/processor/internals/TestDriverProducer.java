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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TaskId;

import java.util.Map;
import java.util.UUID;

public class TestDriverProducer extends StreamsProducer {

    public TestDriverProducer(final StreamsConfig config,
                              final KafkaClientSupplier clientSupplier,
                              final LogContext logContext) {
        super(config, "TopologyTestDriver-Thread", clientSupplier, new TaskId(0, 0), UUID.randomUUID(), logContext);
    }

    @Override
    public void commitTransaction(final Map<TopicPartition, OffsetAndMetadata> offsets,
                                  final ConsumerGroupMetadata consumerGroupMetadata) throws ProducerFencedException {
        super.commitTransaction(offsets, consumerGroupMetadata);
    }
}
