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

package org.apache.kafka.trogdor.workload;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.kafka.clients.producer.KafkaProducer;

/**
 * This interface is used to facilitate flushing the KafkaProducers on a cadence specified by the user.
 *
 * Currently there are 3 flushing methods:
 *
 *   * Disabled, by not specifying this parameter.
 *   * `constant` will use `ConstantFlushGenerator` to keep the number of messages per batch constant.
 *   * `gaussian` will use `GaussianFlushGenerator` to vary the number of messages per batch on a normal distribution.
 *
 * Please see the implementation classes for more details.
 */

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(value = ConstantFlushGenerator.class, name = "constant"),
    @JsonSubTypes.Type(value = GaussianFlushGenerator.class, name = "gaussian")
    })
public interface FlushGenerator {
    <K, V> void increment(KafkaProducer<K, V> producer);
}
