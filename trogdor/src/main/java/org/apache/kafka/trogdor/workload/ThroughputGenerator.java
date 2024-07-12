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

/**
 * This interface is used to facilitate running a configurable number of messages per second by throttling if the
 * throughput goes above a certain amount.
 *
 * Currently there are 2 throughput methods:
 *
 *   * `constant` will use `ConstantThroughputGenerator` to keep the number of messages per second constant.
 *   * `gaussian` will use `GaussianThroughputGenerator` to vary the number of messages per second on a normal
 *     distribution.
 *
 * Please see the implementation classes for more details.
 */

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(value = ConstantThroughputGenerator.class, name = "constant"),
    @JsonSubTypes.Type(value = GaussianThroughputGenerator.class, name = "gaussian")
    })
public interface ThroughputGenerator {
    void throttle() throws InterruptedException;
}
