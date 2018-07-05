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
 * Generates byte arrays based on a position argument.
 *
 * The array generated at a given position should be the same no matter how many
 * times generate() is invoked.  PayloadGenerator instances should be immutable
 * and thread-safe.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(value = ConstantPayloadGenerator.class, name = "constant"),
    @JsonSubTypes.Type(value = SequentialPayloadGenerator.class, name = "sequential"),
    @JsonSubTypes.Type(value = UniformRandomPayloadGenerator.class, name = "uniformRandom"),
    @JsonSubTypes.Type(value = NullPayloadGenerator.class, name = "null")
    })
public interface PayloadGenerator {
    /**
     * Generate a payload.
     *
     * @param position  The position to use to generate the payload
     *
     * @return          A new array object containing the payload.
     */
    byte[] generate(long position);
}
