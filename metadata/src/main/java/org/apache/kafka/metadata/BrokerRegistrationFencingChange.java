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

package org.apache.kafka.metadata;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;


public enum BrokerRegistrationFencingChange {
    FENCE(1, Optional.of(true)),
    NONE(0, Optional.empty()),
    UNFENCE(-1, Optional.of(false));

    private final byte value;

    private final Optional<Boolean> asBoolean;

    private final static Map<Byte, BrokerRegistrationFencingChange> VALUE_TO_ENUM =
        Arrays.stream(BrokerRegistrationFencingChange.values()).
                collect(Collectors.toMap(v -> v.value(), Function.identity()));

    public static Optional<BrokerRegistrationFencingChange> fromValue(byte value) {
        return Optional.ofNullable(VALUE_TO_ENUM.get(value));
    }

    BrokerRegistrationFencingChange(int value, Optional<Boolean> asBoolean) {
        this.value = (byte) value;
        this.asBoolean = asBoolean;
    }

    public Optional<Boolean> asBoolean() {
        return asBoolean;
    }

    public byte value() {
        return value;
    }
}
