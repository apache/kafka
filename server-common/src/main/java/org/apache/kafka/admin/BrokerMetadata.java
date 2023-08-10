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
package org.apache.kafka.admin;

import java.util.Objects;
import java.util.Optional;

/**
 * Broker metadata used by admin tools.
 */
public class BrokerMetadata {
    public final int id;

    public final Optional<String> rack;

    /**
     * @param id an integer that uniquely identifies this broker
     * @param rack the rack of the broker, which is used to in rack aware partition assignment for fault tolerance.
     *             Examples: "RACK1", "us-east-1d"
     */
    public BrokerMetadata(int id, Optional<String> rack) {
        this.id = id;
        this.rack = rack;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BrokerMetadata that = (BrokerMetadata) o;
        return id == that.id && Objects.equals(rack, that.rack);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, rack);
    }
}
